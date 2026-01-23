package com.bullit.application.streaming.input;

import com.bullit.application.streaming.KafkaClientProperties;
import com.bullit.application.streaming.input.Metrics.PartitionMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.bullit.application.FunctionUtils.retryWithBackoff;

final class PartitionWorker<T> {

    @FunctionalInterface
    interface ShouldRun {
        boolean get();
    }

    /**
     * Return value is ignored; modeled this way so callers can pass Map::put directly.
     */
    @FunctionalInterface
    interface AdvanceOffset {
        Long accept(TopicPartition tp, long nextOffset);
    }

    /**
     * Return value is ignored; modeled this way so callers can pass Queue::add directly.
     */
    @FunctionalInterface
    interface RequestResume {
        boolean accept(TopicPartition tp);
    }

    private static final double EWMA_ALPHA = 0.2;
    private static final int HANDLE_RETRIES = 5;

    private final TopicPartition tp;
    private final int partitionQueueCapacity;
    private final ArrayBlockingQueue<ConsumerRecord<String, T>> queue;
    private final HandlerMode<T> handler;

    private final ShouldRun shouldRun;
    private final AdvanceOffset advanceOffset;
    private final RequestResume requestResume;

    private final String handleRetrySubject;
    private final Logger log;

    private boolean resumeRequested = false;
    private Thread thread;

    private double ewmaHeadroom;

    PartitionWorker(
            TopicPartition tp,
            int partitionQueueCapacity,
            int partitionsAtCreation,
            HandlerMode<T> handler,
            ShouldRun shouldRun,
            AdvanceOffset advanceOffset,
            RequestResume requestResume,
            String handleRetrySubject,
            Logger log
    ) {
        this.tp = tp;
        this.partitionQueueCapacity = partitionQueueCapacity;
        this.handler = handler;
        this.shouldRun = shouldRun;
        this.advanceOffset = advanceOffset;
        this.requestResume = requestResume;
        this.handleRetrySubject = handleRetrySubject;
        this.log = log;

        this.queue = new ArrayBlockingQueue<>(hardLimit(partitionQueueCapacity));
        this.ewmaHeadroom = evenlyDistributedHeadroom(partitionsAtCreation);
    }

    void start() {
        thread = Thread.ofVirtual().start(this::runLoop);
    }

    void stop() {
        queue.clear();
        Optional.ofNullable(thread).ifPresent(Thread::interrupt);
    }

    void join() {
        joinPreservingInterrupt(thread);
    }

    private static void joinPreservingInterrupt(Thread t) {
        if (t == null || !t.isAlive()) return;

        try {
            t.join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    void resetHeadroom(int partitions) {
        ewmaHeadroom = evenlyDistributedHeadroom(partitions);
    }

    BackpressureSignal enqueueBatch(List<ConsumerRecord<String, T>> batch, int partitions) {
        if (!shouldRun.get() || batch.isEmpty()) return BackpressureSignal.NONE;

        try {
            resumeRequested = false;

            var sizeBefore = queue.size();
            putAll(batch);
            var sizeAfter = queue.size();

            updateHeadroomEwma(batch.size());
            maybeLogSoftLimitCrossing(sizeBefore, sizeAfter);

            return shouldPauseAfterEnqueue(partitions) ? BackpressureSignal.PAUSE : BackpressureSignal.NONE;
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return BackpressureSignal.NONE;
        }
    }

    PartitionMetrics metricsSnapshot(boolean paused, Long nextOffset) {
        return new PartitionMetrics(
                queue.size(),
                queue.remainingCapacity(),
                paused,
                nextOffset
        );
    }

    private void runLoop() {
        while (shouldRun.get()) {
            var records = takeBatch(Duration.ofMillis(200), handler.maxBatchSize());
            if (records.isEmpty()) continue;

            handleRecords(records);
            advanceOffset(records);
            maybeRequestResume();
        }
    }

    private void handleRecords(List<ConsumerRecord<String, T>> records) {
        var payloads = records.stream().map(ConsumerRecord::value).toList();

        retryWithBackoff(
                handleRetrySubject,
                HANDLE_RETRIES,
                () -> handler.handle(payloads),
                e -> logPoisonBatch(records, e),
                log
        );
    }

    private void advanceOffset(List<ConsumerRecord<String, T>> records) {
        var last = records.getLast();
        advanceOffset.accept(tp, last.offset() + 1);
    }

    private void maybeRequestResume() {
        if (!resumeRequested && queue.size() <= resumeThreshold(partitionQueueCapacity)) {
            resumeRequested = true;
            requestResume.accept(tp);
        }
    }

    private List<ConsumerRecord<String, T>> takeBatch(Duration firstPollTimeout, int maxBatchSize) {
        var first = pollRecord(firstPollTimeout);
        if (first == null) return List.of();

        var batch = new ArrayList<ConsumerRecord<String, T>>(maxBatchSize);
        batch.add(first);
        queue.drainTo(batch, maxBatchSize - 1);

        return List.copyOf(batch);
    }

    private ConsumerRecord<String, T> pollRecord(Duration timeout) {
        try {
            return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private void putAll(List<ConsumerRecord<String, T>> batch) throws InterruptedException {
        for (var rec : batch) queue.put(rec);
    }

    private void updateHeadroomEwma(int observedBatchSize) {
        if (observedBatchSize <= 0) return;
        ewmaHeadroom = ewma(EWMA_ALPHA, ewmaHeadroom, observedBatchSize);
    }

    private boolean shouldPauseAfterEnqueue(int partitions) {
        var headroom = headroomForNextPoll();
        var cushion = pauseCushion(headroom, partitions);

        return queue.size() + headroom > (partitionQueueCapacity - cushion);
    }

    private int headroomForNextPoll() {
        return clamp(
                (int) Math.ceil(ewmaHeadroom),
                1,
                maxPollRecords(partitionQueueCapacity)
        );
    }

    private int pauseCushion(int headroom, int partitions) {
        var baseline = evenlyDistributedHeadroom(partitions);

        return clamp(
                Math.max(headroom / 4, baseline / 10),
                1,
                partitionQueueCapacity / 2
        );
    }

    private int evenlyDistributedHeadroom(int partitions) {
        var p = Math.max(1, partitions);
        var maxPoll = maxPollRecords(partitionQueueCapacity);
        return (int) Math.ceil((double) maxPoll / (double) p);
    }

    private void maybeLogSoftLimitCrossing(int sizeBefore, int sizeAfter) {
        if (sizeBefore <= partitionQueueCapacity && sizeAfter > partitionQueueCapacity) {
            log.warn(
                    "Partition {} exceeded soft queue limit: size={}, softLimit={}, ewmaHeadroom={}",
                    tp,
                    sizeAfter,
                    partitionQueueCapacity,
                    Math.ceil(ewmaHeadroom)
            );
        }
    }

    private void logPoisonBatch(List<ConsumerRecord<String, T>> records, Exception e) {
        log.error("""
                        Poison batch of {} records after max retries
                        Partitions: {}
                        Offsets: {}
                        Payloads: {}
                        Error: {}
                        """,
                records.size(),
                tp.partition(),
                records
                        .stream()
                        .map(ConsumerRecord::offset)
                        .map(Object::toString)
                        .collect(Collectors.joining(", ")),
                records
                        .stream()
                        .map(ConsumerRecord::value)
                        .map(Object::toString)
                        .collect(Collectors.joining(", ")),
                e.toString()
        );
    }

    private int resumeThreshold(int softLimit) {
        return softLimit / 2;
    }

    private int maxPollRecords(int softLimit) {
        return KafkaClientProperties.derivedMaxPollRecords(softLimit);
    }

    private int hardLimit(int softLimit) {
        return softLimit + maxPollRecords(softLimit);
    }

    private static double ewma(double alpha, double prev, double x) {
        return alpha * x + (1.0 - alpha) * prev;
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    enum BackpressureSignal {
        NONE,
        PAUSE
    }
}