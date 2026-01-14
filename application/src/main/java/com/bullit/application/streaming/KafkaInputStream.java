package com.bullit.application.streaming;

import com.bullit.domain.port.driven.stream.InputStreamPort;
import com.bullit.domain.port.driving.stream.StreamHandler;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.bullit.application.FunctionUtils.retryWithBackoff;
import static com.bullit.application.FunctionUtils.runUntilInterrupted;
import static java.util.Objects.requireNonNull;

public final class KafkaInputStream<T> implements InputStreamPort<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaInputStream.class);

    private static final int POLL_RETRIES = 5;
    private static final int HANDLE_RETRIES = 5;
    private static final int COMMIT_RETRIES = 5;

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    // Backpressure + commit tuning knobs (keep as constants for now; can be moved to config later)
    private final int partitionQueueCapacity;
    private final int resumeThreshold;

    private static final Duration COMMIT_INTERVAL = Duration.ofMillis(250);

    private final KafkaConsumer<String, T> consumer;
    private final String topic;

    private final String handleRetrySubject;
    private final String commitRetrySubject;

    private final Map<TopicPartition, Long> nextOffsetByPartition = new ConcurrentHashMap<>();
    private final Map<TopicPartition, PartitionWorker> workersByPartition = new ConcurrentHashMap<>();

    private final Queue<TopicPartition> resumeRequests = new ConcurrentLinkedQueue<>();
    private final Set<TopicPartition> paused = ConcurrentHashMap.newKeySet();

    private volatile boolean stopping = false;
    private Thread poller;
    private StreamHandler<T> handler;

    private Instant lastCommitAt = Instant.EPOCH;

    public KafkaInputStream(
            String topic,
            KafkaConsumer<String, T> consumer,
            int partitionQueueCapacity
    ) {
        this.topic = topic;
        this.consumer = requireNonNull(consumer, "consumer");
        this.partitionQueueCapacity = partitionQueueCapacity;
        this.resumeThreshold = partitionQueueCapacity / 2;

        this.handleRetrySubject = "handling input stream message for topic %s".formatted(topic);
        this.commitRetrySubject = "handling input stream commit for topic %s".formatted(topic);
    }

    @Override
    public synchronized void subscribe(StreamHandler<T> handler) {
        log.info("Subscription received: {}", handler);

        if (this.handler != null) {
            throw new IllegalStateException("KafkaInputStream for topic '%s' already has a handler".formatted(topic));
        }

        this.handler = requireNonNull(handler, "handler");
        consumer.subscribe(Collections.singletonList(topic), rebalanceListener());

        startPollingLoop();
        log.info("Successfully received subscription, polling started");
    }

    private void startPollingLoop() {
        poller = Thread.ofVirtual().start(() -> {
            try {
                runUntilInterrupted(this::pollIteration, () -> stopping);
            } finally {
                shutdownOnPollerThread();
            }
        });
    }

    private void pollIteration() {
        drainResumeRequestsPipeline();
        commitProgressIfDue();

        if (stopping) return;

        pollRecords().ifPresent(this::dispatchRecordsPipeline);
    }

    private Optional<ConsumerRecords<String, T>> pollRecords() {
        return retryWithBackoff(
                "Polling for new record",
                POLL_RETRIES,
                () -> consumer.poll(POLL_TIMEOUT),
                e -> {
                    log.error("Error during polling topic: {}", topic, e);
                    return Optional.empty();
                },
                log
        );
    }

    private void dispatchRecordsPipeline(ConsumerRecords<String, T> records) {
        records.partitions().forEach(tp ->
                enqueuePartitionBatch(tp, records.records(tp))
        );
    }

    private void enqueuePartitionBatch(TopicPartition tp, List<ConsumerRecord<String, T>> batch) {
        var worker = workerFor(tp);

        batch.forEach(rec -> {
            if (stopping) return;

            if (worker.enqueue(rec) == BackpressureSignal.PAUSE) {
                pausePartition(tp);
            }
        });
    }

    private PartitionWorker workerFor(TopicPartition tp) {
        return workersByPartition.computeIfAbsent(tp, this::startWorkerForPartition);
    }

    private PartitionWorker startWorkerForPartition(TopicPartition tp) {
        var worker = new PartitionWorker(tp, partitionQueueCapacity);
        worker.start();
        return worker;
    }

    private void pausePartition(TopicPartition tp) {
        if (paused.add(tp)) {
            consumer.pause(Collections.singleton(tp));
        }
    }

    private void resumePartition(TopicPartition tp) {
        if (paused.remove(tp)) {
            consumer.resume(Collections.singleton(tp));
        }
    }

    private void drainResumeRequestsPipeline() {
        drainQueue(resumeRequests)
                .filter(workersByPartition::containsKey)
                .forEach(this::resumePartition);
    }

    private void commitProgressIfDue() {
        if (!isCommitDue()) return;
        commitAllProgress();
        lastCommitAt = Instant.now();
    }

    private boolean isCommitDue() {
        return Duration.between(lastCommitAt, Instant.now()).compareTo(COMMIT_INTERVAL) >= 0;
    }

    private void commitAllProgress() {
        var offsets = commitOffsetsSnapshot();
        if (offsets.isEmpty()) return;

        retryWithBackoff(
                commitRetrySubject,
                COMMIT_RETRIES,
                () -> commitSync(offsets),
                e -> logCommitFailure(offsets, e),
                log
        );
    }

    private Map<TopicPartition, OffsetAndMetadata> commitOffsetsSnapshot() {
        return nextOffsetByPartition.entrySet().stream()
                .filter(e -> e.getKey() != null && e.getValue() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new OffsetAndMetadata(e.getValue())
                ));
    }

    private void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumer.commitSync(offsets);
        log.debug("Consumer committed {} partitions for topic {}", offsets.size(), topic);
    }

    private ConsumerRebalanceListener rebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                onRevokedPipeline(partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                onAssignedPipeline(partitions);
            }
        };
    }

    private void onRevokedPipeline(Collection<TopicPartition> partitions) {
        stopAndJoinRevokedWorkers(partitions);

        commitAllProgress();

        partitions.forEach(nextOffsetByPartition::remove);
        partitions.forEach(paused::remove);

        log.info("Partitions revoked for topic {}: {}", topic, partitions);
    }

    private void stopAndJoinRevokedWorkers(Collection<TopicPartition> partitions) {
        partitions.stream()
                .map(workersByPartition::remove)
                .filter(Objects::nonNull)
                .forEach(w -> {
                    w.stop();
                    w.join();
                });
    }

    private void onAssignedPipeline(Collection<TopicPartition> partitions) {
        partitions.forEach(this::resumePartition);
        log.info("Partitions assigned for topic {}: {}", topic, partitions);
    }

    public void close() {
        stopping = true;
        consumer.wakeup();
        joinPreservingInterrupt(poller);
    }

    private void shutdownOnPollerThread() {
        try {
            stopAllWorkers();
            joinAllWorkers();

            commitAllProgress();

            paused.forEach(this::resumePartition);
        } catch (Exception e) {
            log.warn("Error during poller-thread shutdown for topic {}", topic, e);
        } finally {
            safeCloseConsumer();
            log.info("Kafka consumer for topic {} shut down cleanly", topic);
        }
    }

    private void stopAllWorkers() {
        workersByPartition.values().forEach(PartitionWorker::stop);
    }

    private void joinAllWorkers() {
        workersByPartition.values().forEach(PartitionWorker::join);
        workersByPartition.clear();
    }

    private void safeCloseConsumer() {
        try {
            consumer.close();
        } catch (Exception e) {
            log.warn("Error closing consumer for {}", topic, e);
        }
    }

    private void logCommitFailure(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
        log.error("""
                Commit failed after retries
                Topic: {}
                Partitions: {}
                Error: {}
                """, topic, offsets.keySet(), e.toString());
    }

    private static <E> Stream<E> drainQueue(Queue<E> queue) {
        return Stream.generate(queue::poll)
                .takeWhile(Objects::nonNull);
    }

    private void joinPreservingInterrupt(Thread t) {
        if (t == null || !t.isAlive()) return;

        try {
            t.join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private final class PartitionWorker {
        private final TopicPartition tp;
        private final ArrayBlockingQueue<ConsumerRecord<String, T>> queue;

        private volatile boolean stopped = false;
        private boolean resumeRequested = false;
        private Thread thread;

        private PartitionWorker(TopicPartition tp, int capacity) {
            this.tp = tp;
            this.queue = new ArrayBlockingQueue<>(capacity);
        }

        private void start() {
            thread = Thread.ofVirtual().start(this::runLoop);
        }

        private boolean shouldRun() {
            return !stopped && !stopping;
        }

        private void runLoop() {
            while (shouldRun()) {
                var rec = pollRecord(Duration.ofMillis(200));
                if (rec == null) continue;

                handleOnce(rec);
                nextOffsetByPartition.put(tp, rec.offset() + 1);
                maybeRequestResume();
            }
        }

        private void handleOnce(ConsumerRecord<String, T> rec) {
            retryWithBackoff(
                    handleRetrySubject,
                    HANDLE_RETRIES,
                    () -> handler.handle(rec.value()),
                    e -> logPoisonRecord(rec, e),
                    log
            );
        }

        private void logPoisonRecord(ConsumerRecord<String, T> rec, Exception e) {
            log.error("""
                    Poison message after max retries
                    Topic: {}
                    Partition: {}
                    Offset: {}
                    Payload: {}
                    Error: {}
                    """, rec.topic(), rec.partition(), rec.offset(), rec.value(), e.toString());
        }

        private void maybeRequestResume() {
            if (!resumeRequested && queue.size() <= resumeThreshold) {
                resumeRequested = true;
                resumeRequests.add(tp);
            }
        }

        private ConsumerRecord<String, T> pollRecord(Duration timeout) {
            try {
                return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        private BackpressureSignal enqueue(ConsumerRecord<String, T> rec) {
            if (!shouldRun()) return BackpressureSignal.NONE;

            try {
                resumeRequested = false;
                queue.put(rec);

                return queue.remainingCapacity() == 0
                        ? BackpressureSignal.PAUSE
                        : BackpressureSignal.NONE;

            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
                return BackpressureSignal.NONE;
            }
        }

        private void stop() {
            stopped = true;
            queue.clear();
            Optional.ofNullable(thread).ifPresent(Thread::interrupt);
        }

        private void join() {
            joinPreservingInterrupt(thread);
        }
    }

    private enum BackpressureSignal {
        NONE,
        PAUSE
    }
}