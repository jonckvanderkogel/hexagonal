package com.bullit.application.streaming.input;

import com.bullit.application.streaming.input.Metrics.PartitionMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class PartitionWorkers<T> {

    private final KafkaConsumer<String, T> consumer;
    private final int partitionQueueCapacity;
    private final HandlerMode<T> handler;
    private final String handleRetrySubject;
    private final Logger log;

    private final PartitionWorker.ShouldRun shouldRun;
    private final PartitionWorker.AdvanceOffset advanceOffset;
    private final ReadNextOffset readNextOffset;

    private final Map<TopicPartition, PartitionWorker<T>> workersByPartition = new ConcurrentHashMap<>();
    private final Queue<TopicPartition> resumeRequests = new ConcurrentLinkedQueue<>();
    private final Set<TopicPartition> paused = ConcurrentHashMap.newKeySet();

    @FunctionalInterface
    interface ReadNextOffset {
        Long apply(TopicPartition tp);
    }

    PartitionWorkers(
            KafkaConsumer<String, T> consumer,
            int partitionQueueCapacity,
            HandlerMode<T> handler,
            String handleRetrySubject,
            PartitionWorker.ShouldRun shouldRun,
            PartitionWorker.AdvanceOffset advanceOffset,
            ReadNextOffset readNextOffset,
            Logger log
    ) {
        this.consumer = consumer;
        this.partitionQueueCapacity = partitionQueueCapacity;
        this.handler = handler;
        this.handleRetrySubject = handleRetrySubject;
        this.shouldRun = shouldRun;
        this.advanceOffset = advanceOffset;
        this.readNextOffset = readNextOffset;
        this.log = log;
    }

    void dispatch(TopicPartition tp, List<ConsumerRecord<String, T>> batch, int partitionsAssigned) {
        if (batch.isEmpty()) return;

        var worker = workerFor(tp, partitionsAssigned);

        if (worker.enqueueBatch(batch, partitionsAssigned) == PartitionWorker.BackpressureSignal.PAUSE) {
            pause(tp);
        }
    }

    void drainResumeRequests() {
        drainQueue(resumeRequests)
                .filter(workersByPartition::containsKey)
                .forEach(this::resume);
    }

    void onRevoked(Collection<TopicPartition> partitions) {
        partitions.stream()
                .map(workersByPartition::remove)
                .filter(Objects::nonNull)
                .forEach(w -> {
                    w.stop();
                    w.join();
                });

        partitions.forEach(paused::remove);
    }

    void onAssigned(Collection<TopicPartition> partitions, int assignedCount) {
        partitions.stream()
                .peek(this::resume)
                .map(workersByPartition::get)
                .filter(Objects::nonNull)
                .forEach(w -> w.resetHeadroom(assignedCount));
    }

    void stopAll() {
        workersByPartition.values().forEach(PartitionWorker::stop);
    }

    void joinAll() {
        workersByPartition.values().forEach(PartitionWorker::join);
        workersByPartition.clear();
    }

    int pausedCount() {
        return paused.size();
    }

    Map<TopicPartition, PartitionMetrics> metricsSnapshot() {
        return workersByPartition.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            var tp = e.getKey();
                            var worker = e.getValue();

                            return worker.metricsSnapshot(
                                    paused.contains(tp),
                                    readNextOffset.apply(tp)
                            );
                        }
                ));
    }

    private PartitionWorker<T> workerFor(TopicPartition tp, int partitionsAssigned) {
        return workersByPartition.computeIfAbsent(tp, _ -> startWorker(tp, partitionsAssigned));
    }

    private PartitionWorker<T> startWorker(TopicPartition tp, int partitionsAssigned) {
        var worker = new PartitionWorker<>(
                tp,
                partitionQueueCapacity,
                partitionsAssigned,
                handler,
                shouldRun,
                advanceOffset,
                resumeRequests::add,
                handleRetrySubject,
                log
        );
        worker.start();
        return worker;
    }

    private void pause(TopicPartition tp) {
        if (paused.add(tp)) {
            consumer.pause(Collections.singleton(tp));
        }
    }

    private void resume(TopicPartition tp) {
        if (paused.remove(tp)) {
            consumer.resume(Collections.singleton(tp));
        }
    }

    private static <E> Stream<E> drainQueue(Queue<E> queue) {
        return Stream.generate(queue::poll).takeWhile(Objects::nonNull);
    }
}