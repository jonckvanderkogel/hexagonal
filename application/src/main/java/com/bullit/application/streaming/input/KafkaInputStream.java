package com.bullit.application.streaming.input;

import com.bullit.domain.port.driven.stream.BatchInputStreamPort;
import com.bullit.domain.port.driven.stream.InputStreamPort;
import com.bullit.domain.port.driving.stream.BatchStreamHandler;
import com.bullit.domain.port.driving.stream.StreamHandler;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.bullit.application.FunctionUtils.retryWithBackoff;
import static com.bullit.application.FunctionUtils.runUntilInterrupted;
import static java.util.Objects.requireNonNull;

public final class KafkaInputStream<T> implements InputStreamPort<T>, BatchInputStreamPort<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaInputStream.class);

    private static final int POLL_RETRIES = 5;
    private static final int COMMIT_RETRIES = 5;

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration COMMIT_INTERVAL = Duration.ofMillis(250);

    private final int partitionQueueCapacity;
    private final int maxBatchSize;

    private final KafkaConsumer<String, T> consumer;
    private final String topic;

    private final String handleRetrySubject;
    private final String commitRetrySubject;

    private final Map<TopicPartition, Long> nextOffsetByPartition = new ConcurrentHashMap<>();

    private volatile boolean stopping = false;
    private Thread poller;
    private HandlerMode<T> handler;
    private PartitionWorkers<T> workers;

    private Instant lastCommitAt = Instant.EPOCH;

    public KafkaInputStream(
            String topic,
            KafkaConsumer<String, T> consumer,
            int partitionQueueCapacity,
            int maxBatchSize
    ) {
        this.topic = topic;
        this.consumer = requireNonNull(consumer, "consumer");
        this.partitionQueueCapacity = partitionQueueCapacity;
        this.maxBatchSize = maxBatchSize;

        this.handleRetrySubject = "handling input stream message for topic %s".formatted(topic);
        this.commitRetrySubject = "handling input stream commit for topic %s".formatted(topic);
    }

    @Override
    public synchronized void subscribe(StreamHandler<T> handler) {
        subscribeInternal(HandlerMode.single(handler));
    }

    @Override
    public synchronized void subscribeBatch(BatchStreamHandler<T> handler) {
        subscribeInternal(HandlerMode.batch(handler, maxBatchSize));
    }

    private synchronized void subscribeInternal(HandlerMode<T> handlerMode) {
        log.info("Subscription received: {}", handlerMode);

        if (this.handler != null) {
            throw new IllegalStateException("KafkaInputStream for topic '%s' already has a handler".formatted(topic));
        }

        this.handler = handlerMode;
        this.workers = new PartitionWorkers<>(
                consumer,
                partitionQueueCapacity,
                handler,
                handleRetrySubject,
                this::shouldRun,
                nextOffsetByPartition::put,
                nextOffsetByPartition::get,
                log
        );

        consumer.subscribe(Collections.singletonList(topic), rebalanceListener());
        startPollingLoop();

        log.info("Successfully received subscription, polling started");
    }

    private boolean shouldRun() {
        return !stopping && !Thread.currentThread().isInterrupted();
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
        workers.drainResumeRequests();
        commitProgressIfDue();

        if (stopping) return;

        pollRecords().ifPresent(this::dispatchRecords);
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

    private void dispatchRecords(ConsumerRecords<String, T> records) {
        var assignedCount = partitionsAssignedCount();

        records.partitions().forEach(tp ->
                workers.dispatch(tp, records.records(tp), assignedCount)
        );
    }

    private int partitionsAssignedCount() {
        var assigned = consumer.assignment();
        return assigned == null || assigned.isEmpty() ? 1 : assigned.size();
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
                () -> consumer.commitSync(offsets),
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
        workers.onRevoked(partitions);
        commitAllProgress();

        partitions.forEach(nextOffsetByPartition::remove);
        log.info("Partitions revoked for topic {}: {}", topic, partitions);
    }

    private void onAssignedPipeline(Collection<TopicPartition> partitions) {
        workers.onAssigned(partitions, partitionsAssignedCount());
        log.info("Partitions assigned for topic {}: {}", topic, partitions);
    }

    public void close() {
        stopping = true;
        consumer.wakeup();
        joinPreservingInterrupt(poller);
    }

    private void shutdownOnPollerThread() {
        try {
            workers.stopAll();
            workers.joinAll();

            commitAllProgress();
        } catch (Exception e) {
            log.warn("Error during poller-thread shutdown for topic {}", topic, e);
        } finally {
            safeCloseConsumer();
            log.info("Kafka consumer for topic {} shut down cleanly", topic);
        }
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

    private void joinPreservingInterrupt(Thread t) {
        if (t == null || !t.isAlive()) return;

        try {
            t.join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public Metrics.StreamMetrics metricsSnapshot() {
        var byPartition = workers.metricsSnapshot();

        var bufferedTotal = byPartition.values().stream().mapToLong(Metrics.PartitionMetrics::bufferedRecords).sum();
        var bufferedMax = byPartition.values().stream().mapToLong(Metrics.PartitionMetrics::bufferedRecords).max().orElse(0);

        return new Metrics.StreamMetrics(
                topic,
                byPartition.size(),
                workers.pausedCount(),
                bufferedTotal,
                bufferedMax,
                byPartition
        );
    }
}