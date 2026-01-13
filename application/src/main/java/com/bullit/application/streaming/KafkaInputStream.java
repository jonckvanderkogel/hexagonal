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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    private final KafkaConsumer<String, T> consumer;
    private final String topic;

    private final String handleRetrySubject;
    private final String commitRetrySubject;

    private final Map<TopicPartition, Long> nextOffsetByPartition = new ConcurrentHashMap<>();
    private final Queue<TopicPartition> completions = new ConcurrentLinkedQueue<>();
    private final Set<TopicPartition> revoked = ConcurrentHashMap.newKeySet();

    private final Set<Thread> workers = Collections.synchronizedSet(ConcurrentHashMap.newKeySet());

    private volatile boolean stopping = false;
    private Thread poller;
    private StreamHandler<T> handler;

    public KafkaInputStream(String topic, KafkaConsumer<String, T> consumer) {
        this.topic = topic;
        this.consumer = requireNonNull(consumer, "consumer");

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
                runUntilInterrupted(
                        this::pollIteration,
                        () -> stopping
                );
            } finally {
                // IMPORTANT: KafkaConsumer is NOT thread-safe. All consumer interactions
                // (commit/resume/close) must happen on this poller thread.
                shutdownOnPollerThread();
            }
        });
    }

    private void shutdownOnPollerThread() {
        // At this point:
        // - stopping is true OR we were interrupted/woken up
        // - no further poll iterations should run
        //
        // We now:
        // 1) wait for workers to finish (they may still advance offsets / enqueue completions)
        // 2) do a last best-effort commit of all progress
        // 3) close the consumer
        try {
            snapshotWorkers().forEach(this::joinPreservingInterrupt);

            // No more nextOffsetByPartition updates after workers are joined.
            commitAndResumeAllProgressPipeline();
        } catch (Exception e) {
            log.warn("Error during poller-thread shutdown for topic {}", topic, e);
        } finally {
            safeCloseConsumer();
            log.info("Kafka consumer for topic {} shut down cleanly", topic);
        }
    }

    private void pollIteration() {
        drainCompletionsPipeline();
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

    private void drainCompletionsPipeline() {
        drainQueue(completions)
                .filter(tp -> !revoked.contains(tp))
                .forEach(this::commitAndResumePartition);
    }

    private void commitAndResumePartition(TopicPartition tp) {
        commitPartitionWithRetry(tp, nextOffsetByPartition.get(tp));
        resumePartition(tp);
    }

    private void dispatchRecordsPipeline(ConsumerRecords<String, T> records) {
        if (records.isEmpty()) return;

        records.partitions().stream()
                .filter(tp -> !revoked.contains(tp))
                .map(tp -> Map.entry(tp, records.records(tp)))
                .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
                .forEach(e -> pauseAndSpawnWorker(e.getKey(), e.getValue()));
    }

    private void pauseAndSpawnWorker(TopicPartition tp, List<ConsumerRecord<String, T>> batch) {
        pausePartition(tp);

        var t = Thread.ofVirtual().start(() -> runWorkerBatch(tp, batch));
        workers.add(t);
    }

    private void runWorkerBatch(TopicPartition tp, List<ConsumerRecord<String, T>> batch) {
        try {
            batch.stream()
                    .takeWhile(_ -> !stopping && !revoked.contains(tp))
                    .forEach(rec -> handleAndAdvanceOffset(tp, rec));

            if (!revoked.contains(tp)) {
                completions.add(tp);
            }
        } finally {
            workers.remove(Thread.currentThread());
        }
    }

    private void handleAndAdvanceOffset(TopicPartition tp, ConsumerRecord<String, T> rec) {
        handleOnce(rec);

        nextOffsetByPartition.put(tp, rec.offset() + 1);
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

    private void pausePartition(TopicPartition tp) {
        consumer.pause(Collections.singleton(tp));
    }

    private void resumePartition(TopicPartition tp) {
        consumer.resume(Collections.singleton(tp));
    }

    private void commitPartitionWithRetry(TopicPartition tp, long nextOffset) {
        retryWithBackoff(
                commitRetrySubject,
                COMMIT_RETRIES,
                () -> commitPartition(tp, nextOffset),
                e -> logCommitFailure(tp, nextOffset, e),
                log
        );
    }

    private void commitPartition(TopicPartition tp, long nextOffset) {
        consumer.commitSync(Map.of(tp, new OffsetAndMetadata(nextOffset)));
        log.debug("Consumer committed partition {} -> {}", tp, nextOffset);
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
        revoked.addAll(partitions);

        // Best-effort commit of progress so far (at most one in-flight record may duplicate).
        partitions.stream()
                .map(tp -> Map.entry(tp, nextOffsetByPartition.get(tp)))
                .filter(e -> e.getValue() != null)
                .forEach(e -> commitPartitionWithRetry(e.getKey(), e.getValue()));

        // We no longer own these partitions; clean up any progress we were tracking.
        partitions.forEach(nextOffsetByPartition::remove);

        log.info("Partitions revoked for topic {}: {}", topic, partitions);
    }

    private void onAssignedPipeline(Collection<TopicPartition> partitions) {
        partitions.forEach(revoked::remove);

        log.info("Partitions assigned for topic {}: {}", topic, partitions);
    }

    public void close() {
        stopping = true;

        // Wake up the poller if it's blocked in poll(). This is the supported cross-thread signal.
        consumer.wakeup();

        // Wait for poller to finish; poller will join workers, commit best-effort, and close consumer.
        joinPreservingInterrupt(poller);
    }

    private void commitAndResumeAllProgressPipeline() {
        commitTargetsOnShutdown()
                .forEach(this::commitAndResumePartitionIfProgressPresent);
    }

    private Stream<TopicPartition> commitTargetsOnShutdown() {
        // Union of:
        // - partitions that completed normally (completions queue)
        // - partitions that have progressed offsets (nextOffsetByPartition keys)
        return Stream.concat(
                        drainQueue(completions),
                        nextOffsetByPartition.keySet().stream()
                )
                .filter(tp -> !revoked.contains(tp))
                .distinct();
    }

    private void commitAndResumePartitionIfProgressPresent(TopicPartition tp) {
        var nextOffset = nextOffsetByPartition.get(tp);
        if (nextOffset == null) return;

        commitPartitionWithRetry(tp, nextOffset);
        resumePartition(tp);
    }

    private void safeCloseConsumer() {
        try {
            consumer.close();
        } catch (Exception e) {
            log.warn("Error closing consumer for {}", topic, e);
        }
    }

    private void logCommitFailure(TopicPartition tp, long nextOffset, Exception e) {
        log.error("""
                Commit failed after retries
                Topic: {}
                Partition: {}
                NextOffset: {}
                Error: {}
                """, tp.topic(), tp.partition(), nextOffset, e.toString());
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

    private static <E> Stream<E> drainQueue(Queue<E> queue) {
        return Stream.generate(queue::poll)
                .takeWhile(java.util.Objects::nonNull);
    }

    private List<Thread> snapshotWorkers() {
        synchronized (workers) {
            return List.copyOf(workers);
        }
    }

    private void joinPreservingInterrupt(Thread t) {
        if (t == null || !t.isAlive()) return;

        try {
            t.join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}