package com.bullit.application.streaming;

import com.bullit.domain.port.driven.stream.InputStreamPort;
import com.bullit.domain.port.driving.stream.StreamHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.bullit.application.FunctionUtils.retryWithBackoff;
import static com.bullit.application.FunctionUtils.runUntilInterrupted;

public final class KafkaInputStream<T> implements InputStreamPort<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaInputStream.class);
    private static final int POLL_RETRIES = 10;
    private static final int HANDLE_RETRIES = 5;
    private static final int COMMIT_RETRIES = 5;
    private final String commitRetrySubject;
    private final String handleRetrySubject;

    private final KafkaConsumer<String, T> consumer;
    private final String topic;

    private volatile boolean stopping = false;
    private Thread worker;
    private StreamHandler<T> handler;

    public KafkaInputStream(String topic,
                            KafkaConsumer<String, T> consumer) {
        this.topic = topic;
        this.consumer = consumer;

        this.commitRetrySubject = "handling input stream commit for topic %s".formatted(topic);
        this.handleRetrySubject = "handling input stream message for topic %s".formatted(topic);
    }

    @Override
    public synchronized void subscribe(StreamHandler<T> handler) {
        log.info("Subscription received: {}", handler);
        if (this.handler != null) {
            throw new IllegalStateException(
                    "KafkaInputStream for topic '%s' already has a handler".formatted(topic)
            );
        }
        this.consumer.subscribe(Collections.singletonList(topic));
        this.handler = handler;
        startPollingLoop();
        log.info("Successfully received subscription, polling started");
    }

    private void startPollingLoop() {
        worker = Thread.ofVirtual().start(() ->
                runUntilInterrupted(
                        this::consumeMessages,
                        () -> stopping
                )
        );
    }

    private void consumeMessages() {
        pollRecords().ifPresent(records -> records.forEach(this::processRecord));
    }

    private Optional<ConsumerRecords<String, T>> pollRecords() {
        return retryWithBackoff(
                "Polling for new record",
                POLL_RETRIES,
                () -> consumer.poll(Duration.ofSeconds(1)),
                e -> {
                    log.error("Error during polling topic: {}", topic, e);
                    return Optional.empty();
                }
        );
    }

    private void processRecord(ConsumerRecord<String, T> rec) {
        retryWithBackoff(
                handleRetrySubject,
                HANDLE_RETRIES,
                () -> handleOnce(rec),
                e -> logPoisonRecord(rec, e)
        );

        retryWithBackoff(
                commitRetrySubject,
                COMMIT_RETRIES,
                () -> commitOffset(rec),
                e -> logCommitFailure(rec, e)
        );
    }

    private void handleOnce(ConsumerRecord<String, T> rec) {
        log.debug("Processing inbound message: {}", rec);
        handler.handle(rec.value());
        log.debug("Handled message: {}", rec);
    }

    private void commitOffset(ConsumerRecord<String, T> rec) {
        var tp = new TopicPartition(rec.topic(), rec.partition());
        var nextOffset = new OffsetAndMetadata(rec.offset() + 1);

        consumer.commitSync(Map.of(tp, nextOffset));
        log.debug("Consumer committed for message: {}", rec);
    }

    private void logCommitFailure(ConsumerRecord<String, T> rec, Exception e) {
        log.error("""
                Commit failed after retries
                Topic: {}
                Partition: {}
                Offset: {}
                Error: {}
                """, rec.topic(), rec.partition(), rec.offset(), e.toString());
    }

    private void logPoisonRecord(ConsumerRecord<String, T> rec, Exception e) {
        log.error("""
                Poison message after max retries
                Topic: {}
                Offset: {}
                Payload: {}
                Error: {}
                """, topic, rec.offset(), rec.value(), e.toString());
    }

    public void close() {
        stopping = true;
        consumer.wakeup();
        waitForWorkerToFinish();
        safeCloseConsumer();
        log.info("Kafka consumer for topic {} shut down cleanly", topic);
    }

    private void waitForWorkerToFinish() {
        if (worker != null && worker.isAlive()) {
            try {
                worker.join();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void safeCloseConsumer() {
        try {
            consumer.close();
        } catch (Exception e) {
            log.warn("Error closing consumer for {}", topic, e);
        }
    }
}