package com.bullit.application.streaming;

import com.bullit.domain.model.stream.InputStreamPort;
import com.bullit.domain.model.stream.StreamHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

import static com.bullit.application.streaming.StreamingUtils.retryWithBackoff;
import static com.bullit.application.streaming.StreamingUtils.runUntilInterrupted;

public final class KafkaInputStream<T> implements InputStreamPort<T>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaInputStream.class);

    private final KafkaConsumer<String, T> consumer;
    private final String topic;

    private volatile boolean stopping = false;
    private Thread worker;
    private StreamHandler<T> handler;

    public KafkaInputStream(String topic,
                            KafkaConsumer<String, T> consumer) {
        this.topic = topic;
        this.consumer = consumer;
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
        var records = consumer.poll(Duration.ofSeconds(1));
        records.forEach(rec ->
                retryWithBackoff(
                        5,
                        () -> processInboundMessage(rec),
                        e -> logPoisonRecord(rec, e)
                )
        );
    }

    private void processInboundMessage(ConsumerRecord<String, T> rec) {
        handler.handle(rec.value());
        consumer.commitSync();
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

    @Override
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