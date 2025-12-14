package com.bullit.application.streaming;

import com.bullit.domain.model.stream.InputStreamPort;
import com.bullit.domain.model.stream.StreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper mapper;
    private final Class<T> type;
    private final String topic;

    private volatile boolean stopping = false;
    private Thread worker;
    private StreamHandler<T> handler;

    public KafkaInputStream(String topic,
                            KafkaConsumer<String, String> consumer,
                            Class<T> type,
                            ObjectMapper mapper) {
        this.topic = topic;
        this.consumer = consumer;
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public synchronized void subscribe(StreamHandler<T> handler) {
        if (this.handler != null) {
            throw new IllegalStateException(
                    "KafkaInputStream for topic '%s' already has a handler".formatted(topic)
            );
        }
        this.consumer.subscribe(Collections.singletonList(topic));
        this.handler = handler;
        startPollingLoop();
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

    private void processInboundMessage(ConsumerRecord<String, String> rec) throws Exception {
        T value = mapper.readValue(rec.value(), type);
        handler.handle(value);
        consumer.commitSync();
    }

    private void logPoisonRecord(ConsumerRecord<String, String> rec, Exception e) {
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