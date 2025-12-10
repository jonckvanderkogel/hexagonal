package com.bullit.application.streaming;

import com.bullit.domain.model.stream.OutputStreamPort;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.bullit.application.streaming.StreamingUtils.retryWithBackoff;
import static com.bullit.application.streaming.StreamingUtils.runUntilInterrupted;

public final class KafkaOutputStream<T> implements OutputStreamPort<T>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutputStream.class);

    private static final int MAX_BUFFER_SIZE = 10_000;
    private static final int MAX_RETRY_ATTEMPTS = 5;

    private final String topic;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>(MAX_BUFFER_SIZE);

    private Thread worker;
    private volatile boolean stopping = false;

    public KafkaOutputStream(String topic,
                             KafkaClientProperties kafkaProps,
                             ObjectMapper mapper) {
        this.topic = topic;
        this.mapper = mapper;
        this.producer = new KafkaProducer<>(kafkaProps.buildProducerProperties());
    }

    @PostConstruct
    private void startSendingLoop() {
        worker = Thread.ofVirtual().start(() ->
                runUntilInterrupted(
                        this::sendQueuedMessages,
                        () -> stopping
                )
        );
    }

    private void sendQueuedMessages() {
        try {
            T element = queue.take();
            retryWithBackoff(
                    MAX_RETRY_ATTEMPTS,
                    () -> sendOnce(element),
                    e -> logPoisonMessage(element, e)
            );
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendOnce(T element) throws Exception {
        String json = mapper.writeValueAsString(element);
        producer.send(new ProducerRecord<>(topic, json)).get();
    }

    private void logPoisonMessage(T element, Exception e) {
        log.error("Poison outbound message after retries: {}", element, e);
    }

    @Override
    public void emit(T element) {
        if (stopping) {
            throw new IllegalStateException("Producer is shutting down");
        }
        try {
            queue.put(element);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted enqueuing message", e);
        }
    }

    @Override
    public void close() {
        stopping = true;
        worker.interrupt();
        waitForWorkerToFinish();
        flushRemainingMessages();
        producer.flush();
        safeCloseProducer();
        log.info("Kafka producer for topic {} shut down cleanly", topic);
    }

    private void flushRemainingMessages() {
        T element;
        while ((element = queue.poll()) != null) {
            try {
                sendOnce(element);
            } catch (Exception e) {
                log.error("Error sending during shutdown: {}", element, e);
            }
        }
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

    private void safeCloseProducer() {
        try {
            producer.close();
        } catch (Exception e) {
            log.warn("Error closing Kafka producer for topic {}", topic, e);
        }
    }
}