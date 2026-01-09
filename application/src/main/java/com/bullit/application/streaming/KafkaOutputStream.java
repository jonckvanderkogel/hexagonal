package com.bullit.application.streaming;

import com.bullit.domain.port.driven.stream.OutputStreamPort;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.bullit.application.FunctionUtils.retryWithBackoff;

public final class KafkaOutputStream<T> implements OutputStreamPort<T>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutputStream.class);

    private static final int MAX_RETRY_ATTEMPTS = 5;

    private final String topic;
    private final KafkaProducer<String, T> producer;
    private final String sendRetrySubject;

    private volatile boolean stopping = false;

    public KafkaOutputStream(String topic, KafkaProducer<String, T> kafkaProducer) {
        this.topic = requireText(topic, "topic");
        this.producer = Objects.requireNonNull(kafkaProducer, "kafkaProducer");
        this.sendRetrySubject = "sending output stream message for topic %s".formatted(topic);
    }

    @Override
    public void emit(T element) {
        requireRunning();
        Objects.requireNonNull(element, "element");

        retryWithBackoff(
                sendRetrySubject,
                MAX_RETRY_ATTEMPTS,
                () -> sendAndAwaitAck(element),
                ex -> logPoisonMessage(element, ex),
                log
        );
    }

    private void sendAndAwaitAck(T element) throws Exception {
        producer.send(new ProducerRecord<>(topic, element)).get();
    }

    private void logPoisonMessage(T element, Exception e) {
        log.error("Poison outbound message after retries for topic {}: {}", topic, element, e);
    }

    private void requireRunning() {
        if (stopping) {
            throw new IllegalStateException("Producer is shutting down");
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required " + name);
        }
        return value;
    }

    @Override
    public void close() {
        stopping = true;

        try {
            producer.flush();
        } catch (Exception e) {
            log.warn("Error flushing Kafka producer for topic {}", topic, e);
        }

        try {
            producer.close();
        } catch (Exception e) {
            log.warn("Error closing Kafka producer for topic {}", topic, e);
        }

        log.info("Kafka producer for topic {} shut down cleanly", topic);
    }
}