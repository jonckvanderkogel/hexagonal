package com.bullit.application.streaming;

import com.bullit.domain.model.stream.OutputStreamPort;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import static com.bullit.application.streaming.StreamingUtils.retryWithBackoff;

public final class KafkaOutputStream<T> implements OutputStreamPort<T>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutputStream.class);

    private static final int MAX_BUFFER_SIZE = 10_000;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final int MAX_RETRY_ATTEMPTS_ON_SHUTDOWN = 5;

    private final String topic;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>(MAX_BUFFER_SIZE);

    private Thread worker;
    private volatile boolean stopping = false;

    public KafkaOutputStream(String topic,
                             KafkaProducer<String, String> kafkaProducer,
                             ObjectMapper mapper) {
        this.topic = topic;
        this.producer = kafkaProducer;
        this.mapper = mapper;
    }

    @PostConstruct
    void startSendingLoop() {
        worker = Thread.ofVirtual().start(() ->
                blockingQueueStream().forEach(element ->
                        serializeOrLogPoison(element)
                                .ifPresent(json -> sendAsync(element, json))
                )
        );
    }

    private Stream<T> blockingQueueStream() {
        return Stream.generate(() -> {
            if (stopping) return null;
            try {
                return queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }).takeWhile(Objects::nonNull);
    }

    private Optional<String> serializeOrLogPoison(T element) {
        try {
            return Optional.of(mapper.writeValueAsString(element));
        } catch (Exception e) {
            logPoisonMessage(element, e);
            return Optional.empty();
        }
    }

    private void sendAsync(T element, String json) {
        producer.send(
                new ProducerRecord<>(topic, json),
                (_, exception) -> {
                    if (exception != null) {
                        Thread.ofVirtual().start(() ->
                                retryWithBackoff(
                                        MAX_RETRY_ATTEMPTS,
                                        () -> retrySendSynchronously(json),
                                        ex -> logPoisonMessage(element, ex)
                                )
                        );
                    }
                }
        );
    }

    private void retrySendSynchronously(String json) throws Exception {
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
        drainRemainingQueue();
        producer.flush();
        safeCloseProducer();
        log.info("Kafka producer for topic {} shut down cleanly", topic);
    }

    private void drainRemainingQueue() {
        Stream.generate(queue::poll)
                .takeWhile(Objects::nonNull)
                .forEach(element ->
                        serializeOrLogPoison(element)
                                .ifPresent(json ->
                                        retryWithBackoff(
                                                MAX_RETRY_ATTEMPTS_ON_SHUTDOWN,
                                                () -> retrySendSynchronously(json),
                                                ex -> logPoisonMessage(element, ex)
                                        )
                                )
                );
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