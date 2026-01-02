package com.bullit.application.streaming;

import com.bullit.domain.port.driven.stream.OutputStreamPort;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import static com.bullit.application.FunctionUtils.retryWithBackoff;

public final class KafkaOutputStream<T> implements OutputStreamPort<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutputStream.class);

    private static final int MAX_BUFFER_SIZE = 10_000;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final int MAX_RETRY_ATTEMPTS_ON_SHUTDOWN = 5;

    private final String topic;
    private final KafkaProducer<String, T> producer;
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>(MAX_BUFFER_SIZE);
    private final String sendRetrySubject;

    private Thread worker;
    private volatile boolean stopping = false;

    public KafkaOutputStream(String topic,
                             KafkaProducer<String, T> kafkaProducer) {
        this.topic = topic;
        this.producer = kafkaProducer;
        this.sendRetrySubject = "sending output stream message for topic %s".formatted(topic);
    }

    @PostConstruct
    void startSendingLoop() {
        log.info("KafkaOutputStream for topic {} is starting", topic);
        worker = Thread.ofVirtual().start(() -> {
                    log.info("Virtual thread started");
                    blockingQueueStream().forEach(this::sendAsync);
                }
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

    private void sendAsync(T element) {
        producer.send(
                new ProducerRecord<>(topic, element),
                (_, exception) -> {
                    if (exception != null) {
                        log.error("Received error: {}", element, exception);
                        Thread.ofVirtual().start(() ->
                                retryWithBackoff(
                                        sendRetrySubject,
                                        MAX_RETRY_ATTEMPTS,
                                        () -> retrySendSynchronously(element),
                                        ex -> logPoisonMessage(element, ex)
                                )
                        );
                    }
                }
        );
    }

    private void retrySendSynchronously(T element) throws Exception {
        log.warn("Retrying: {}", element);
        sendSynchronously(element);
    }

    private void sendSynchronously(T element) throws Exception {
        producer.send(new ProducerRecord<>(topic, element)).get();
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
                        retryWithBackoff(
                                "draining remaining queue for output stream for topic %s".formatted(topic),
                                MAX_RETRY_ATTEMPTS_ON_SHUTDOWN,
                                () -> sendSynchronously(element),
                                ex -> logPoisonMessage(element, ex)
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