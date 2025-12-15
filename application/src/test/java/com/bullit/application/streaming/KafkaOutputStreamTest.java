package com.bullit.application.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class KafkaOutputStreamTest {

    private static final String TOPIC = "test-topic";
    private static final long WAIT_MS = 3_100;

    private record TestPayload(String value) {
    }

    private KafkaOutputStream<TestPayload> stream;

    @AfterEach
    void tearDown() {
        if (stream != null) {
            stream.close();
        }
    }

    @Test
    void emit_serializes_and_sends_message() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        var payload = new TestPayload("ok");

        when(producer.send(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        stream = new KafkaOutputStream<>(TOPIC, producer);
        stream.startSendingLoop();

        stream.emit(payload);

        assertSoftly(s -> {
            s.check(() ->
                    verify(producer).send(
                            eq(new ProducerRecord<>(TOPIC, payload)),
                            any()
                    )
            );
        });
    }

    @Test
    void async_send_failure_triggers_retry_and_eventual_success() throws JsonProcessingException {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        var payload = new TestPayload("retry");

        var retrySucceeded = new CountDownLatch(1);

        // async send fails
        when(producer.send(any(ProducerRecord.class), any()))
                .thenAnswer(invocation -> {
                    var cb = invocation.getArgument(
                            1,
                            org.apache.kafka.clients.producer.Callback.class
                    );
                    cb.onCompletion(null, new RuntimeException("fail"));
                    return CompletableFuture.completedFuture(null);
                });

        // retry send succeeds
        when(producer.send(any(ProducerRecord.class)))
                .thenAnswer(_ -> {
                    retrySucceeded.countDown();
                    return CompletableFuture.completedFuture(mock(RecordMetadata.class));
                });

        stream = new KafkaOutputStream<>(TOPIC, producer);
        stream.startSendingLoop();

        stream.emit(payload);

        assertSoftly(s -> {
            s.check(() ->
                    assertThat(
                            retrySucceeded.await(WAIT_MS, TimeUnit.MILLISECONDS)
                    ).isTrue()
            );

            // one async send
            s.check(() ->
                    verify(producer, times(1))
                            .send(any(ProducerRecord.class), any())
            );

            // one retry send
            s.check(() ->
                    verify(producer, times(1))
                            .send(any(ProducerRecord.class))
            );
        });
    }

    @Test
    void emit_throws_when_stream_is_stopping() {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        stream = new KafkaOutputStream<>(TOPIC, producer);
        stream.startSendingLoop();
        stream.close();

        assertSoftly(s -> {
            s.assertThatThrownBy(() -> stream.emit(new TestPayload("x")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("shutting down");
        });
    }

    @Test
    void close_drains_queue_flushes_and_closes_producer() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        // Arrange: synchronous send succeeds
        when(producer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

        var payload = new TestPayload("final");
        var stream = new KafkaOutputStream<>(TOPIC, producer);
        stream.startSendingLoop();

        // Act: enqueue but do NOT start worker
        stream.emit(payload);
        stream.close();

        // Assert: shutdown contract
        assertSoftly(s -> {
            s.check(() ->
                    verify(producer, atLeastOnce())
                            .send(any(ProducerRecord.class))
            );
            s.check(() -> verify(producer).flush());
            s.check(() -> verify(producer).close());
        });
    }

    @Test
    void close_does_not_throw_if_producer_close_fails() {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        doThrow(new RuntimeException("boom")).when(producer).close();

        stream = new KafkaOutputStream<>(TOPIC, producer);
        stream.startSendingLoop();

        assertSoftly(s -> {
            s.assertThatCode(stream::close).doesNotThrowAnyException();
        });
    }
}
