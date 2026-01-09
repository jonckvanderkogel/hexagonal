package com.bullit.application.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.bullit.application.TestUtils.nullKeyFunction;
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

        when(producer.send(any()))
                .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

        stream = new KafkaOutputStream<>(TOPIC, producer, nullKeyFunction());

        stream.emit(payload);

        assertSoftly(s -> {
            s.check(() ->
                    verify(producer).send(
                            eq(new ProducerRecord<>(TOPIC, payload))
                    )
            );
        });
    }

    @Test
    void send_failure_triggers_retry_and_eventual_success() {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        var payload = new TestPayload("retry");

        // Fail twice, then succeed on the 3rd attempt
        var failuresBeforeSuccess = 2;
        var attempts = new AtomicInteger(0);

        when(producer.send(any(ProducerRecord.class)))
                .thenAnswer(invocation -> {
                    var attempt = attempts.incrementAndGet();
                    if (attempt <= failuresBeforeSuccess) {
                        return CompletableFuture.failedFuture(new RuntimeException("fail-" + attempt));
                    }
                    return CompletableFuture.completedFuture(mock(RecordMetadata.class));
                });

        stream = new KafkaOutputStream<>(TOPIC, producer, nullKeyFunction());

        // should NOT throw (eventually succeeds within retry budget)
        stream.emit(payload);

        assertSoftly(s -> {
            s.assertThat(attempts.get()).isEqualTo(3);

            s.check(() ->
                    verify(producer, times(3))
                            .send(eq(new ProducerRecord<>(TOPIC, payload)))
            );
        });
    }

    @Test
    void emit_throws_when_stream_is_stopping() {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        stream = new KafkaOutputStream<>(TOPIC, producer, nullKeyFunction());
        stream.close();

        assertSoftly(s -> {
            s.assertThatThrownBy(() -> stream.emit(new TestPayload("x")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("shutting down");
        });
    }

    @Test
    void close_flushes_and_closes_producer() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        // Arrange: synchronous send succeeds
        when(producer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

        var payload = new TestPayload("final");
        var stream = new KafkaOutputStream<>(TOPIC, producer, nullKeyFunction());

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

        stream = new KafkaOutputStream<>(TOPIC, producer, nullKeyFunction());

        assertSoftly(s -> {
            s.assertThatCode(stream::close).doesNotThrowAnyException();
        });
    }

    @Test
    void emit_applies_key_function_and_sends_record_with_key() {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, TestPayload> producer = mock(KafkaProducer.class);

        var payload = new TestPayload("ok");

        when(producer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

        var keyFun = (Function<TestPayload, String>) p -> "k:" + p.value();

        stream = new KafkaOutputStream<>(TOPIC, producer, keyFun);

        stream.emit(payload);

        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());

        @SuppressWarnings("unchecked")
        var rec = (ProducerRecord<String, TestPayload>) captor.getValue();

        assertSoftly(s -> {
            s.assertThat(rec.topic()).isEqualTo(TOPIC);
            s.assertThat(rec.key()).isEqualTo("k:ok");
            s.assertThat(rec.value()).isEqualTo(payload);
        });
    }
}
