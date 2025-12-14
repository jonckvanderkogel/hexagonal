package com.bullit.application.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.mockito.Mockito.*;

final class KafkaOutputStreamTest {

    private static final String TOPIC = "test-topic";
    private static final long WAIT_MS = 3_100;

    private record TestPayload(String value) {}

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
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        ObjectMapper mapper = mock(ObjectMapper.class);

        var payload = new TestPayload("ok");
        var json = "{\"value\":\"ok\"}";

        when(mapper.writeValueAsString(payload)).thenReturn(json);

        when(producer.send(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        stream = new KafkaOutputStream<>(TOPIC, producer, mapper);
        stream.startSendingLoop();

        stream.emit(payload);

        assertSoftly(s -> {
            s.check(() -> verify(mapper).writeValueAsString(payload));
            s.check(() ->
                    verify(producer).send(
                            eq(new ProducerRecord<>(TOPIC, json)),
                            any()
                    )
            );
        });
    }

    @Test
    void serialization_failure_logs_poison_and_does_not_send() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        ObjectMapper mapper = mock(ObjectMapper.class);

        var payload = new TestPayload("bad");

        when(mapper.writeValueAsString(payload))
                .thenThrow(new RuntimeException("boom"));

        stream = new KafkaOutputStream<>(TOPIC, producer, mapper);
        stream.startSendingLoop();

        stream.emit(payload);

        Thread.sleep(200);

        assertSoftly(s -> {
            s.check(() -> verify(producer, never()).send(any(), any()));
        });
    }

    @Test
    void async_send_failure_triggers_retry_and_eventual_success() throws JsonProcessingException {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        ObjectMapper mapper = mock(ObjectMapper.class);

        var payload = new TestPayload("retry");
        var json = "{\"value\":\"retry\"}";

        when(mapper.writeValueAsString(payload)).thenReturn(json);

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

        stream = new KafkaOutputStream<>(TOPIC, producer, mapper);
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
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        ObjectMapper mapper = mock(ObjectMapper.class);

        stream = new KafkaOutputStream<>(TOPIC, producer, mapper);
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
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        ObjectMapper mapper = mock(ObjectMapper.class);

        var payload = new TestPayload("final");
        var json = "{\"value\":\"final\"}";

        when(mapper.writeValueAsString(payload)).thenReturn(json);
        when(producer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(RecordMetadata.class)));

        stream = new KafkaOutputStream<>(TOPIC, producer, mapper);
        stream.startSendingLoop();

        stream.emit(payload);

        stream.close();

        assertSoftly(s -> {
            s.check(() -> verify(producer, atLeastOnce()).send(any(ProducerRecord.class)));
            s.check(() -> verify(producer).flush());
            s.check(() -> verify(producer).close());
        });
    }

    @Test
    void close_does_not_throw_if_producer_close_fails() {
        @SuppressWarnings("unchecked")
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        ObjectMapper mapper = mock(ObjectMapper.class);

        doThrow(new RuntimeException("boom")).when(producer).close();

        stream = new KafkaOutputStream<>(TOPIC, producer, mapper);
        stream.startSendingLoop();

        assertSoftly(s -> {
            s.assertThatCode(stream::close).doesNotThrowAnyException();
        });
    }
}
