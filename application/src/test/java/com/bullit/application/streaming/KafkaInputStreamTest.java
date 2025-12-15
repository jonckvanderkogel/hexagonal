package com.bullit.application.streaming;

import com.bullit.domain.model.stream.StreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class KafkaInputStreamTest {

    private static final String TOPIC = "test-topic";
    private static final long RETRY_WAIT_MS = 3_100;

    private record TestPayload(String value) {
    }

    private KafkaInputStream<TestPayload> stream;

    @AfterEach
    void tearDown() {
        if (stream != null) {
            stream.close();
        }
    }

    @Test
    void subscribing_registers_handler_and_starts_polling_loop_once() {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        ObjectMapper mapper = mock(ObjectMapper.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        when(consumer.poll(any(Duration.class)))
                .thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);

        stream.subscribe(handler);

        assertSoftly(s -> {
            s.assertThatThrownBy(() -> stream.subscribe(handler))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already");

            s.check(() -> verify(consumer, times(1)).subscribe(List.of(TOPIC)));
            s.check(() -> verify(consumer, atLeastOnce()).poll(any(Duration.class)));
        });
    }

    @Test
    void valid_message_is_deserialized_handled_and_committed() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        var payload = new TestPayload("ok");

        var committed = new CountDownLatch(1);
        doAnswer(_ -> {
            committed.countDown();
            return null;
        }).when(consumer).commitSync();

        when(consumer.poll(any(Duration.class)))
                .thenReturn(records(payload))
                .thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            s.check(() -> assertThat(committed.await(1, TimeUnit.SECONDS)).isTrue());
            s.check(() -> verify(handler).handle(payload));
            s.check(() -> verify(consumer).commitSync());
        });
    }

    @Test
    void handler_exception_triggers_retries_and_does_not_commit() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        var payload = new TestPayload("boom");

        doThrow(new RuntimeException("fail")).when(handler).handle(payload);

        when(consumer.poll(any(Duration.class)))
                .thenReturn(records(payload))
                .thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        Thread.sleep(RETRY_WAIT_MS);

        assertSoftly(s -> {
            s.check(() -> verify(handler, atLeast(5)).handle(payload));
            s.check(() -> verify(consumer, never()).commitSync());
        });
    }

    @Test
    void close_wakes_consumer_stops_worker_and_closes_consumer_once() {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        when(consumer.poll(any(Duration.class)))
                .thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            s.assertThatCode(stream::close).doesNotThrowAnyException();

            s.check(() -> verify(consumer).wakeup());
            s.check(() -> verify(consumer, times(1)).close());
        });
    }

    private static ConsumerRecords<String, TestPayload> records(TestPayload value) {
        var tp = new TopicPartition(TOPIC, 0);
        var record = new ConsumerRecord<>(TOPIC, 0, 0L, "k", value);
        return new ConsumerRecords<>(Map.of(tp, List.of(record)));
    }
}