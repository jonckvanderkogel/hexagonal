package com.bullit.application.streaming;

import com.bullit.domain.port.driving.stream.StreamHandler;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class KafkaInputStreamTest {

    private static final String TOPIC = "test-topic";

    private record TestPayload(String value) {
    }

    private KafkaInputStream<TestPayload> stream;

    @AfterEach
    void tearDown() {
        if (stream != null) stream.close();
    }

    @Test
    void subscribing_registers_handler_and_starts_polling_loop_once() {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            s.assertThatThrownBy(() -> stream.subscribe(handler))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already");

            s.check(() -> verify(consumer, times(1))
                    .subscribe(eq(List.of(TOPIC)), any(ConsumerRebalanceListener.class)));

            s.check(() -> verify(consumer, atLeastOnce()).poll(any(Duration.class)));
        });
    }

    @Test
    void valid_message_is_handled_and_committed_and_resumed() {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        var payload = new TestPayload("ok");

        var handled = new CountDownLatch(1);
        doAnswer(inv -> {
            handled.countDown();
            return null;
        }).when(handler).handle(eq(payload));

        var paused = new CountDownLatch(1);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            var tps = (Collection<TopicPartition>) inv.getArgument(0);
            if (tps.contains(tp(0))) paused.countDown();
            return null;
        }).when(consumer).pause(anyCollection());

        var committed = new CountDownLatch(1);
        doAnswer(inv -> {
            committed.countDown();
            return null;
        }).when(consumer).commitSync(anyMap());

        var resumed = new CountDownLatch(1);
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            var tps = (Collection<TopicPartition>) inv.getArgument(0);
            if (tps.contains(tp(0))) resumed.countDown();
            return null;
        }).when(consumer).resume(anyCollection());

        // First poll returns one record; subsequent polls are empty so the poller can drain completions.
        when(consumer.poll(any(Duration.class)))
                .thenReturn(recordsPartition0(payload))
                .thenReturn(ConsumerRecords.empty())
                .thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            s.check(() -> assertThat(paused.await(2, TimeUnit.SECONDS))
                    .as("partition should be paused")
                    .isTrue());

            s.check(() -> assertThat(handled.await(2, TimeUnit.SECONDS))
                    .as("handler should be invoked")
                    .isTrue());

            s.check(() -> assertThat(committed.await(2, TimeUnit.SECONDS))
                    .as("commit should happen after completion drain")
                    .isTrue());

            s.check(() -> assertThat(resumed.await(2, TimeUnit.SECONDS))
                    .as("partition should be resumed after commit")
                    .isTrue());

            s.check(() -> verify(handler, times(1)).handle(payload));
            s.check(() -> verify(consumer, atLeastOnce()).pause(anyCollection()));
            s.check(() -> verify(consumer, atLeastOnce()).commitSync(anyMap()));
            s.check(() -> verify(consumer, atLeastOnce()).resume(anyCollection()));
        });
    }

    @Test
    void poison_message_retries_then_is_committed_and_pipeline_continues() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        var payload = new TestPayload("boom");

        doThrow(new RuntimeException("fail")).when(handler).handle(payload);

        var committed = new CountDownLatch(1);
        doAnswer(inv -> {
            committed.countDown();
            return null;
        }).when(consumer).commitSync(anyMap());

        when(consumer.poll(any(Duration.class)))
                .thenReturn(recordsPartition0(payload))
                .thenReturn(ConsumerRecords.empty())
                .thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            s.check(() -> assertThat(committed.await(5, TimeUnit.SECONDS)).isTrue());
            s.check(() -> verify(handler, atLeast(5)).handle(payload));
            s.check(() -> verify(consumer, atLeastOnce()).commitSync(anyMap()));
        });
    }

    @Test
    void records_are_dispatched_per_partition_pause_happens_before_processing_and_each_partition_commits_independently() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        var p0 = new TestPayload("p0");
        var p1 = new TestPayload("p1");

        // We want to assert: for each partition, pause happens before any handler call for records from that poll.
        // Because execution is async, we synchronize the test by waiting until we observed both pause calls
        // before allowing handler.handle(..) to return.
        var pauseSeen = new CountDownLatch(2);

        doAnswer(inv -> {
            pauseSeen.countDown();
            return null;
        }).when(consumer).pause(any());

        // block handler until both pause() calls have been observed
        doAnswer(inv -> {
            assertThat(pauseSeen.await(1, TimeUnit.SECONDS)).isTrue();
            return null;
        }).when(handler).handle(any());

        var committedP0 = new CountDownLatch(1);
        var committedP1 = new CountDownLatch(1);

        doAnswer(inv -> {
            Map<TopicPartition, OffsetAndMetadata> m = inv.getArgument(0);

            if (m.containsKey(tp(0))) committedP0.countDown();
            if (m.containsKey(tp(1))) committedP1.countDown();
            return null;
        }).when(consumer).commitSync(anyMap());

        when(consumer.poll(any(Duration.class)))
                .thenReturn(recordsTwoPartitions(p0, p1))
                .thenReturn(ConsumerRecords.empty())
                .thenReturn(ConsumerRecords.empty())
                .thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            s.check(() -> assertThat(committedP0.await(2, TimeUnit.SECONDS)).isTrue());
            s.check(() -> assertThat(committedP1.await(2, TimeUnit.SECONDS)).isTrue());

            s.check(() -> verify(consumer, atLeastOnce())
                    .pause(argThat(c -> c != null && c.size() == 1 && c.contains(tp(0)))));
            s.check(() -> verify(consumer, atLeastOnce())
                    .pause(argThat(c -> c != null && c.size() == 1 && c.contains(tp(1)))));

            s.check(() -> verify(consumer, atLeastOnce())
                    .resume(argThat(c -> c != null && c.size() == 1 && c.contains(tp(0)))));
            s.check(() -> verify(consumer, atLeastOnce())
                    .resume(argThat(c -> c != null && c.size() == 1 && c.contains(tp(1)))));

            s.check(() -> verify(handler).handle(p0));
            s.check(() -> verify(handler).handle(p1));
        });
    }

    @Test
    void revoking_partition_commits_progress_in_revoke_callback_and_completion_does_not_resume_partition() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        var listenerRef = captureRebalanceListener(consumer);

        var p0a = new TestPayload("p0-a");
        var p0b = new TestPayload("p0-b");

        var firstHandled = new CountDownLatch(1);
        var blockSecond = new CountDownLatch(1);
        var blockSecondPoll = new CountDownLatch(1);

        // p0a completes normally, exactly once.
        doAnswer(inv -> {
            firstHandled.countDown();
            return null;
        }).when(handler).handle(eq(p0a));

        // p0b blocks so the worker doesn't finish the batch (no completion -> no poller resume path).
        doAnswer(inv -> {
            blockSecond.await(2, TimeUnit.SECONDS);
            return null;
        }).when(handler).handle(eq(p0b));

        var committedOnRevoke = new CountDownLatch(1);
        doAnswer(inv -> {
            Map<TopicPartition, OffsetAndMetadata> m = inv.getArgument(0);

            var md = m.get(tp(0));
            if (md != null && md.offset() == 1L) committedOnRevoke.countDown();
            return null;
        }).when(consumer).commitSync(anyMap());

        // First poll returns both records; second poll blocks so the poller can't loop/drain/resume while we revoke.
        doAnswer(new Answer<ConsumerRecords<String, TestPayload>>() {
            private int calls = 0;

            @Override
            public ConsumerRecords<String, TestPayload> answer(InvocationOnMock inv) throws Throwable {
                calls++;
                if (calls == 1) {
                    return recordsPartition0TwoRecords(p0a, p0b);
                }
                blockSecondPoll.await(2, TimeUnit.SECONDS);
                return ConsumerRecords.empty();
            }
        }).when(consumer).poll(any(Duration.class));

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        var listener = listenerRef.get();
        assertThat(listener).as("rebalance listener must be captured").isNotNull();

        // Wait until p0a is done; then nextOffsetByPartition should be 1 shortly after.
        assertThat(firstHandled.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(25);

        // Revoke while:
        // - worker is blocked on p0b (no completion enqueued)
        // - poller is blocked in poll() (cannot drain completions/resume)
        listener.onPartitionsRevoked(List.of(tp(0)));

        assertSoftly(s -> {
            s.check(() -> assertThat(committedOnRevoke.await(2, TimeUnit.SECONDS)).isTrue());

            s.check(() -> verify(handler, times(1)).handle(p0a));
            s.check(() -> verify(handler, atMost(1)).handle(p0b)); // may have started before revoke depending on timing

            // With poller blocked + no completion, resume must not happen.
            s.check(() -> verify(consumer, never())
                    .resume(argThat(c -> c != null && c.contains(tp(0)))));
        });

        // Unblock test scaffolding so close() can finish cleanly.
        blockSecond.countDown();
        blockSecondPoll.countDown();
    }

    @Test
    void after_reassignment_processing_resumes_for_partition() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        var listenerRef = captureRebalanceListener(consumer);

        var first = new TestPayload("first");
        var second = new TestPayload("second");

        var firstHandled = new CountDownLatch(1);
        doAnswer(inv -> {
            firstHandled.countDown();
            return null;
        })
                .when(handler).handle(eq(first));

        var secondHandled = new CountDownLatch(1);
        doAnswer(inv -> {
            secondHandled.countDown();
            return null;
        })
                .when(handler).handle(eq(second));

        var committedFirst = new CountDownLatch(1);
        var committedSecond = new CountDownLatch(1);

        doAnswer(inv -> {
            Map<TopicPartition, OffsetAndMetadata> m = inv.getArgument(0);

            var md = m.get(tp(0));
            if (md == null) return null;

            if (md.offset() == 1L) committedFirst.countDown();
            if (md.offset() == 2L) committedSecond.countDown();

            return null;
        }).when(consumer).commitSync(anyMap());

        var allowSecondPoll = new CountDownLatch(1);
        var secondEmitted = new AtomicBoolean(false);

        doAnswer(new Answer<ConsumerRecords<String, TestPayload>>() {
            private int calls = 0;

            @Override
            public ConsumerRecords<String, TestPayload> answer(InvocationOnMock inv) {
                calls++;

                if (calls == 1) return recordsPartition0WithOffset(first, 0L);

                if (allowSecondPoll.getCount() == 0 && secondEmitted.compareAndSet(false, true)) {
                    return recordsPartition0WithOffset(second, 1L);
                }

                return ConsumerRecords.empty();
            }
        }).when(consumer).poll(any(Duration.class));

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            var listener = listenerRef.get();
            s.check(() -> assertThat(listener)
                    .as("rebalance listener must be captured")
                    .isNotNull());

            s.check(() -> assertThat(firstHandled.await(2, TimeUnit.SECONDS))
                    .as("first record should be handled")
                    .isTrue());

            s.check(() -> assertThat(committedFirst.await(2, TimeUnit.SECONDS))
                    .as("first batch should have committed before we simulate reassignment")
                    .isTrue());

            if (listener != null) {
                listener.onPartitionsRevoked(List.of(tp(0)));
                listener.onPartitionsAssigned(List.of(tp(0)));
            }

            allowSecondPoll.countDown();

            s.check(() -> assertThat(secondHandled.await(2, TimeUnit.SECONDS))
                    .as("second record should be handled after reassignment")
                    .isTrue());

            s.check(() -> assertThat(committedSecond.await(2, TimeUnit.SECONDS))
                    .as("second batch should be committed")
                    .isTrue());

            s.check(() -> verify(handler, times(1)).handle(first));
            s.check(() -> verify(handler, times(1)).handle(second));
        });
    }

    @Test
    void close_wakes_consumer_and_closes_consumer_once() {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            s.assertThatCode(stream::close).doesNotThrowAnyException();

            s.check(() -> verify(consumer).wakeup());
            s.check(() -> verify(consumer, times(1)).close());
        });
    }

    @Test
    void close_commits_progress_handled_so_far_best_effort() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaConsumer<String, TestPayload> consumer = mock(KafkaConsumer.class);
        @SuppressWarnings("unchecked")
        StreamHandler<TestPayload> handler = mock(StreamHandler.class);

        var payload = new TestPayload("p0-a");

        // Handler will block until we let it proceed.
        var handlerEntered = new CountDownLatch(1);
        var allowHandlerReturn = new CountDownLatch(1);

        // We want to know the poller started its *second* poll call.
        var secondPollStarted = new CountDownLatch(1);

        // Block the second poll until close() calls wakeup().
        var allowSecondPollReturn = new CountDownLatch(1);

        // Observe commit of progressed offset (offset 0 handled => commit next offset 1).
        var committedOffset1 = new CountDownLatch(1);

        doAnswer(inv -> {
            handlerEntered.countDown();
            assertThat(allowHandlerReturn.await(2, TimeUnit.SECONDS)).isTrue();
            return null;
        }).when(handler).handle(eq(payload));

        doAnswer(inv -> {
            Map<TopicPartition, OffsetAndMetadata> m = inv.getArgument(0);

            var md = m.get(tp(0));
            if (md != null && md.offset() == 1L) committedOffset1.countDown();

            return null;
        }).when(consumer).commitSync(anyMap());

        doAnswer(new Answer<ConsumerRecords<String, TestPayload>>() {
            private int calls = 0;

            @Override
            public ConsumerRecords<String, TestPayload> answer(InvocationOnMock inv) throws Throwable {
                calls++;

                if (calls == 1) {
                    return recordsPartition0WithOffset(payload, 0L);
                }

                if (calls == 2) {
                    secondPollStarted.countDown();
                    assertThat(allowSecondPollReturn.await(2, TimeUnit.SECONDS)).isTrue();
                    return ConsumerRecords.empty();
                }

                return ConsumerRecords.empty();
            }
        }).when(consumer).poll(any(Duration.class));

        doAnswer(inv -> {
            allowSecondPollReturn.countDown();
            return null;
        }).when(consumer).wakeup();

        stream = new KafkaInputStream<>(TOPIC, consumer);
        stream.subscribe(handler);

        assertSoftly(s -> {
            s.check(() -> assertThat(handlerEntered.await(2, TimeUnit.SECONDS))
                    .as("handler should start processing the first record")
                    .isTrue());

            s.check(() -> assertThat(secondPollStarted.await(2, TimeUnit.SECONDS))
                    .as("poller should reach second poll() while handler is still blocked, so it cannot commit")
                    .isTrue());
        });

        // Now let the handler complete so the worker can:
        // - update nextOffsetByPartition to 1
        // - enqueue completion
        // ...while poller is blocked inside poll()
        allowHandlerReturn.countDown();

        // Shutdown: should commit progressed work in finalizeShutdownCommitPipeline()
        stream.close();
        stream = null; // prevent tearDown double-close

        assertSoftly(s -> {
            s.check(() -> assertThat(committedOffset1.await(2, TimeUnit.SECONDS))
                    .as("close() should commit progressed offset (next offset after handled record)")
                    .isTrue());

            s.check(() -> verify(handler, times(1)).handle(payload));
            s.check(() -> verify(consumer, atLeastOnce()).commitSync(anyMap()));
            s.check(() -> verify(consumer).wakeup());
            s.check(() -> verify(consumer, times(1)).close());
        });
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition(TOPIC, partition);
    }

    private static ConsumerRecords<String, TestPayload> recordsPartition0(TestPayload value) {
        var record = new ConsumerRecord<>(TOPIC, 0, 0L, "k", value);
        return new ConsumerRecords<>(Map.of(tp(0), List.of(record)), Map.of());
    }

    private static ConsumerRecords<String, TestPayload> recordsPartition0WithOffset(TestPayload value, long offset) {
        var tp = new TopicPartition(TOPIC, 0);
        var record = new ConsumerRecord<>(TOPIC, 0, offset, "k", value);
        return new ConsumerRecords<>(Map.of(tp, List.of(record)), Map.of());
    }

    private static ConsumerRecords<String, TestPayload> recordsPartition0TwoRecords(TestPayload a, TestPayload b) {
        var r0 = new ConsumerRecord<>(TOPIC, 0, 0L, "k", a);
        var r1 = new ConsumerRecord<>(TOPIC, 0, 1L, "k", b);
        return new ConsumerRecords<>(Map.of(tp(0), List.of(r0, r1)), Map.of());
    }

    private static ConsumerRecords<String, TestPayload> recordsTwoPartitions(TestPayload p0, TestPayload p1) {
        var r0 = new ConsumerRecord<>(TOPIC, 0, 0L, "k0", p0);
        var r1 = new ConsumerRecord<>(TOPIC, 1, 0L, "k1", p1);
        return new ConsumerRecords<>(Map.of(
                tp(0), List.of(r0),
                tp(1), List.of(r1)
        ), Map.of());
    }

    private static AtomicReference<ConsumerRebalanceListener> captureRebalanceListener(KafkaConsumer<String, TestPayload> consumer) {
        var ref = new AtomicReference<ConsumerRebalanceListener>();

        doAnswer(inv -> {
            ConsumerRebalanceListener l = inv.getArgument(1);
            ref.set(l);
            return null;
        }).when(consumer).subscribe(eq(List.of(TOPIC)), any(ConsumerRebalanceListener.class));

        return ref;
    }
}