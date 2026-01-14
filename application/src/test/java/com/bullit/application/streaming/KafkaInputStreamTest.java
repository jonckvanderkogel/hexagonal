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
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class KafkaInputStreamTest {

    private KafkaInputStream<String> streamUnderTest;

    @AfterEach
    void tearDown() {
        if (streamUnderTest != null) {
            streamUnderTest.close();
        }
    }

    @Test
    void subscribing_registers_handler_and_starts_polling_loop_once() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();


        stubPollFromQueue(consumer, pollQueue);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 10);

        var handler = (StreamHandler<String>) _ -> {
        };
        streamUnderTest.subscribe(handler);

        assertSoftly(s -> {
            s.check(() ->
                    verify(consumer, times(1))
                            .subscribe(eq(List.of("topic-a")), any(ConsumerRebalanceListener.class))
            );

            // Prove polling loop is running by observing at least one poll call
            s.check(() ->
                    verify(consumer, timeout(1_000).atLeastOnce())
                            .poll(any(Duration.class))
            );
        });
    }

    @Test
    void valid_message_is_handled_and_committed() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        var tp = new TopicPartition("topic-a", 0);
        var rec = new ConsumerRecord<>("topic-a", 0, 7L, "k", "payload");

        pollQueue.add(recordsOf(tp, rec));
        pollQueue.add(emptyRecords());

        var handled = new CountDownLatch(1);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 10);
        streamUnderTest.subscribe(_ -> handled.countDown());

        var commitCaptor = ArgumentCaptor.forClass(Map.class);
        verify(consumer, timeout(2_000).atLeastOnce()).commitSync(commitCaptor.capture());

        var commitValues = awaitValue(
                Duration.ofSeconds(2),
                Duration.ofMillis(25),
                commitCaptor::getAllValues,
                values -> {
                    var m = findCommittedOffsets(values, mm -> mm.containsKey(tp));
                    return !m.isEmpty();
                }
        );

        var match = findCommittedOffsets(
                commitValues,
                m -> m.containsKey(tp) && m.get(tp).offset() >= 8L
        );

        assertSoftly(s -> {
            s.assertThat(await(handled, 2, TimeUnit.SECONDS)).isTrue();
            s.assertThat(match).containsKey(tp);
            s.assertThat(match.get(tp).offset()).isEqualTo(8L);
        });
    }

    @Test
    void poison_message_retries_then_is_committed_and_pipeline_continues() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        var tp = new TopicPartition("topic-a", 0);

        var rec1 = new ConsumerRecord<>("topic-a", 0, 0L, "k", "boom");
        var rec2 = new ConsumerRecord<>("topic-a", 0, 1L, "k", "ok");

        pollQueue.add(recordsOf(tp, rec1, rec2));
        pollQueue.add(emptyRecords());

        var calls = new AtomicInteger(0);
        var okHandled = new CountDownLatch(1);

        StreamHandler<String> handler = payload -> {
            if (payload.equals("boom")) {
                if (calls.incrementAndGet() <= 2) { // fail twice, then succeed
                    throw new RuntimeException("transient");
                }
                return;
            }
            okHandled.countDown();
        };

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 1_000);
        streamUnderTest.subscribe(handler);

        var commitCaptor = ArgumentCaptor.forClass(Map.class);
        verify(consumer, timeout(3_000).atLeastOnce()).commitSync(commitCaptor.capture());

        var commitValues = awaitValue(
                Duration.ofSeconds(3),
                Duration.ofMillis(25),
                commitCaptor::getAllValues,
                values -> {
                    var m = findCommittedOffsets(values, mm -> mm.containsKey(tp) && mm.get(tp).offset() >= 2L);
                    return !m.isEmpty();
                }
        );

        var match = findCommittedOffsets(
                commitValues,
                m -> m.containsKey(tp) && m.get(tp).offset() >= 2L
        );

        assertSoftly(s -> {
            s.assertThat(await(okHandled, 3, TimeUnit.SECONDS)).isTrue();
            s.assertThat(match.get(tp).offset()).isEqualTo(2L);
            s.assertThat(calls.get()).isGreaterThanOrEqualTo(2);
        });
    }

    @Test
    void records_are_dispatched_per_partition_and_offsets_are_committed_per_partition() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        var tp0 = new TopicPartition("topic-a", 0);
        var tp1 = new TopicPartition("topic-a", 1);

        pollQueue.add(new ConsumerRecords<>(Map.of(
                tp0, List.of(new ConsumerRecord<>("topic-a", 0, 10L, "k", "p0")),
                tp1, List.of(new ConsumerRecord<>("topic-a", 1, 20L, "k", "p1"))
        )));
        pollQueue.add(emptyRecords());

        var handled = new CountDownLatch(2);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 1_000);
        streamUnderTest.subscribe(_ -> handled.countDown());

        var commitCaptor = ArgumentCaptor.forClass(Map.class);
        verify(consumer, timeout(3_000).atLeastOnce()).commitSync(commitCaptor.capture());

        var commitValues = awaitValue(
                Duration.ofSeconds(3),
                Duration.ofMillis(25),
                commitCaptor::getAllValues,
                values -> {
                    var m = findCommittedOffsets(values, mm ->
                            mm.containsKey(tp0) && mm.get(tp0).offset() >= 11L
                                    && mm.containsKey(tp1) && mm.get(tp1).offset() >= 21L
                    );
                    return !m.isEmpty();
                }
        );

        var match = findCommittedOffsets(
                commitValues,
                m -> m.containsKey(tp0) && m.get(tp0).offset() >= 11L
                        && m.containsKey(tp1) && m.get(tp1).offset() >= 21L
        );

        assertSoftly(s -> {
            s.assertThat(await(handled, 2, TimeUnit.SECONDS)).isTrue();
            s.assertThat(match.get(tp0).offset()).isEqualTo(11L);
            s.assertThat(match.get(tp1).offset()).isEqualTo(21L);
        });
    }

    @Test
    void revoking_partition_stops_worker_and_commits_best_effort_then_forgets_progress() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        var rebalanceCaptor = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
        doNothing().when(consumer).subscribe(anyList(), rebalanceCaptor.capture());

        var tp = new TopicPartition("topic-a", 0);
        pollQueue.add(recordsOf(tp, new ConsumerRecord<>("topic-a", 0, 5L, "k", "p")));
        pollQueue.add(emptyRecords());

        var handled = new CountDownLatch(1);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 1_000);
        streamUnderTest.subscribe(_ -> handled.countDown());

        var listener = rebalanceCaptor.getValue();

        // Trigger revoke
        listener.onPartitionsRevoked(List.of(tp));

        var commitCaptor = ArgumentCaptor.forClass(Map.class);
        verify(consumer, timeout(3_000).atLeastOnce()).commitSync(commitCaptor.capture());

        var commitValues = awaitValue(
                Duration.ofSeconds(2),
                Duration.ofMillis(25),
                commitCaptor::getAllValues,
                values -> {
                    var m = findCommittedOffsets(values, mm -> mm.containsKey(tp));
                    return !m.isEmpty();
                }
        );

        var match = findCommittedOffsets(
                commitValues,
                m -> m.containsKey(tp) && m.get(tp).offset() >= 6L
        );

        assertSoftly(s -> {
            s.assertThat(await(handled, 2, TimeUnit.SECONDS)).isTrue();
            s.assertThat(match).containsKey(tp);
            s.assertThat(match.get(tp).offset()).isEqualTo(6L);
        });
    }

    @Test
    void after_revoke_then_reassignment_new_records_are_processed_again() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        var rebalanceCaptor = ArgumentCaptor.forClass(ConsumerRebalanceListener.class);
        doNothing().when(consumer).subscribe(anyList(), rebalanceCaptor.capture());

        var tp = new TopicPartition("topic-a", 0);

        pollQueue.add(recordsOf(tp, new ConsumerRecord<>("topic-a", 0, 0L, "k", "first")));
        pollQueue.add(emptyRecords());

        var handledFirst = new CountDownLatch(1);
        var handledSecond = new CountDownLatch(1);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 1_000);
        streamUnderTest.subscribe(payload -> {
            if (payload.equals("first")) handledFirst.countDown();
            if (payload.equals("second")) handledSecond.countDown();
        });

        var listener = rebalanceCaptor.getValue();

        listener.onPartitionsRevoked(List.of(tp));
        listener.onPartitionsAssigned(List.of(tp));

        pollQueue.add(recordsOf(tp, new ConsumerRecord<>("topic-a", 0, 1L, "k", "second")));
        pollQueue.add(emptyRecords());

        assertSoftly(s -> {
            s.assertThat(await(handledFirst, 2, TimeUnit.SECONDS)).isTrue();
            s.assertThat(await(handledSecond, 2, TimeUnit.SECONDS)).isTrue();
        });
    }

    @Test
    void close_wakes_consumer_and_closes_consumer_once() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 1_000);
        streamUnderTest.subscribe(_ -> {
        });

        streamUnderTest.close();

        assertSoftly(s -> {
            s.check(() -> verify(consumer, times(1)).wakeup());
            s.check(() -> verify(consumer, times(1)).close());
        });
    }

    @Test
    void close_commits_progress_handled_so_far_best_effort() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        var tp = new TopicPartition("topic-a", 0);
        pollQueue.add(recordsOf(tp, new ConsumerRecord<>("topic-a", 0, 3L, "k", "p")));

        var handled = new CountDownLatch(1);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 1_000);
        streamUnderTest.subscribe(_ -> handled.countDown());

        assertSoftly(s -> {
            s.assertThat(await(handled, 2, TimeUnit.SECONDS)).isTrue();
            streamUnderTest.close();

            var commitCaptor = ArgumentCaptor.forClass(Map.class);
            s.check(() -> verify(consumer, atLeastOnce()).commitSync(commitCaptor.capture()));

            var commitValues = awaitValue(
                    Duration.ofSeconds(2),
                    Duration.ofMillis(25),
                    commitCaptor::getAllValues,
                    values -> {
                        var m = findCommittedOffsets(values, mm -> mm.containsKey(tp) && mm.get(tp).offset() >= 4L);
                        return !m.isEmpty();
                    }
            );

            var match = findCommittedOffsets(
                    commitValues,
                    m -> m.containsKey(tp) && m.get(tp).offset() >= 4L
            );

            s.assertThat(match).containsKey(tp);
            s.assertThat(match.get(tp).offset()).isEqualTo(4L);
        });
    }

    @Test
    void when_partition_queue_is_full_partition_is_paused() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        var tp = new TopicPartition("topic-a", 0);

        var blockHandler = new CountDownLatch(1);
        var handlerEntered = new CountDownLatch(1);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 1);
        streamUnderTest.subscribe(_ -> {
            handlerEntered.countDown();
            await(blockHandler, 2, TimeUnit.SECONDS);
        });

        pollQueue.add(recordsOf(tp, new ConsumerRecord<>("topic-a", 0, 0L, "k", "a")));
        pollQueue.add(recordsOf(tp, new ConsumerRecord<>("topic-a", 0, 1L, "k", "b")));

        assertSoftly(s -> {
            s.assertThat(await(handlerEntered, 2, TimeUnit.SECONDS)).isTrue();
            s.check(() ->
                    verify(consumer, timeout(2_000).atLeastOnce()).pause(eq(Set.of(tp)))
            );
        });

        blockHandler.countDown();
    }

    @Test
    void after_queue_drains_below_threshold_partition_is_resumed() {
        var consumer = mock(KafkaConsumer.class);
        var pollQueue = new LinkedBlockingQueue<ConsumerRecords<String, String>>();
        stubPollFromQueue(consumer, pollQueue);

        var tp = new TopicPartition("topic-a", 0);

        var blockHandler = new CountDownLatch(1);
        var handlerEntered = new CountDownLatch(1);
        var handledSecond = new CountDownLatch(1);

        streamUnderTest = new KafkaInputStream<>("topic-a", consumer, 1);
        streamUnderTest.subscribe(payload -> {
            if (payload.equals("a")) {
                handlerEntered.countDown();
                await(blockHandler, 2, TimeUnit.SECONDS);
                return;
            }
            handledSecond.countDown();
        });

        pollQueue.add(recordsOf(tp, new ConsumerRecord<>("topic-a", 0, 0L, "k", "a")));
        pollQueue.add(recordsOf(tp, new ConsumerRecord<>("topic-a", 0, 1L, "k", "b")));

        assertSoftly(s -> {
            s.assertThat(await(handlerEntered, 2, TimeUnit.SECONDS)).isTrue();

            s.check(() ->
                    verify(consumer, timeout(2_000).atLeastOnce()).pause(eq(Set.of(tp)))
            );

            blockHandler.countDown();

            s.assertThat(await(handledSecond, 2, TimeUnit.SECONDS)).isTrue();

            s.check(() ->
                    verify(consumer, timeout(2_000).atLeastOnce()).resume(eq(Set.of(tp)))
            );
        });
    }

    private static boolean await(CountDownLatch latch, long timeout, TimeUnit unit) {
        try {
            return latch.await(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static <T> T awaitValue(
            Duration timeout,
            Duration pollInterval,
            Supplier<T> supplier,
            Predicate<T> done
    ) {
        var deadline = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
            var value = supplier.get();
            if (done.test(value)) return value;

            sleepUninterruptibly(pollInterval);
        }

        return supplier.get();
    }

    private static void sleepUninterruptibly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> findCommittedOffsets(
            List<Map> captured,
            Predicate<Map<TopicPartition, OffsetAndMetadata>> predicate
    ) {
        return captured.stream()
                .map(KafkaInputStreamTest::unsafeCastCommitMap)
                .filter(predicate)
                .reduce((_, last) -> last)
                .orElse(Map.of());
    }

    @SuppressWarnings("unchecked")
    private static Map<TopicPartition, OffsetAndMetadata> unsafeCastCommitMap(Map raw) {
        return (Map<TopicPartition, OffsetAndMetadata>) raw;
    }

    private static void stubPollFromQueue(
            KafkaConsumer<String, String> consumer,
            BlockingQueue<ConsumerRecords<String, String>> pollQueue
    ) {
        when(consumer.poll(any(Duration.class)))
                .thenAnswer((Answer<ConsumerRecords<String, String>>) _ -> {
                    var next = pollQueue.poll(250, TimeUnit.MILLISECONDS);
                    return next == null ? emptyRecords() : next;
                });
    }

    @SafeVarargs
    private static ConsumerRecords<String, String> recordsOf(
            TopicPartition tp,
            ConsumerRecord<String, String>... records
    ) {
        return new ConsumerRecords<>(Map.of(tp, List.of(records)));
    }

    private static ConsumerRecords<String, String> emptyRecords() {
        return new ConsumerRecords<>(Map.of());
    }
}