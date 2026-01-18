package com.bullit.application;

import com.bullit.application.streaming.KafkaInputStream;
import com.bullit.application.streaming.StreamConfigProperties;
import com.bullit.data.adapter.driven.jpa.FooEntity;
import com.bullit.data.adapter.driven.jpa.FooRepository;
import com.bullit.domain.event.FooEvent;
import com.bullit.domain.port.driven.stream.BatchInputStreamPort;
import com.bullit.domain.port.driven.stream.OutputStreamPort;
import com.bullit.domain.port.driven.stream.StreamKey;
import com.bullit.domain.port.driving.stream.BatchStreamHandler;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = RANDOM_PORT)
@Import(StreamToDatabaseIT.TestConfig.class)
public class StreamToDatabaseIT {
    private static final String TOPIC = "stream-to-db-it";
    private static final String ROYALTY_TOPIC = "stream-to-db-royalty-it";
    private static final Logger log = LoggerFactory.getLogger(StreamToDatabaseIT.class);

    @Autowired
    private TestDataGenerator testDataGenerator;
    @Autowired
    private FooRepository fooRepository;
    @Autowired
    private StreamToDbHandler handler;
    @Autowired
    private KafkaInputStream<FooEvent> fooInputStream;

    @Test
    public void testPerformance() {
        var total = 1_000_000L;

        System.gc();

        try (var sampler = new MemorySampler(Duration.ofMillis(200))) {

            var startedAt = Instant.now();
            testDataGenerator.startGenerating(total);
            var generatedAt = Instant.now();

            awaitAtMost(
                    Duration.ofMinutes(15),
                    Duration.ofMillis(50),
                    () -> handler.persistedCount() >= total,
                    sampler
            );

            var finishedAt = Instant.now();

            log.info("Generated {} events in {} ms",
                    total, Duration.between(startedAt, generatedAt).toMillis());

            log.info("End-to-end (generated -> persisted) {} events in {} ms",
                    total, Duration.between(startedAt, finishedAt).toMillis());

            logMemoryUsage(sampler.snapshot());

            assertSoftly(s -> s.assertThat(fooRepository.count()).isEqualTo(total));
        }
    }

    private static long toMb(long bytes) {
        return bytes / (1024 * 1024);
    }

    private void awaitAtMost(
            Duration timeout,
            Duration pollEvery,
            BooleanSupplier done,
            MemorySampler sampler
    ) {
        var deadlineNanos = System.nanoTime() + timeout.toNanos();

        var logStep = 100_000L;
        var nextLogAt = nextLogAt(handler.persistedCount(), logStep);

        while (System.nanoTime() < deadlineNanos && !Thread.currentThread().isInterrupted()) {
            if (done.getAsBoolean()) return;

            var persistedNow = handler.persistedCount();

            if (persistedNow >= nextLogAt) {
                log.info("persisted={}", persistedNow);
                logStreamMetrics(fooInputStream.metricsSnapshot());
                logMemoryUsage(sampler.snapshot());
                nextLogAt += logStep;
            }

            sleepUninterruptibly(pollEvery);
        }

        if (Thread.currentThread().isInterrupted()) {
            throw new AssertionError("Interrupted while awaiting condition");
        }

        throw new AssertionError("Timed out after " + timeout + " waiting for condition");
    }

    private static long nextLogAt(long initial, long step) {
        if (initial <= 0) return step;
        return ((initial / step) + 1) * step;
    }

    private static void logMemoryUsage(MemorySampler.Snapshot snapshot) {
        log.info("Memory used={} MB, peak={} MB, gcCollections={}, gcTimeMs={}",
                toMb(snapshot.usedBytes()),
                toMb(snapshot.peakUsedBytes()),
                snapshot.gcCollections(),
                snapshot.gcTimeMs());
    }

    private void logStreamMetrics(KafkaInputStream.StreamMetrics m) {
        log.info(
                "StreamMetrics topic={} partitions={} paused={} bufferedTotal={} bufferedMaxPerPartition={}",
                m.topic(), m.partitionsKnown(), m.pausedPartitions(), m.bufferedRecordsTotal(), m.bufferedRecordsMaxPerPartition()
        );

        m.byPartition().entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue().bufferedRecords(), a.getValue().bufferedRecords()))
                .forEach(e -> {
                    var tp = e.getKey();
                    var pm = e.getValue();
                    log.info(
                            "  {}-{} buffered={} remaining={} paused={} nextOffset={}",
                            tp.topic(), tp.partition(),
                            pm.bufferedRecords(), pm.remainingCapacity(), pm.paused(), pm.nextOffset()
                    );
                });
    }

    private static void sleepUninterruptibly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class TestDataGenerator {
        private final OutputStreamPort<FooEvent> outputStreamPort;

        public TestDataGenerator(OutputStreamPort<FooEvent> outputStreamPort) {
            this.outputStreamPort = outputStreamPort;
        }

        public void startGenerating(long n) {
            Stream.generate(this::newEvent)
                    .limit(n)
                    .forEach(outputStreamPort::fireAndForget);
        }

        private FooEvent newEvent() {
            return FooEvent.newBuilder()
                    .setFooId(null)
                    .setFoo1(UUID.randomUUID().toString())
                    .setFoo2(UUID.randomUUID().toString())
                    .setFoo3(UUID.randomUUID().toString())
                    .build();
        }
    }

    public static class FooStreamKey implements StreamKey<FooEvent> {
        @Override
        public Class<FooEvent> payloadType() {
            return FooEvent.class;
        }

        @Override
        public String apply(FooEvent event) {
            return event.getFoo1();
        }
    }

    public static class StreamToDbHandler implements BatchStreamHandler<FooEvent> {
        private static final Logger log = LoggerFactory.getLogger(StreamToDbHandler.class);

        private final BatchInputStreamPort<FooEvent> inputStream;
        private final FooRepository fooRepository;
        private final EntityManager entityManager;
        private final AtomicLong persisted = new AtomicLong();

        public StreamToDbHandler(
                BatchInputStreamPort<FooEvent> inputStream,
                FooRepository fooRepository,
                EntityManager entityManager
        ) {
            this.inputStream = inputStream;
            this.fooRepository = fooRepository;
            this.entityManager = entityManager;
        }

        @PostConstruct
        void register() {
            log.info("StreamToDatabaseIT registering stream handler");
            inputStream.subscribeBatch(this);
        }

        @Override
        public void handleBatch(List<FooEvent> events) {
            fooRepository.saveAll(events.stream().map(this::fromEvent).toList());
            fooRepository.flush();
            entityManager.clear();
            persisted.addAndGet(events.size());
        }

        private FooEntity fromEvent(FooEvent event) {
            return new FooEntity(
                    event.getFoo1(),
                    event.getFoo2(),
                    event.getFoo3()
            );
        }

        public long persistedCount() {
            return persisted.get();
        }
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        public TestDataGenerator testDataGenerator(OutputStreamPort<FooEvent> outputStreamPort) {
            return new TestDataGenerator(outputStreamPort);
        }

        @Bean
        @Primary
        public StreamConfigProperties streamConfigProperties() {
            return new StreamConfigProperties(
                    List.of(
                            new StreamConfigProperties.InputConfig(
                                    com.bullit.domain.event.FooEvent.class,
                                    TOPIC,
                                    "stream-to-db-it",
                                    25000,
                                    1000
                            )),
                    List.of(
                            new StreamConfigProperties.OutputConfig(
                                    com.bullit.domain.event.FooEvent.class,
                                    TOPIC,
                                    FooStreamKey.class
                            ),

                            new StreamConfigProperties.OutputConfig(
                                    com.bullit.domain.event.SaleEvent.class,
                                    ROYALTY_TOPIC,
                                    null
                            )), // this one is needed for the royaltyServicePort in BeansConfig
                    List.of(),
                    List.of(
                            new StreamConfigProperties.BatchHandlerConfig(StreamToDbHandler.class)
                    )
            );
        }
    }
}
