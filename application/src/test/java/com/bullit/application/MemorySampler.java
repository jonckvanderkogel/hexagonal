package com.bullit.application;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.bullit.application.FunctionUtils.runUntilInterrupted;

public final class MemorySampler implements AutoCloseable {
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong peakUsedBytes = new AtomicLong(0);

    private final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    private final long gcCountStart;
    private final long gcTimeMsStart;

    private final Thread thread;

    public MemorySampler(Duration sampleEvery) {
        this.gcCountStart = gcCount();
        this.gcTimeMsStart = gcTimeMs();

        this.thread = Thread.ofVirtual().start(() -> {
            runUntilInterrupted(() -> {
                        sample();
                        sleepUninterruptibly(sampleEvery);
                    },
                    () -> !running.get());
        });
    }

    public Snapshot snapshot() {
        return new Snapshot(
                usedBytes(),
                peakUsedBytes.get(),
                gcCount() - gcCountStart,
                gcTimeMs() - gcTimeMsStart
        );
    }

    @Override
    public void close() {
        running.set(false);
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        sample();
    }

    private void sample() {
        var used = usedBytes();
        peakUsedBytes.accumulateAndGet(used, Math::max);
    }

    private static long usedBytes() {
        var rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private long gcCount() {
        return gcBeans.stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionCount)
                .filter(v -> v >= 0)
                .sum();
    }

    private long gcTimeMs() {
        return gcBeans.stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                .filter(v -> v >= 0)
                .sum();
    }

    private static void sleepUninterruptibly(Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public record Snapshot(
            long usedBytes,
            long peakUsedBytes,
            long gcCollections,
            long gcTimeMs
    ) {
    }
}
