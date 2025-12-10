package com.bullit.application.streaming;

import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Supplier;

public final class StreamingUtils {
    private StreamingUtils() {
    }

    private static final Logger log = LoggerFactory.getLogger(StreamingUtils.class);

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    public static void runUntilInterrupted(Runnable block, Supplier<Boolean> isStopped) {
        try {
            while (!Thread.currentThread().isInterrupted() && !isStopped.get()) {
                block.run();
            }
        } catch (WakeupException ignored) {
            // expected exit path on shutdown
        }
    }

    public static void retryWithBackoff(
            int maxAttempts,
            CheckedRunnable action,
            Consumer<Exception> onPoison
    ) {
        retryWithBackoff(maxAttempts, 1, action, onPoison);
    }

    private static void retryWithBackoff(
            int maxAttempts,
            int attempt,
            CheckedRunnable action,
            Consumer<Exception> onPoison
    ) {
        try {
            action.run();
        } catch (Exception e) {
            if (attempt >= maxAttempts || Thread.currentThread().isInterrupted()) {
                onPoison.accept(e);
                return;
            }
            long wait = exponentialBackoff(attempt);
            log.warn("Retry {}/{} failed, backing off {}ms", attempt, maxAttempts, wait);
            sleep(wait);
            retryWithBackoff(maxAttempts, attempt + 1, action, onPoison);
        }
    }

    private static long exponentialBackoff(int attempt) {
        return (long) Math.pow(2, attempt) * 100; // 200ms → 1600ms
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}
