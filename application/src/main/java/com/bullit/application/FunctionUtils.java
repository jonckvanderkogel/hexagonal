package com.bullit.application;

import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class FunctionUtils {
    private FunctionUtils() {
    }

    private static final Logger log = LoggerFactory.getLogger(FunctionUtils.class);

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedSupplier<T> {
        T get() throws Exception;
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
            String subject,
            int maxAttempts,
            CheckedRunnable action,
            Consumer<Exception> onPoison
    ) {
        retryWithBackoff(subject, maxAttempts, 1, action, onPoison);
    }

    public static <T> Optional<T> retryWithBackoff(
            String subject,
            int maxAttempts,
            CheckedSupplier<T> action,
            Function<Exception, Optional<T>> onPoison
    ) {
        return retryWithBackoff(subject, maxAttempts, 1, action, onPoison);
    }

    private static void retryWithBackoff(
            String subject,
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
            extracted(subject, maxAttempts, attempt, wait);
            sleep(wait);
            retryWithBackoff(subject, maxAttempts, attempt + 1, action, onPoison);
        }
    }

    private static <T> Optional<T> retryWithBackoff(
            String subject,
            int maxAttempts,
            int attempt,
            CheckedSupplier<T> action,
            Function<Exception, Optional<T>> onPoison
    ) {
        try {
            return Optional.ofNullable(action.get());
        } catch (Exception e) {
            if (attempt >= maxAttempts || Thread.currentThread().isInterrupted()) {
                return onPoison.apply(e);
            }
            long wait = exponentialBackoff(attempt);
            extracted(subject, maxAttempts, attempt, wait);
            sleep(wait);
            return retryWithBackoff(subject, maxAttempts, attempt + 1, action, onPoison);
        }
    }

    private static void extracted(String subject, int maxAttempts, int attempt, long wait) {
        log.warn("Retry {}/{} for subject {} failed, backing off {}ms", attempt, maxAttempts, subject, wait);
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
