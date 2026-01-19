package com.bullit.application;

import com.bullit.application.tailrecursion.TailCall;
import com.bullit.application.tailrecursion.TailCalls.Unit;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.bullit.application.tailrecursion.TailCalls.done;

public final class FunctionUtils {
    private FunctionUtils() {
    }

    public static final String HOST_PATTERN = "^(localhost|(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*|(?:25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(?:\\.(?:25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}|(\\[(?=.*:)[0-9A-Fa-f:]{2,}]))$";

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
            Consumer<Exception> onPoison,
            Logger log
    ) {
        retryWithBackoff(subject, maxAttempts, 1, action, onPoison, log).invoke();
    }

    public static <T> Optional<T> retryWithBackoff(
            String subject,
            int maxAttempts,
            CheckedSupplier<T> action,
            Function<Exception, Optional<T>> onPoison,
            Logger log
    ) {
        return retryWithBackoff(subject, maxAttempts, 1, action, onPoison, log).invoke();
    }

    private static TailCall<Unit> retryWithBackoff(
            String subject,
            int maxAttempts,
            int attempt,
            CheckedRunnable action,
            Consumer<Exception> onPoison,
            Logger log
    ) {
        try {
            action.run();
            return done();
        } catch (Exception e) {
            if (attempt >= maxAttempts || Thread.currentThread().isInterrupted()) {
                onPoison.accept(e);
                return done();
            }

            var wait = exponentialBackoff(attempt);
            extracted(subject, maxAttempts, attempt, wait, log);
            sleep(wait);

            return () -> retryWithBackoff(subject, maxAttempts, attempt + 1, action, onPoison, log);
        }
    }

    private static <T> TailCall<Optional<T>> retryWithBackoff(
            String subject,
            int maxAttempts,
            int attempt,
            CheckedSupplier<T> action,
            Function<Exception, Optional<T>> onPoison,
            Logger log
    ) {
        try {
            return done(Optional.ofNullable(action.get()));
        } catch (Exception e) {
            if (attempt >= maxAttempts || Thread.currentThread().isInterrupted()) {
                return done(onPoison.apply(e));
            }
            var wait = exponentialBackoff(attempt);
            extracted(subject, maxAttempts, attempt, wait, log);
            sleep(wait);

            return () -> retryWithBackoff(subject, maxAttempts, attempt + 1, action, onPoison, log);
        }
    }

    private static void extracted(String subject, int maxAttempts, int attempt, Duration wait, Logger log) {
        log.warn("Retry {}/{} for subject {} failed, backing off {}ms", attempt, maxAttempts, subject, wait);
    }

    private static Duration exponentialBackoff(int attempt) {
        return Duration.ofMillis((long) Math.pow(2, attempt) * 100); // 200ms → 1600ms
    }

    public static void sleep(Duration t) {
        try {
            Thread.sleep(t);
        } catch (InterruptedException ignored) {
        }
    }
}
