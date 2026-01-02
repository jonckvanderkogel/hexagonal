package com.bullit.application.file;

import com.bullit.domain.port.driven.file.FileEnvelope;
import com.bullit.domain.port.driving.file.FileHandler;
import com.bullit.domain.port.driven.file.FileInputPort;
import com.bullit.domain.port.driven.file.FileTarget;
import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.bullit.application.FunctionUtils.retryWithBackoff;
import static com.bullit.application.FunctionUtils.runUntilInterrupted;
import static java.util.stream.StreamSupport.stream;

public final class S3FileInput<T> implements FileInputPort<T> {

    private static final Logger log = LoggerFactory.getLogger(S3FileInput.class);

    private static final int LIST_RETRIES = 5;
    private static final int HANDLE_RETRIES = 3;
    private static final int MOVE_RETRIES = 5;

    private final MinioClient client;

    private final String bucket;
    private final String incomingPrefix;
    private final String handledPrefix;
    private final String errorPrefix;
    private final Duration pollInterval;
    private final String listObjectsRetrySubject;

    private volatile boolean stopping = false;
    private Thread worker;
    private FileHandler<T> handler;

    public S3FileInput(
            MinioClient client,
            String bucket,
            String incomingPrefix,
            String handledPrefix,
            String errorPrefix,
            Duration pollInterval
    ) {
        this.client = Objects.requireNonNull(client, "client is required");
        this.bucket = requireNonBlank(bucket);

        this.incomingPrefix = normalizePrefix(incomingPrefix);
        this.handledPrefix = normalizePrefix(handledPrefix);
        this.errorPrefix = normalizePrefix(errorPrefix);
        this.pollInterval = pollInterval == null ? Duration.ofSeconds(1) : pollInterval;
        this.listObjectsRetrySubject = "listing incoming objects in bucket %s prefix '%s'".formatted(bucket, incomingPrefix);
        validateConfiguration();
    }

    @Override
    public synchronized void subscribe(FileHandler<T> handler) {
        log.info("S3FileInputPort subscription received: {}", handler);
        if (this.handler != null) {
            throw new IllegalStateException("S3FileInputPort for bucket '%s' already has a handler".formatted(bucket));
        }
        this.handler = Objects.requireNonNull(handler, "handler is required");
        startPollingLoop();
        log.info("S3FileInputPort polling started for bucket {}", bucket);
    }

    private void startPollingLoop() {
        worker = Thread.ofVirtual().start(() ->
                runUntilInterrupted(
                        this::pollAndHandleBatch,
                        () -> stopping
                )
        );
    }

    private void pollAndHandleBatch() {
        listIncomingItems()
                .forEach(this::processObject);

        sleep(pollInterval);
    }

    private List<Item> listIncomingItems() {
        return retryWithBackoff(
                listObjectsRetrySubject,
                LIST_RETRIES,
                () -> fetchIncoming()
                        .toList(),
                e -> {
                    log.error("Poison during listing of objects: {}", listObjectsRetrySubject, e);
                    return Optional.of(List.<Item>of());
                }
        ).orElse(List.of());
    }

    private Stream<Item> fetchIncoming() {
        var results = client.listObjects(
                ListObjectsArgs.builder()
                        .bucket(bucket)
                        .prefix(incomingPrefix)
                        .recursive(false)
                        .build()
        );

        return stream(results.spliterator(), false)
                .map(this::unwrap)
                .flatMap(Optional::stream)
                .filter(i -> !i.isDir());
    }

    private Optional<Item> unwrap(Result<Item> r) {
        try {
            return Optional.ofNullable(r.get());
        } catch (Exception e) {
            log.warn("Failed to unwrap listObjects item", e);
            return Optional.empty();
        }
    }

    private void processObject(Item item) {
        var objectKey = item.objectName();
        var subject = "handling file %s/%s".formatted(bucket, objectKey);

        retryWithBackoff(
                subject,
                HANDLE_RETRIES,
                () -> handleOnce(objectKey),
                e -> moveToError(objectKey, e)
        );
    }

    private void handleOnce(String objectKey) throws Exception {
        requireIncomingObjectKey(objectKey);

        log.info("Processing incoming S3 object: {}/{}", bucket, objectKey);

        try (var in = client.getObject(
                GetObjectArgs
                        .builder()
                        .bucket(bucket)
                        .object(objectKey)
                        .build());
             var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        ) {

            handler.handle(new FileEnvelope(new FileTarget(bucket, objectKey), reader.lines()));
        }

        moveToHandled(objectKey);
        log.info("Handled S3 object: {}/{}", bucket, objectKey);
    }

    private void moveToHandled(String objectKey) {
        requireIncomingObjectKey(objectKey);

        var relative = objectKey.substring(incomingPrefix.length());
        moveObject(objectKey, handledPrefix + relative, "handled");
    }

    private void moveToError(String objectKey, Exception e) {
        requireIncomingObjectKey(objectKey);

        log.error("Error handling S3 object {}/{}: {}", bucket, objectKey, e.toString());
        var relative = objectKey.substring(incomingPrefix.length());
        moveObject(objectKey, errorPrefix + relative, "error");
    }

    private void moveObject(String srcKey, String dstKey, String outcome) {
        var copySubject = "copying %s object %s/%s -> %s/%s".formatted(outcome, bucket, srcKey, bucket, dstKey);
        var deleteSubject = "deleting %s source object %s/%s".formatted(outcome, bucket, srcKey);

        retryWithBackoff(
                copySubject,
                MOVE_RETRIES,
                () -> idempotentCopy(srcKey, dstKey),
                ex -> log.error("Poison during copy: {}", copySubject, ex)
        );

        retryWithBackoff(
                deleteSubject,
                MOVE_RETRIES,
                () -> idempotentDelete(srcKey),
                ex -> log.error("Poison during delete: {}", deleteSubject, ex)
        );
    }

    private void idempotentCopy(String srcKey, String dstKey) throws Exception {
        if (objectExists(dstKey)) {
            return;
        }

        client.copyObject(
                CopyObjectArgs.builder()
                        .bucket(bucket)
                        .object(dstKey)
                        .source(
                                CopySource.builder()
                                        .bucket(bucket)
                                        .object(srcKey)
                                        .build()
                        )
                        .build()
        );
    }

    private void idempotentDelete(String srcKey) throws Exception {
        if (!objectExists(srcKey)) {
            return;
        }

        client.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(bucket)
                        .object(srcKey)
                        .build()
        );
    }

    private boolean objectExists(String objectKey) throws Exception {
        try {
            client.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucket)
                            .object(objectKey)
                            .build()
            );
            return true;
        } catch (ErrorResponseException e) {
            if (isNotFound(e)) return false;
            throw e;
        }
    }

    private boolean isNotFound(ErrorResponseException e) {
        var code = e.errorResponse() == null ? null : e.errorResponse().code();
        return "NoSuchKey".equals(code) || "NoSuchObject".equals(code);
    }

    public void close() {
        stopping = true;
        if (worker != null) worker.interrupt();
        waitForWorkerToFinish();
        log.info("S3FileInputPort for bucket {} shut down cleanly", bucket);
    }

    private void waitForWorkerToFinish() {
        if (worker != null && worker.isAlive()) {
            try {
                worker.join();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void validateConfiguration() {
        if (incomingPrefix.isEmpty()) {
            if (!handledPrefix.isEmpty() || !errorPrefix.isEmpty()) {
                throw new IllegalArgumentException("handledPrefix/errorPrefix must be empty when incomingPrefix is empty");
            }
            return;
        }

        if (handledPrefix.isEmpty() || errorPrefix.isEmpty()) {
            throw new IllegalArgumentException("handledPrefix and errorPrefix must be set when incomingPrefix is set");
        }

        if (incomingPrefix.equals(handledPrefix) || incomingPrefix.equals(errorPrefix)) {
            throw new IllegalArgumentException("handledPrefix/errorPrefix must differ from incomingPrefix");
        }
    }

    private void requireIncomingObjectKey(String objectKey) {
        if (objectKey == null || objectKey.isBlank()) {
            throw new IllegalStateException("objectKey is required");
        }
        if (!incomingPrefix.isEmpty() && !objectKey.startsWith(incomingPrefix)) {
            throw new IllegalStateException(
                    "Object key '%s' does not start with configured incomingPrefix '%s'".formatted(objectKey, incomingPrefix)
            );
        }
    }

    private static String normalizePrefix(String prefix) {
        var p = prefix == null ? "" : prefix.trim();
        if (p.isEmpty()) return "";
        return p.endsWith("/") ? p : (p + "/");
    }

    private static String requireNonBlank(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Bucket name is required");
        }
        return value.trim();
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}