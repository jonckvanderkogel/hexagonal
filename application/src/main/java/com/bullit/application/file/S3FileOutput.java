package com.bullit.application.file;

import com.bullit.domain.port.driven.file.FileOutputPort;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public final class S3FileOutput<T> implements FileOutputPort<T> {

    private static final Logger log = LoggerFactory.getLogger(S3FileOutput.class);

    private static final int PIPE_BUFFER_BYTES = 64 * 1024;
    private static final long PART_SIZE_BYTES = 10L * 1024 * 1024; // 10 MiB

    private final String bucket;
    private final MinioClient client;
    private final ObjectMapper mapper;

    private volatile boolean stopping = false;

    public S3FileOutput(String bucket, MinioClient client, ObjectMapper mapper) {
        this.bucket = bucket;
        this.client = Objects.requireNonNull(client, "client is required");
        this.mapper = Objects.requireNonNull(mapper, "mapper is required");
    }

    @Override
    public void emit(Stream<T> contents, String objectKey) {
        if (stopping) throw new IllegalStateException("S3FileOutput is shutting down");

        try (contents) {
            putObjectStreaming(objectKey, contents);
            log.info("Wrote S3 object {}/{}", bucket, objectKey);
        }
    }

    private void putObjectStreaming(String objectKey, Stream<T> contents) {
        try {
            putObjectOnce(objectKey, contents);
        } catch (Exception e) {
            throw new IllegalStateException("Failed writing S3 object %s/%s".formatted(bucket, objectKey), e);
        }
    }

    private void putObjectOnce(String objectKey, Stream<T> contents) throws Exception {
        try (var in = new PipedInputStream(PIPE_BUFFER_BYTES);
             var out = new PipedOutputStream(in)) {

            var writerFailure = new AtomicReference<Throwable>(null);
            var writer = startWriter(contents, out, writerFailure);

            try {
                client.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucket)
                                .object(objectKey)
                                .stream(in, -1, PART_SIZE_BYTES)
                                .contentType("application/x-ndjson")
                                .build()
                );
            } finally {
                join(writer);
            }

            throwIfWriterFailed(writerFailure);
        }
    }

    private Thread startWriter(Stream<T> contents, PipedOutputStream out, AtomicReference<Throwable> writerFailure) {
        return Thread.ofVirtual().start(() -> writeJsonLines(contents, out, writerFailure));
    }

    private void writeJsonLines(Stream<T> contents, PipedOutputStream out, AtomicReference<Throwable> writerFailure) {
        try (var writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            contents.forEach(v -> writeLine(writer, v));
            writer.flush();
        } catch (Throwable t) {
            writerFailure.set(t);
            safeClose(out);
        }
    }

    private void writeLine(BufferedWriter writer, T value) {
        try {
            writer.write(mapper.writeValueAsString(value));
            writer.newLine();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize payload to json", e);
        }
    }

    private void throwIfWriterFailed(AtomicReference<Throwable> writerFailure) {
        var t = writerFailure.get();
        switch (t) {
            case null -> {
                return;
            }
            case RuntimeException re -> throw re;
            case Error err -> throw err;
            default -> {
            }
        }

        throw new IllegalStateException("Failed while streaming json lines", t);
    }

    public void close() {
        stopping = true;
        log.info("S3FileOutput shut down cleanly");
    }

    private static void join(Thread t) {
        if (t != null && t.isAlive()) {
            try {
                t.join();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void safeClose(PipedOutputStream out) {
        try {
            out.close();
        } catch (Exception ignored) {
        }
    }
}