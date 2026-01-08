package com.bullit.application.file;

import com.bullit.domain.port.driven.file.CsvRecordMapping;
import com.bullit.domain.port.driven.file.FileOutputPort;
import de.siegmar.fastcsv.writer.CsvWriter;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.bullit.application.FunctionUtils.sleep;

public final class S3FileOutput<T> implements FileOutputPort<T> {

    private static final Logger log = LoggerFactory.getLogger(S3FileOutput.class);

    private static final int PIPE_BUFFER_BYTES = 64 * 1024;
    private static final long PART_SIZE_BYTES = 10L * 1024 * 1024; // 10 MiB
    private static final Duration SHUTDOWN_POLL = Duration.ofMillis(50);

    private final String bucket;
    private final MinioClient client;

    private final AtomicInteger inFlight = new AtomicInteger(0);
    private volatile boolean stopping = false;

    public S3FileOutput(String bucket, MinioClient client) {
        this.bucket = bucket;
        this.client = Objects.requireNonNull(client, "client is required");
    }

    @Override
    public void emit(Stream<T> contents, String objectKey, CsvRecordMapping<T> mapping) {
        throwIfStopping();

        inFlight.incrementAndGet();
        try (contents) {
            putObjectStreaming(objectKey, contents, mapping);
            log.info("Wrote S3 object {}/{}", bucket, objectKey);
        } finally {
            inFlight.decrementAndGet();
        }
    }

    public void close() {
        stopping = true;
        waitForInflightToFinish();
        log.info("S3FileOutput shut down cleanly");
    }

    private void throwIfStopping() {
        if (!stopping) return;
        throw new IllegalStateException("S3FileOutput is shutting down");
    }

    private void putObjectStreaming(String objectKey, Stream<T> contents, CsvRecordMapping<T> mapping) {
        try {
            putObjectOnce(objectKey, contents, mapping);
        } catch (Exception e) {
            throw new IllegalStateException("Failed writing S3 object %s/%s".formatted(bucket, objectKey), e);
        }
    }

    private void putObjectOnce(String objectKey, Stream<T> contents, CsvRecordMapping<T> mapping) throws Exception {
        try (var in = new PipedInputStream(PIPE_BUFFER_BYTES);
             var out = new PipedOutputStream(in)) {

            var writerFailure = new AtomicReference<Throwable>(null);
            var writer = startWriter(contents, out, mapping, writerFailure);

            try {
                client.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucket)
                                .object(objectKey)
                                .stream(in, -1, PART_SIZE_BYTES)
                                .contentType("text/csv; charset=utf-8")
                                .build()
                );
            } finally {
                join(writer);
            }

            throwIfWriterFailed(writerFailure);
        }
    }

    private Thread startWriter(
            Stream<T> contents,
            PipedOutputStream out,
            CsvRecordMapping<T> mapping,
            AtomicReference<Throwable> writerFailure
    ) {
        return Thread.ofVirtual().start(() -> writeCsv(contents, out, mapping, writerFailure));
    }

    private void writeCsv(
            Stream<T> contents,
            PipedOutputStream out,
            CsvRecordMapping<T> mapping,
            AtomicReference<Throwable> writerFailure
    ) {
        try (var buffered = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
             var csv = CsvWriter.builder().build(buffered)
        ) {
            writeHeader(csv, mapping.header());
            contents.map(mapping.toFields()).forEach(fields -> writeRecord(csv, fields));
            buffered.flush();
        } catch (Throwable t) {
            writerFailure.set(t);
            safeClose(out);
        }
    }

    private void writeHeader(CsvWriter csv, List<String> header) {
        if (header.isEmpty()) return;
        writeRecord(csv, header);
    }

    private void writeRecord(CsvWriter csv, List<String> fields) {
        csv.writeRecord(fields);
    }

    private void throwIfWriterFailed(AtomicReference<Throwable> writerFailure) {
        var t = writerFailure.get();
        if (t == null) return;

        if (t instanceof RuntimeException re) throw re;
        if (t instanceof Error err) throw err;

        throw new IllegalStateException("Failed while streaming csv", t);
    }

    private void waitForInflightToFinish() {
        while (inFlight.get() > 0 && !Thread.currentThread().isInterrupted()) {
            sleep(SHUTDOWN_POLL);
        }
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