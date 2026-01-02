package com.bullit.application.file;

import com.bullit.domain.port.driven.file.FileOutputPort;
import com.bullit.domain.port.driven.file.FileTarget;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.PutObjectArgs;
import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.bullit.application.FunctionUtils.retryWithBackoff;

public final class S3FileOutput<T> implements FileOutputPort<T> {

    private static final Logger log = LoggerFactory.getLogger(S3FileOutput.class);
    private static final int PUT_RETRIES = 5;

    private final MinioClient client;
    private final ObjectMapper mapper;

    public S3FileOutput(MinioClient client, ObjectMapper mapper) {
        this.client = client;
        this.mapper = mapper;
    }

    @Override
    public void emit(Stream<T> contents, Supplier<FileTarget> targetSupplier) {
        var target = targetSupplier.get();
        var subject = "writing S3 object %s/%s".formatted(target.bucket(), target.objectKey());

        retryWithBackoff(
                subject,
                PUT_RETRIES,
                () -> putObject(target, toJsonArray(contents)),
                e -> log.error("Poison during put: {}", subject, e)
        );
    }

    private void putObject(FileTarget target, byte[] bytes) throws Exception {
        try (var in = new ByteArrayInputStream(bytes)) {
            client.putObject(
                    PutObjectArgs.builder()
                            .bucket(target.bucket())
                            .object(target.objectKey())
                            .stream(in, bytes.length, -1)
                            .contentType("application/json")
                            .build()
            );
        }
        log.info("Wrote S3 object {}/{} ({} bytes)", target.bucket(), target.objectKey(), bytes.length);
    }

    private byte[] toJsonArray(Stream<T> contents) {
        var body = contents
                .map(this::toJson)
                .collect(Collectors.joining("\n"));

        return body.getBytes(StandardCharsets.UTF_8);
    }

    private String toJson(T value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize payload to json", e);
        }
    }
}