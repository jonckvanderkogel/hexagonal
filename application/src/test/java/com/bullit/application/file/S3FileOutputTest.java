package com.bullit.application.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class S3FileOutputTest {

    private static final String BUCKET = "test-bucket";
    private static final String OBJECT_KEY = "out/test.ndjson";

    private record TestPayload(String value) {
    }

    private S3FileOutput<TestPayload> out;

    @AfterEach
    void tearDown() {
        if (out != null) out.close();
    }

    @Test
    void emit_streams_ndjson_and_calls_putObject_once_with_expected_args() throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient client = mock(MinioClient.class);
        ObjectMapper mapper = new ObjectMapper();

        var wrote = new CountDownLatch(1);
        var capturedBody = new ByteArrayOutputStream();
        var putCaptor = ArgumentCaptor.forClass(PutObjectArgs.class);

        when(client.putObject(any(PutObjectArgs.class)))
                .thenAnswer(invocation -> {
                    PutObjectArgs args = invocation.getArgument(0);

                    // Drain the stream so the writer thread can complete.
                    args.stream().transferTo(capturedBody);

                    wrote.countDown();
                    return mock(ObjectWriteResponse.class);
                });

        out = new S3FileOutput<>(BUCKET, client, mapper);

        out.emit(List.of(new TestPayload("a"), new TestPayload("b")).stream(), OBJECT_KEY);

        boolean done = await(wrote, 1, TimeUnit.SECONDS);

        verify(client, times(1)).putObject(putCaptor.capture());
        var args = putCaptor.getValue();

        assertSoftly(s -> {
            s.assertThat(done).isTrue();

            s.assertThat(args.bucket()).isEqualTo(BUCKET);
            s.assertThat(args.object()).isEqualTo(OBJECT_KEY);

            s.assertThat(safeContentType(args)).isEqualTo("application/x-ndjson");

            var body = capturedBody.toString(StandardCharsets.UTF_8);
            s.assertThat(body).isEqualTo(
                    "{\"value\":\"a\"}\n" +
                            "{\"value\":\"b\"}\n"
            );
        });
    }

    @Test
    void close_then_emit_throws_and_does_not_call_putObject() {
        MinioClient client = mock(MinioClient.class);
        ObjectMapper mapper = new ObjectMapper();

        out = new S3FileOutput<>(BUCKET, client, mapper);
        out.close();

        assertSoftly(s -> {
            s.assertThatThrownBy(() -> out.emit(Stream.of(new TestPayload("x")), OBJECT_KEY))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("shutting down");

            s.check(() -> verify(client, never()).putObject(any(PutObjectArgs.class)));
        });
    }

    @Test
    void serialization_failure_is_propagated_and_putObject_is_attempted_once() throws IOException, ServerException, InsufficientDataException, ErrorResponseException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient client = mock(MinioClient.class);
        ObjectMapper mapper = mock(ObjectMapper.class);

        when(mapper.writeValueAsString(any()))
                .thenThrow(new RuntimeException("boom"));

        when(client.putObject(any(PutObjectArgs.class)))
                .thenAnswer(invocation -> {
                    PutObjectArgs args = invocation.getArgument(0);
                    args.stream().transferTo(new ByteArrayOutputStream());
                    return mock(ObjectWriteResponse.class);
                });

        out = new S3FileOutput<>(BUCKET, client, mapper);

        assertSoftly(s -> {
            s.assertThatThrownBy(() -> out.emit(Stream.of(new TestPayload("x")), OBJECT_KEY))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed writing S3 object " + BUCKET + "/" + OBJECT_KEY);

            s.check(
                    () -> verify(client, times(1))
                            .putObject(any(PutObjectArgs.class))
            );
        });
    }

    @Test
    void close_waits_for_inflight_emit_to_finish() throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient client = mock(MinioClient.class);
        ObjectMapper mapper = new ObjectMapper();

        var allowPutToReturn = new CountDownLatch(1);
        var putStarted = new CountDownLatch(1);

        when(client.putObject(any(PutObjectArgs.class)))
                .thenAnswer(invocation -> {
                    putStarted.countDown();

                    // Block the putObject call to keep inFlight > 0 for a while.
                    await(allowPutToReturn, 2, TimeUnit.SECONDS);

                    // Drain stream to let writer complete / join cleanly.
                    PutObjectArgs args = invocation.getArgument(0);
                    args.stream().transferTo(new ByteArrayOutputStream());

                    return mock(ObjectWriteResponse.class);
                });

        out = new S3FileOutput<>(BUCKET, client, mapper);

        var emitFinished = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            try {
                out.emit(Stream.of(new TestPayload("x")), OBJECT_KEY);
            } finally {
                emitFinished.countDown();
            }
        });

        // Ensure emit reached putObject (so inFlight is definitely > 0).
        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(
                        () -> assertThat(await(putStarted, 1, TimeUnit.SECONDS))
                                .isTrue()
                );

        var closeFinished = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            out.close();
            closeFinished.countDown();
        });

        // close should be blocked while putObject is blocked
        assertThat(await(closeFinished, 150, TimeUnit.MILLISECONDS)).isFalse();

        // release putObject, then both emit + close should complete
        allowPutToReturn.countDown();

        assertThat(await(emitFinished, 2, TimeUnit.SECONDS)).isTrue();
        assertThat(await(closeFinished, 2, TimeUnit.SECONDS)).isTrue();
    }

    private static boolean await(CountDownLatch latch, long time, TimeUnit unit) {
        try {
            return latch.await(time, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String safeContentType(PutObjectArgs args) {
        try {
            return args.contentType();
        } catch (IOException e) {
            throw new AssertionError("PutObjectArgs.contentType() threw IOException", e);
        }
    }
}