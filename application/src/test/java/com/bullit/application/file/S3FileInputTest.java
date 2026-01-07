package com.bullit.application.file;

import com.bullit.domain.port.driven.file.FileEnvelope;
import com.bullit.domain.port.driven.file.FileLocation;
import com.bullit.domain.port.driving.file.FileHandler;
import io.minio.CopyObjectArgs;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.ErrorResponse;
import io.minio.messages.Item;
import okhttp3.Headers;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class S3FileInputTest {

    private static final String BUCKET = "raw-sales";
    private static final Duration POLL_INTERVAL = Duration.ofMillis(10);

    private S3FileInput<String> in;

    @AfterEach
    void tearDown() {
        if (in != null) in.close();
    }

    @Test
    void subscribing_registers_handler_and_starts_polling_loop_once() {
        MinioClient client = mock(MinioClient.class);
        when(client.listObjects(any(ListObjectsArgs.class))).thenReturn(List.of());

        @SuppressWarnings("unchecked")
        FileHandler<String> handler = mock(FileHandler.class);

        in = new S3FileInput<>(client, BUCKET, "incoming/", "handled/", "error/", POLL_INTERVAL);
        in.subscribe(handler);

        assertSoftly(s -> {
            s.assertThatThrownBy(() -> in.subscribe(handler))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already");

            // prove the loop actually runs
            s.check(() -> verify(client, atLeastOnce()).listObjects(any(ListObjectsArgs.class)));
        });
    }

    @Test
    void valid_object_is_handled_then_copied_to_handled_and_source_deleted() throws Exception {
        MinioClient client = mock(MinioClient.class);

        var objectKey = "incoming/file1.ndjson";
        var handledKey = "handled/file1.ndjson";

        when(client.listObjects(any(ListObjectsArgs.class)))
                .thenReturn(List.of(resultOf(fileItem(objectKey))))
                .thenReturn(List.of());

        when(client.getObject(any(GetObjectArgs.class)))
                .thenReturn(getObjectResponse(objectKey, "a\nb\n"));

        // dest missing => copy runs; source exists => delete runs
        when(client.statObject(any(StatObjectArgs.class))).thenAnswer(inv -> {
            StatObjectArgs args = inv.getArgument(0);
            if (handledKey.equals(args.object())) {
                throw notFound("NoSuchKey");
            }
            if (objectKey.equals(args.object())) {
                return mock(StatObjectResponse.class);
            }
            return mock(StatObjectResponse.class);
        });

        var handled = new CountDownLatch(1);
        var capturedLines = new AtomicReference<List<String>>();

        @SuppressWarnings("unchecked")
        FileHandler<String> handler = mock(FileHandler.class);
        captureHandledEnvelope(handler, handled, capturedLines);

        in = new S3FileInput<>(client, BUCKET, "incoming/", "handled/", "error/", POLL_INTERVAL);
        in.subscribe(handler);

        boolean done = await(handled, 1, TimeUnit.SECONDS);
        Thread.sleep(50);

        var copyCaptor = ArgumentCaptor.forClass(CopyObjectArgs.class);
        var delCaptor = ArgumentCaptor.forClass(RemoveObjectArgs.class);

        assertSoftly(s -> {
            s.assertThat(done).isTrue();

            s.check(() -> verify(handler, times(1)).handle(any(FileEnvelope.class)));

            s.assertThat(capturedLines.get()).containsExactly("a", "b");

            s.check(() -> verify(client, atLeastOnce()).copyObject(copyCaptor.capture()));
            s.check(() -> verify(client, atLeastOnce()).removeObject(delCaptor.capture()));

            var copy = copyCaptor.getAllValues().getLast();
            s.assertThat(copy.bucket()).isEqualTo(BUCKET);
            s.assertThat(copy.object()).isEqualTo(handledKey);
            s.assertThat(copy.source().bucket()).isEqualTo(BUCKET);
            s.assertThat(copy.source().object()).isEqualTo(objectKey);

            var del = delCaptor.getAllValues().getLast();
            s.assertThat(del.bucket()).isEqualTo(BUCKET);
            s.assertThat(del.object()).isEqualTo(objectKey);
        });
    }

    @Test
    void handler_failure_retries_then_moves_to_error_and_deletes_source() throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient client = mock(MinioClient.class);

        var objectKey = "incoming/bad.ndjson";
        var errorKey = "error/bad.ndjson";

        when(client.listObjects(any(ListObjectsArgs.class)))
                .thenReturn(List.of(resultOf(fileItem(objectKey))))
                .thenReturn(List.of());

        when(client.getObject(any(GetObjectArgs.class)))
                .thenReturn(getObjectResponse(objectKey, "x\n"));

        StatObjectResponse exists = mock(StatObjectResponse.class);

        // dest missing => copy; src exists => delete
        when(client.statObject(any(StatObjectArgs.class))).thenAnswer(inv -> {
            StatObjectArgs args = inv.getArgument(0);
            return switch (args.object()) {
                case "error/bad.ndjson" -> throw notFound("NoSuchKey");
                case "incoming/bad.ndjson" -> exists;
                default -> exists;
            };
        });

        @SuppressWarnings("unchecked")
        FileHandler<String> handler = mock(FileHandler.class);
        doThrow(new RuntimeException("fail")).when(handler).handle(any(FileEnvelope.class));

        in = new S3FileInput<>(client, BUCKET, "incoming/", "handled/", "error/", POLL_INTERVAL);
        in.subscribe(handler);

        // Wait until we see at least 3 attempts and the move-to-error side effects.
        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> verify(handler, atLeast(3)).handle(any(FileEnvelope.class)));

        var copyCaptor = ArgumentCaptor.forClass(CopyObjectArgs.class);
        var delCaptor = ArgumentCaptor.forClass(RemoveObjectArgs.class);

        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    verify(client, atLeastOnce()).copyObject(copyCaptor.capture());
                    verify(client, atLeastOnce()).removeObject(delCaptor.capture());
                });

        assertSoftly(s -> {
            var copy = copyCaptor.getAllValues().getLast();
            s.assertThat(copy.bucket()).isEqualTo(BUCKET);
            s.assertThat(copy.object()).isEqualTo(errorKey);
            s.assertThat(copy.source().bucket()).isEqualTo(BUCKET);
            s.assertThat(copy.source().object()).isEqualTo(objectKey);

            var del = delCaptor.getAllValues().getLast();
            s.assertThat(del.bucket()).isEqualTo(BUCKET);
            s.assertThat(del.object()).isEqualTo(objectKey);
        });
    }

    @Test
    void source_missing_short_circuits_delete_and_dest_existing_skips_copy() throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient client = mock(MinioClient.class);

        var objectKey = "incoming/file2.ndjson";

        when(client.listObjects(any(ListObjectsArgs.class)))
                .thenReturn(List.of(resultOf(fileItem(objectKey))))
                .thenReturn(List.of());

        when(client.getObject(any(GetObjectArgs.class)))
                .thenReturn(getObjectResponse(objectKey, "ok\n"));

        StatObjectResponse exists = mock(StatObjectResponse.class);

        // dest exists => copy skipped; src missing => delete skipped
        when(client.statObject(any(StatObjectArgs.class))).thenAnswer(inv -> {
            StatObjectArgs args = inv.getArgument(0);
            return switch (args.object()) {
                case "handled/file2.ndjson" -> exists;
                case "incoming/file2.ndjson" -> throw notFound("NoSuchKey");
                default -> exists;
            };
        });

        @SuppressWarnings("unchecked")
        FileHandler<String> handler = mock(FileHandler.class);

        in = new S3FileInput<>(client, BUCKET, "incoming/", "handled/", "error/", POLL_INTERVAL);
        in.subscribe(handler);

        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> verify(handler, times(1)).handle(any(FileEnvelope.class)));

        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(client, never()).copyObject(any(CopyObjectArgs.class));
                    verify(client, never()).removeObject(any(RemoveObjectArgs.class));
                });
    }

    @Test
    void unwrap_failure_is_ignored_and_other_items_still_processed() throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient client = mock(MinioClient.class);

        var goodKey = "incoming/good.ndjson";

        var badResult = throwingResult(new RuntimeException("boom"));
        var goodResult = resultOf(fileItem(goodKey));

        when(client.listObjects(any(ListObjectsArgs.class)))
                .thenReturn(List.of(badResult, goodResult))
                .thenReturn(List.of());

        when(client.getObject(any(GetObjectArgs.class)))
                .thenReturn(getObjectResponse(goodKey, "1\n2\n"));

        when(client.statObject(any(StatObjectArgs.class)))
                .thenReturn(mock(StatObjectResponse.class));

        @SuppressWarnings("unchecked")
        FileHandler<String> handler = mock(FileHandler.class);

        in = new S3FileInput<>(client, BUCKET, "incoming/", "handled/", "error/", POLL_INTERVAL);
        in.subscribe(handler);

        Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> verify(handler, times(1)).handle(any(FileEnvelope.class)));

        verify(client, atLeastOnce()).getObject(any(GetObjectArgs.class));
    }

    @Test
    void wrong_prefix_does_not_invoke_handler_or_getObject() throws Exception {
        MinioClient client = mock(MinioClient.class);

        var wrongKey = "not-incoming/file.ndjson";

        when(client.listObjects(any(ListObjectsArgs.class)))
                .thenReturn(List.of(resultOf(fileItem(wrongKey))))
                .thenReturn(List.of());

        @SuppressWarnings("unchecked")
        FileHandler<String> handler = mock(FileHandler.class);

        in = new S3FileInput<>(client, BUCKET, "incoming/", "handled/", "error/", POLL_INTERVAL);
        in.subscribe(handler);

        Thread.sleep(500);

        assertSoftly(s -> {
            s.check(() -> verify(client, never()).getObject(any(GetObjectArgs.class)));
            s.check(() -> verify(handler, never()).handle(any(FileEnvelope.class)));
        });
    }

    private static boolean await(CountDownLatch latch, long time, TimeUnit unit) {
        try {
            return latch.await(time, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void captureHandledEnvelope(
            FileHandler<?> handler,
            CountDownLatch handled,
            AtomicReference<List<String>> capturedLines
    ) {
        doAnswer(invocation -> {
            FileEnvelope env = invocation.getArgument(0);

            List<String> lines = env.lines().toList();
            capturedLines.set(lines);

            assertSoftly(s -> {
                FileLocation loc = env.location();
                s.assertThat(loc.bucket()).isEqualTo(BUCKET);
                s.assertThat(loc.objectKey()).isNotBlank();
            });

            handled.countDown();
            return null; // void method => Answer<Void>
        }).when(handler).handle(any(FileEnvelope.class));
    }

    private static Result<Item> resultOf(Item item) {
        return new Result<>(item);
    }

    private static Result<Item> throwingResult(RuntimeException ex) {
        return new Result<>(ex);
    }

    private static Item fileItem(String objectKey) {
        return new TestItem(objectKey, false);
    }

    private static final class TestItem extends Item {
        private final String objectKey;
        private final boolean isDir;

        private TestItem(String objectKey, boolean isDir) {
            super();
            this.objectKey = objectKey;
            this.isDir = isDir;
        }

        @Override
        public String objectName() {
            return objectKey;
        }

        @Override
        public boolean isDir() {
            return isDir;
        }
    }

    private static GetObjectResponse getObjectResponse(String objectKey, String contents) {
        var bytes = contents.getBytes(StandardCharsets.UTF_8);
        return new GetObjectResponse(
                Headers.of(Map.of()),
                BUCKET,
                null,
                objectKey,
                new ByteArrayInputStream(bytes)
        );
    }

    private static ErrorResponseException notFound(String code) {
        var ex = mock(ErrorResponseException.class);
        var err = mock(ErrorResponse.class);
        when(err.code()).thenReturn(code);
        when(ex.errorResponse()).thenReturn(err);
        return ex;
    }
}