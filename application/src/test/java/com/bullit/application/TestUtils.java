package com.bullit.application;

import com.bullit.application.streaming.KafkaClientProperties;
import com.bullit.domain.event.RoyaltyReportEvent;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtils {
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    public static KafkaProducer<String, RoyaltyReportEvent> createTestProducer(
            KafkaClientProperties kafkaClientProperties
    ) {
        var props = kafkaClientProperties.buildProducerProperties();
        return new KafkaProducer<>(props);
    }

    public static <T> KafkaConsumer<String, T> createTestConsumer(
            KafkaClientProperties kafkaClientProperties,
            String groupId,
            Class<T> valueType,
            int partitionQueueCapacity
    ) {
        Properties props = kafkaClientProperties.buildConsumerProperties(groupId, partitionQueueCapacity);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("specific.avro.value.type", valueType.getName());

        return new KafkaConsumer<>(props);
    }

    public static <T> ConsumerRecord<String, T> pollForSingleConsumerRecord(
            KafkaConsumer<String, T> consumer,
            Duration timeout
    ) {
        var deadline = System.currentTimeMillis() + timeout.toMillis();

        while (System.currentTimeMillis() < deadline) {
            var records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }

        throw new AssertionError("No Kafka message received within timeout");
    }

    public static <T> T pollForSingleRecord(
            KafkaConsumer<String, T> consumer,
            Duration timeout
    ) {
        return pollForSingleConsumerRecord(consumer, timeout).value();
    }

    public static <T> boolean pollForAnyRecord(
            KafkaConsumer<String, T> consumer,
            Duration timeout
    ) {
        var deadline = System.currentTimeMillis() + timeout.toMillis();

        while (System.currentTimeMillis() < deadline) {
            var records = consumer.poll(Duration.ofMillis(250));
            if (!records.isEmpty()) {
                return true;
            }
        }

        return false;
    }

    public static void awaitAssignment(KafkaConsumer<?, ?> consumer) {
        var deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200));
            if (!consumer.assignment().isEmpty()) {
                return;
            }
        }

        throw new AssertionError("Kafka consumer did not receive partition assignment within timeout");
    }

    public static void seekToEnd(KafkaConsumer<?, ?> consumer) {
        consumer.poll(Duration.ofMillis(200));
        consumer.seekToEnd(consumer.assignment());
        consumer.poll(Duration.ofMillis(200));
    }

    public static void putObject(MinioClient minioClient,
                                 String bucket,
                                 String objectKey,
                                 String body
    ) {
        var bytes = body.getBytes(StandardCharsets.UTF_8);

        try (var in = new ByteArrayInputStream(bytes)) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(objectKey)
                            .stream(in, bytes.length, -1)
                            .contentType("text/csv; charset=utf-8")
                            .build()
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to put S3 object %s/%s".formatted(bucket, objectKey), e);
        }
    }

    public static boolean objectExists(MinioClient minioClient,
                                       String bucket,
                                       String objectKey
    ) {
        var deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            if (statObject(minioClient, bucket, objectKey)) {
                return true;
            }
            sleep(Duration.ofMillis(200));
        }

        return false;
    }

    public static String readObject(MinioClient minioClient, String bucket, String objectKey) {
        try (var in = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucket)
                        .object(objectKey)
                        .build()
        )) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to read S3 object %s/%s".formatted(bucket, objectKey),
                    e
            );
        }
    }

    public static String normalizeNewlines(String s) {
        return s.replace("\r\n", "\n").replace("\r", "\n");
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    public static boolean statObject(MinioClient minioClient,
                                     String bucket,
                                     String objectKey
    ) {
        try {
            minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucket)
                            .object(objectKey)
                            .build()
            );
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    public static <T> Function<T, String> nullKeyFunction() {
        return _ -> null;
    }

    public static <T> Set<ConstraintViolation<T>> validate(T value) {
        return VALIDATOR.validate(value);
    }

    public static List<String> messages(Set<? extends ConstraintViolation<?>> violations) {
        return violations.stream()
                .map(ConstraintViolation::getMessage)
                .toList();
    }

    public static void anyMessageContains(
            Set<? extends ConstraintViolation<?>> violations,
            String expectedSubstring
    ) {
        assertThat(messages(violations))
                .anySatisfy(m -> assertThat(m).contains(expectedSubstring));
    }

    public static void assertNoViolations(Set<? extends ConstraintViolation<?>> violations) {
        assertThat(violations).isEmpty();
    }
}