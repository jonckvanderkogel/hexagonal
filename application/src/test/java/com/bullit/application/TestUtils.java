package com.bullit.application;

import com.bullit.application.streaming.KafkaClientProperties;
import com.bullit.domain.event.RoyaltyReportEvent;
import com.bullit.domain.event.SaleEvent;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

public class TestUtils {

    public static KafkaProducer<String, RoyaltyReportEvent> createTestProducer(
            KafkaClientProperties kafkaClientProperties
    ) {
        var props = kafkaClientProperties.buildProducerProperties();
        return new KafkaProducer<>(props);
    }

    public static <T> KafkaConsumer<String, T> createTestConsumer(
            KafkaClientProperties kafkaClientProperties,
            String groupId,
            Class<T> valueType
    ) {
        Properties props = kafkaClientProperties.buildConsumerProperties(groupId);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("specific.avro.value.type", valueType.getName());

        return new KafkaConsumer<>(props);
    }

    public static <T> T pollForSingleRecord(
            KafkaConsumer<String, T> consumer,
            Duration timeout
    ) {
        var deadline = System.currentTimeMillis() + timeout.toMillis();

        while (System.currentTimeMillis() < deadline) {
            var records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records.iterator().next().value();
            }
        }

        throw new AssertionError("No Kafka message received within timeout");
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
}