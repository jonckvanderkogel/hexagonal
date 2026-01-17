package com.bullit.application;

import com.bullit.application.file.FileConfigProperties;
import com.bullit.application.streaming.KafkaClientProperties;
import com.bullit.application.streaming.StreamConfigProperties;
import com.bullit.core.usecase.SaleEventKey;
import com.bullit.core.usecase.SaleFileToKafkaHandler;
import com.bullit.domain.event.SaleEvent;
import com.bullit.domain.model.royalty.Sale;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.bullit.application.TestUtils.awaitAssignment;
import static com.bullit.application.TestUtils.createTestConsumer;
import static com.bullit.application.TestUtils.objectExists;
import static com.bullit.application.TestUtils.pollForAnyRecord;
import static com.bullit.application.TestUtils.pollForSingleConsumerRecord;
import static com.bullit.application.TestUtils.putObject;
import static com.bullit.application.TestUtils.seekToEnd;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = RANDOM_PORT)
@Import(SaleFileToKafkaIT.TestConfig.class)
class SaleFileToKafkaIT {

    private static final String TOPIC = "raw-sales-sale-file-it";
    private static final String BUCKET = "raw-sales-sale-file-it";

    private static final String INCOMING_PREFIX = "incoming/";
    private static final String HANDLED_PREFIX = "handled/";
    private static final String ERROR_PREFIX = "error/";

    @Autowired
    KafkaClientProperties kafkaClientProperties;

    @Autowired
    MinioClient minioClient;

    @Test
    void puttingSaleFileOnS3_emitsSaleEventToKafka_andMovesFileToHandledPrefix() {
        var fileName = "sales-it-" + UUID.randomUUID() + ".csv";

        var incomingKey = INCOMING_PREFIX + fileName;
        var handledKey = HANDLED_PREFIX + fileName;

        var saleId = UUID.randomUUID();
        var bookId = UUID.fromString("55555555-5555-5555-5555-555555555555");

        var soldAt = "2024-08-13T09:00:00Z";
        var expectedSoldAtMillis = Instant.parse(soldAt).toEpochMilli();

        var csv = """
                id,bookId,units,amountEur,soldAt
                %s,%s,2,19.99,%s
                """.formatted(saleId, bookId, soldAt);

        try (var consumer = createTestConsumer(
                kafkaClientProperties,
                "sale-file-it-" + UUID.randomUUID(),
                SaleEvent.class,
                1000)
        ) {
            consumer.subscribe(List.of(TOPIC));

            putObject(minioClient, BUCKET, incomingKey, csv);

            var record = pollForSingleConsumerRecord(consumer, Duration.ofSeconds(5));
            var event = record.value();

            assertSoftly(s -> {
                s.assertThat(record.key())
                        .as("Kafka record key (StreamKey should be applied)")
                        .isEqualTo(saleId.toString());

                s.assertThat(event).isNotNull();
                s.assertThat(event.getId()).isEqualTo(saleId.toString());
                s.assertThat(event.getBookId()).isEqualTo(bookId.toString());
                s.assertThat(event.getUnits()).isEqualTo(2);
                s.assertThat(event.getAmountEur().toString()).isEqualTo("19.99");
                s.assertThat(event.getSoldAt()).isEqualTo(expectedSoldAtMillis);
            });

            assertSoftly(s -> {
                s.assertThat(objectExists(minioClient, BUCKET, handledKey))
                        .as("expected file to be moved to handled prefix")
                        .isTrue();
            });
        }
    }

    @Test
    void invalid_sale_file_is_moved_to_error_prefix_and_no_kafka_event_is_emitted() {
        var fileName = "sales-it-bad-" + UUID.randomUUID() + ".csv";

        var incomingKey = INCOMING_PREFIX + fileName;
        var errorKey = ERROR_PREFIX + fileName;

        var badCsv = """
                id,bookId,units,amountEur,soldAt
                %s,%s,,19.99,2024-08-13T09:00:00Z
                """.formatted(
                UUID.randomUUID(),
                UUID.fromString("55555555-5555-5555-5555-555555555555")
        );

        try (var consumer = createTestConsumer(
                kafkaClientProperties,
                "sale-file-it-bad-" + UUID.randomUUID(),
                SaleEvent.class,
                1000)
        ) {
            consumer.subscribe(List.of(TOPIC));

            awaitAssignment(consumer);
            seekToEnd(consumer);

            putObject(minioClient, BUCKET, incomingKey, badCsv);

            assertSoftly(s -> {
                s.assertThat(objectExists(minioClient, BUCKET, errorKey))
                        .as("expected file to be moved to error prefix")
                        .isTrue();
            });

            var anyRecord = pollForAnyRecord(consumer, Duration.ofSeconds(5));

            assertSoftly(s -> {
                s.assertThat(anyRecord)
                        .as("expected no kafka message for invalid file")
                        .isFalse();
            });
        }
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        @Primary
        public StreamConfigProperties streamConfigProperties() {
            return new StreamConfigProperties(
                    List.of(),
                    List.of(
                            new StreamConfigProperties.OutputConfig(
                                    SaleEvent.class,
                                    TOPIC,
                                    SaleEventKey.class
                            )
                    ),
                    List.of(),
                    List.of()
            );
        }

        @Bean
        @Primary
        public FileConfigProperties fileConfigProperties() {
            return new FileConfigProperties(
                    List.of(
                            new FileConfigProperties.InputConfig(
                                    Sale.class,
                                    BUCKET,
                                    INCOMING_PREFIX,
                                    HANDLED_PREFIX,
                                    ERROR_PREFIX,
                                    Duration.ofMillis(250)
                            )
                    ),
                    List.of(),
                    List.of(
                            new FileConfigProperties.HandlerConfig(SaleFileToKafkaHandler.class)
                    )
            );
        }
    }
}