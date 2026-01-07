package com.bullit.application;

import com.bullit.application.file.FileConfigProperties;
import com.bullit.application.streaming.KafkaClientProperties;
import com.bullit.application.streaming.StreamConfigProperties;
import com.bullit.core.usecase.SaleFileToKafkaHandler;
import com.bullit.domain.event.SaleEvent;
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
import java.util.List;
import java.util.UUID;

import static com.bullit.application.TestUtils.createTestConsumer;
import static com.bullit.application.TestUtils.objectExists;
import static com.bullit.application.TestUtils.pollForSingleRecord;
import static com.bullit.application.TestUtils.putObject;
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
        var fileName = "sales-it-" + UUID.randomUUID() + ".ndjson";

        var incomingKey = INCOMING_PREFIX + fileName;
        var handledKey = HANDLED_PREFIX + fileName;

        var saleId = UUID.randomUUID();
        var bookId = UUID.fromString("55555555-5555-5555-5555-555555555555");

        var ndjson = """
                {"id":"%s","bookId":"%s","units":2,"amountEur":19.99,"soldAt":"2024-08-13T09:00:00Z"}
                """.formatted(saleId, bookId);

        try (var consumer = createTestConsumer(
                kafkaClientProperties,
                "sale-file-it-" + UUID.randomUUID(),
                SaleEvent.class)
        ) {
            consumer.subscribe(List.of(TOPIC));

            putObject(minioClient, BUCKET, incomingKey, ndjson);

            var event = pollForSingleRecord(consumer);

            assertSoftly(s -> {
                s.assertThat(event).isNotNull();
                s.assertThat(event.getId()).isEqualTo(saleId.toString());
                s.assertThat(event.getBookId()).isEqualTo(bookId.toString());
                s.assertThat(event.getUnits()).isEqualTo(2);
                s.assertThat(event.getAmountEur().toString()).isEqualTo("19.99");
                s.assertThat(event.getSoldAt()).isGreaterThan(0L);
            });

            assertSoftly(s -> {
                s.assertThat(objectExists(minioClient, BUCKET, handledKey))
                        .as("expected file to be moved to handled prefix")
                        .isTrue();
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
                                    com.bullit.domain.event.SaleEvent.class,
                                    TOPIC
                            )),
                    List.of()
            );
        }

        @Bean
        @Primary
        public FileConfigProperties fileConfigProperties() {
            return new FileConfigProperties(
                    List.of(
                            new FileConfigProperties.InputConfig(
                                    com.bullit.domain.model.royalty.Sale.class,
                                    BUCKET,
                                    INCOMING_PREFIX,
                                    HANDLED_PREFIX,
                                    ERROR_PREFIX,
                                    Duration.ofMillis(250)
                            )),
                    List.of(),
                    List.of(
                            new FileConfigProperties.HandlerConfig(SaleFileToKafkaHandler.class)
                    )
            );
        }
    }
}