package com.bullit.application;

import com.bullit.application.file.FileConfigProperties;
import com.bullit.application.streaming.KafkaClientProperties;
import com.bullit.application.streaming.StreamConfigProperties;
import com.bullit.core.usecase.RoyaltyReportToS3FileHandler;
import com.bullit.domain.event.RoyaltyReportEvent;
import com.bullit.domain.event.TierBreakdownEvent;
import io.minio.MinioClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static com.bullit.application.TestUtils.createTestProducer;
import static com.bullit.application.TestUtils.objectExists;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@ActiveProfiles("test")
@SpringBootTest(webEnvironment = RANDOM_PORT)
@Import(RoyaltyReportToS3FileIT.TestConfig.class)
class RoyaltyReportToS3FileIT {

    private static final String INPUT_TOPIC = "author-royalties-royalty-report-it";
    private static final String OUTPUT_TOPIC = "raw-sales-sale-file-it";
    private static final String BUCKET = "royalty-reports-royalty-report-it";

    @Autowired
    KafkaClientProperties kafkaClientProperties;

    @Autowired
    MinioClient minioClient;

    @Test
    void producingRoyaltyReportEvent_writesJsonFileToS3() {
        var authorId = UUID.randomUUID().toString();
        var period = "2024-08";

        var expectedKey = "%s/%s.json".formatted(authorId, period);

        var event = RoyaltyReportEvent.newBuilder()
                .setAuthorId(authorId)
                .setPeriod(period)
                .setUnits(120L)
                .setGrossRevenue(new BigDecimal("600.00"))
                .setEffectiveRate(new BigDecimal("0.17"))
                .setRoyaltyDue(new BigDecimal("100.00"))
                .setMinimumGuarantee(new BigDecimal("100.00"))
                .setBreakdown(List.of(
                        TierBreakdownEvent.newBuilder()
                                .setUnitsInTier(100L)
                                .setAppliedRate(new BigDecimal("0.10"))
                                .setRoyaltyAmount(new BigDecimal("50.00"))
                                .build()
                ))
                .build();

        publish(event);

        assertSoftly(s -> {
            s.assertThat(objectExists(minioClient, BUCKET, expectedKey))
                    .as("expected royalty report file to exist in S3")
                    .isTrue();
        });
    }

    private void publish(RoyaltyReportEvent event) {
        try (var producer = createTestProducer(kafkaClientProperties)) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, event)).get();
            producer.flush();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish RoyaltyReportEvent", e);
        }
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        @Primary
        public StreamConfigProperties streamConfigProperties() {
            return new StreamConfigProperties(
                    List.of(
                            new StreamConfigProperties.InputConfig(
                                    com.bullit.domain.event.RoyaltyReportEvent.class,
                                    INPUT_TOPIC,
                                    "royalty-report-to-s3-it"
                            )
                    ),
                    List.of(
                            new StreamConfigProperties.OutputConfig(
                                    com.bullit.domain.event.SaleEvent.class,
                                    OUTPUT_TOPIC
                            )), // this one is needed for the royaltyServicePort in BeansConfig
                    List.of(
                            new StreamConfigProperties.HandlerConfig(RoyaltyReportToS3FileHandler.class)
                    )
            );
        }

        @Bean
        @Primary
        public FileConfigProperties fileConfigProperties() {
            return new FileConfigProperties(
                    List.of(),
                    List.of(
                            new FileConfigProperties.OutputConfig(
                                    com.bullit.domain.event.RoyaltyReportEvent.class,
                                    BUCKET
                            )
                    ),
                    List.of()
            );
        }
    }
}