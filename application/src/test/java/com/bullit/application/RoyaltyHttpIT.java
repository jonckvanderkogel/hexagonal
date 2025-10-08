package com.bullit.application;

import com.bullit.domain.model.royalty.RoyaltyScheme;
import com.bullit.domain.model.royalty.RoyaltyTier;
import com.bullit.web.adapter.driving.http.Response.RoyaltyReportResponse;
import com.bullit.web.adapter.driving.http.Response.ErrorResponse;
import com.bullit.web.adapter.driving.http.Response.SaleResponse;
import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@DirtiesContext
@TestExecutionListeners(
        value = {DbUnitTestExecutionListener.class},
        mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
@DbUnitConfiguration
@DatabaseSetup("/dbunit/royaltyHttpDataset.xml")
@Import(RoyaltyHttpIT.TestConfig.class)
class RoyaltyHttpIT extends AbstractIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String base(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void monthlyRoyalty_happyPath_returnsReport() {
        var authorId = "44444444-4444-4444-4444-444444444444";
        ResponseEntity<RoyaltyReportResponse> res =
                rest.getForEntity(base("/authors/{id}/royalties/{period}"),
                        RoyaltyReportResponse.class, authorId, "2025-03");

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        var body = res.getBody();
        assertThat(body).isNotNull();

        assertThat(body.authorId()).isEqualTo(authorId.toString());
        assertThat(body.period()).isEqualTo("2025-03");
        assertThat(body.totalUnits()).isEqualTo(120L);
        assertThat(body.grossRevenue()).isEqualByComparingTo(new BigDecimal("600.00"));

        // Tier breakdown
        assertThat(body.breakdown()).hasSize(3);
        assertThat(body.breakdown().getFirst().unitsInTier()).isEqualTo(100L);
        assertThat(body.breakdown().getFirst().royaltyAmount()).isEqualByComparingTo(new BigDecimal("50.00"));

        assertThat(body.breakdown().get(1).unitsInTier()).isEqualTo(20L);
        assertThat(body.breakdown().get(1).royaltyAmount()).isEqualByComparingTo(new BigDecimal("15.00"));

        assertThat(body.breakdown().get(2).unitsInTier()).isEqualTo(0L);
        assertThat(body.breakdown().get(2).royaltyAmount()).isEqualByComparingTo(new BigDecimal("0.00"));

        assertThat(body.minimumGuarantee()).isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(body.royaltyDue()).isEqualByComparingTo(new BigDecimal("100.00"));

        assertThat(body.effectiveRate())
                .isGreaterThan(new BigDecimal("0.16"))
                .isLessThan(new BigDecimal("0.17"));
    }

    @Test
    void monthlyRoyalty_unknownAuthor_returns404() {
        var missing = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        ResponseEntity<ErrorResponse> res =
                rest.getForEntity(base("/authors/{id}/royalties/{period}"),
                        ErrorResponse.class, missing, "2025-03");

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(res.getBody()).isNotNull();
        assertThat(res.getBody().error()).isNotBlank();
    }

    @Test
    void addSale_returns201() {
        var createReq = Map.of("bookId", "33333333-3333-3333-3333-333333333333", "units", "100", "amountEur", "550.15");
        ResponseEntity<SaleResponse> created = rest.postForEntity(base("/sale"), createReq, SaleResponse.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody()).isNotNull();
        assertThat(created.getBody().id()).isNotNull();
        assertThat(created.getBody().bookId()).isEqualTo(UUID.fromString("33333333-3333-3333-3333-333333333333"));
        assertThat(created.getBody().amountEur()).isEqualTo("550.15");
        assertThat(created.getBody().units()).isEqualTo(100);
        assertThat(created.getBody().soldAt()).isEqualTo(
                LocalDateTime.of(2024, 8, 13, 9, 0, 0)
                        .toInstant(ZoneOffset.UTC)
        );
    }

    @Test
    void invalidSale_returns400() {
        var createReq = Map.of("units", "100", "amountEur", "550.15");
        ResponseEntity<SaleResponse> created = rest.postForEntity(base("/sale"), createReq, SaleResponse.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        public RoyaltyScheme testRoyaltyScheme() {
            return RoyaltyScheme.of(
                    List.of(
                            RoyaltyTier.of(100, new BigDecimal("0.10")),
                            RoyaltyTier.of(500, new BigDecimal("0.15")),
                            RoyaltyTier.of(Long.MAX_VALUE, new BigDecimal("0.20"))
                    ),
                    new BigDecimal("100.00")
            );
        }

        @Bean
        @Primary
        public Clock clock() {
            return Clock
                    .fixed(
                            LocalDateTime.of(2024, 8, 13, 9, 0, 0)
                                    .toInstant(ZoneOffset.UTC),
                            ZoneOffset.UTC
                    );
        }
    }
}
