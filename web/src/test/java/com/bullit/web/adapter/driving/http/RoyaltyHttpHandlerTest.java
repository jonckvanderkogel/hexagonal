package com.bullit.web.adapter.driving.http;

import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.model.royalty.TierBreakdown;
import com.bullit.domain.port.driving.RoyaltyServicePort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.function.HandlerFilterFunction;
import org.springframework.web.servlet.function.ServerResponse;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

final class RoyaltyHttpHandlerTest extends AbstractHttpTest {

    private final RoyaltyServicePort royaltyService = mock(RoyaltyServicePort.class);

    private RoyaltyHttpHandler handler;
    private HandlerFilterFunction<ServerResponse, ServerResponse> errorFilter;

    @BeforeEach
    void setUp() {
        handler = new RoyaltyHttpHandler(royaltyService);
        errorFilter = new HttpErrorFilter();
    }

    @Test
    void getMonthlyRoyalty_ok_returns200_and_body() throws Exception {
        var authorId = UUID.randomUUID();
        var period = YearMonth.of(2025, 1);

        var breakdown = List.of(
                TierBreakdown.of(500, new BigDecimal("0.07"), new BigDecimal("350.00")),
                TierBreakdown.of(300, new BigDecimal("0.10"), new BigDecimal("300.00")),
                TierBreakdown.of(200, new BigDecimal("0.12"), new BigDecimal("240.00"))
        );

        var report = RoyaltyReport.of(
                authorId,
                period,
                1000L,
                new BigDecimal("10000.00"),
                new BigDecimal("0.0890"),
                new BigDecimal("890.00"),
                new BigDecimal("500.00"),
                breakdown
        );

        when(royaltyService.generateMonthlyReport(authorId, period)).thenReturn(report);

        var req = getWithPathVars(
                "/authors/{id}/royalties/{period}",
                Map.of("id", authorId.toString(), "period", "2025-01")
        );

        var res = errorFilter.filter(req, handler::getMonthlyRoyalty);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(200);
            s.assertThat(body).contains("\"authorId\":\"" + authorId + "\"");
            s.assertThat(body).contains("\"period\":\"2025-01\"");
            s.assertThat(body).contains("\"totalUnits\":1000");
            s.assertThat(body).contains("\"grossRevenue\":10000.00");
            s.assertThat(body).contains("\"effectiveRate\":0.0890");
            s.assertThat(body).contains("\"royaltyDue\":890.00");
            s.assertThat(body).contains("\"minimumGuarantee\":500.00");
            // one tier sample check
            s.assertThat(body).contains("\"unitsInTier\":500");
            s.assertThat(body).contains("\"appliedRate\":0.07");
            s.assertThat(body).contains("\"royaltyAmount\":350.00");
        });
    }

    @Test
    void getMonthlyRoyalty_invalid_period_returns400_via_filter() throws Exception {
        var authorId = UUID.randomUUID();

        var req = getWithPathVars(
                "/authors/{id}/royalties/{period}",
                Map.of("id", authorId.toString(), "period", "2025-13") // invalid month
        );

        var res = errorFilter.filter(req, handler::getMonthlyRoyalty);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(400);
            s.assertThat(body).contains("\"error\":\"Invalid request:");
            s.check(() -> verifyNoInteractions(royaltyService));
        });
    }

    @Test
    void getMonthlyRoyalty_invalid_uuid_returns400_via_filter() throws Exception {
        var req = getWithPathVars(
                "/authors/{id}/royalties/{period}",
                Map.of("id", "not-a-uuid", "period", "2025-01")
        );

        var res = errorFilter.filter(req, handler::getMonthlyRoyalty);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(400);
            s.assertThat(body).contains("\"error\":\"Invalid request:");
            s.check(() -> verifyNoInteractions(royaltyService));
        });
    }

    @Test
    void getMonthlyRoyalty_not_found_returns404_via_filter() throws Exception {
        var authorId = UUID.randomUUID();

        when(royaltyService.generateMonthlyReport(authorId, YearMonth.of(2025, 1)))
                .thenThrow(new NotFoundException("Author %s not found".formatted(authorId)));

        var req = getWithPathVars(
                "/authors/{id}/royalties/{period}",
                Map.of("id", authorId.toString(), "period", "2025-01")
        );

        var res = errorFilter.filter(req, handler::getMonthlyRoyalty);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(404);
            s.assertThat(body).contains("\"error\":\"Invalid resource identifier:");
        });
    }

    @Test
    void createSale_returns201_with_payload() throws Exception {
        var id = UUID.randomUUID();
        var bookId = UUID.randomUUID();
        when(royaltyService.createSale(bookId, 10, new BigDecimal("100.1")))
                .thenReturn(Sale.rehydrate(id, bookId,10, new BigDecimal("100.1"), Instant.parse("2024-01-01T00:00:00Z")));

        var req = postJson("/sale", """
                {"bookId":"%s","units":10,"amountEur":100.1}
                """.formatted(bookId.toString())
        );
        var res = errorFilter.filter(req, handler::createSale);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(201);
            s.assertThat(body).contains("\"id\":\"%s\"".formatted(id.toString()));
            s.assertThat(body).contains("\"bookId\":\"%s\"".formatted(bookId.toString()));
            s.assertThat(body).contains("\"units\":10");
            s.assertThat(body).contains("\"amountEur\":100.1");
            s.assertThat(body).contains("\"soldAt\":\"2024-01-01T00:00:00Z\"");
        });
    }
}
