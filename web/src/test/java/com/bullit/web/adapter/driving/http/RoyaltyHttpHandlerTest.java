package com.bullit.web.adapter.driving.http;

import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.royalty.TierBreakdown;
import com.bullit.domain.port.inbound.RoyaltyServicePort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.function.HandlerFilterFunction;
import org.springframework.web.servlet.function.ServerResponse;

import java.math.BigDecimal;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

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
        assertThat(status(res)).isEqualTo(200);
        assertThat(body).contains("\"authorId\":\"" + authorId + "\"");
        assertThat(body).contains("\"period\":\"2025-01\"");
        assertThat(body).contains("\"totalUnits\":1000");
        assertThat(body).contains("\"grossRevenue\":10000.00");
        assertThat(body).contains("\"effectiveRate\":0.0890");
        assertThat(body).contains("\"royaltyDue\":890.00");
        assertThat(body).contains("\"minimumGuarantee\":500.00");
        // one tier sample check
        assertThat(body).contains("\"unitsInTier\":500");
        assertThat(body).contains("\"appliedRate\":0.07");
        assertThat(body).contains("\"royaltyAmount\":350.00");
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
        assertThat(status(res)).isEqualTo(400);
        assertThat(body).contains("\"error\":\"Invalid request:");
        verifyNoInteractions(royaltyService);
    }

    @Test
    void getMonthlyRoyalty_invalid_uuid_returns400_via_filter() throws Exception {
        var req = getWithPathVars(
                "/authors/{id}/royalties/{period}",
                Map.of("id", "not-a-uuid", "period", "2025-01")
        );

        var res = errorFilter.filter(req, handler::getMonthlyRoyalty);

        var body = writeToString(res);
        assertThat(status(res)).isEqualTo(400);
        assertThat(body).contains("\"error\":\"Invalid request:");
        verifyNoInteractions(royaltyService);
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
        assertThat(status(res)).isEqualTo(404);
        assertThat(body).contains("\"error\":\"Invalid resource identifier:");
    }
}
