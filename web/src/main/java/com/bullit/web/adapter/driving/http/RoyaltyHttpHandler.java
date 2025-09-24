package com.bullit.web.adapter.driving.http;

import com.bullit.domain.port.inbound.RoyaltyServicePort;
import com.bullit.web.adapter.driving.http.Response.RoyaltyReportResponse;
import com.bullit.web.adapter.driving.http.util.HttpUtil;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.time.YearMonth;
import java.util.UUID;

import static org.springframework.http.HttpStatus.OK;

public final class RoyaltyHttpHandler {

    private final RoyaltyServicePort royaltyService;

    public RoyaltyHttpHandler(RoyaltyServicePort royaltyService) {
        this.royaltyService = royaltyService;
    }

    /**
     * GET /authors/{id}/royalties/{period}
     * period format: yyyy-MM (e.g. 2025-01)
     */
    public ServerResponse getMonthlyRoyalty(ServerRequest request) {
        UUID authorId = HttpUtil.parseUuid(request.pathVariable("id"));
        YearMonth period = HttpUtil.parseYearMonth(request.pathVariable("period"));

        var report = royaltyService.generateMonthlyReport(authorId, period);
        return ServerResponse.status(OK).body(RoyaltyReportResponse.fromDomain(report));
    }
}