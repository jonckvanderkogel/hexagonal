package com.bullit.web.adapter.driving.http;

import com.bullit.domain.port.driving.RoyaltyServicePort;
import com.bullit.web.adapter.driving.http.Response.RoyaltyReportResponse;
import com.bullit.web.adapter.driving.http.util.HttpUtil;
import jakarta.servlet.ServletException;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.io.IOException;
import java.time.YearMonth;
import java.util.UUID;

import static com.bullit.web.adapter.driving.http.util.HttpUtil.parseAndValidateBody;
import static org.springframework.http.HttpStatus.OK;

public final class RoyaltyHttpHandler {

    private final RoyaltyServicePort royaltyService;

    public RoyaltyHttpHandler(RoyaltyServicePort royaltyService) {
        this.royaltyService = royaltyService;
    }

    public ServerResponse createSale(ServerRequest request) throws ServletException, IOException {
        var dto = parseAndValidateBody(request, Request.CreateSaleRequest.class);
        var created = royaltyService.createSale(dto.bookId(), dto.units(), dto.amountEur());
        return ServerResponse
                .status(HttpStatus.CREATED)
                .body(Response.SaleResponse.fromDomain(created));
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