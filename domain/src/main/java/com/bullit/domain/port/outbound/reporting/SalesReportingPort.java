package com.bullit.domain.port.outbound.reporting;

import com.bullit.domain.model.sales.SalesSummary;

import java.time.YearMonth;
import java.util.UUID;

public interface SalesReportingPort {
    SalesSummary monthlyAuthorSales(UUID authorId, YearMonth period);
}