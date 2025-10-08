package com.bullit.data.adapter.driven.adapter;

import com.bullit.data.adapter.driven.jpa.SaleJpaRepository;
import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.PersistenceException;
import com.bullit.domain.model.sales.SalesSummary;
import com.bullit.domain.port.outbound.reporting.SalesReportingPort;
import org.springframework.dao.DataAccessException;

import java.math.BigDecimal;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.UUID;

public final class SalesReportingAdapter implements SalesReportingPort {
    private final SaleJpaRepository repo;

    public SalesReportingAdapter(SaleJpaRepository repo) { this.repo = repo; }

    @Override
    public SalesSummary monthlyAuthorSales(UUID authorId, YearMonth period) {
        var start = period.atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC);
        var end   = period.plusMonths(1).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC);

        try {
            var agg = repo.sumForAuthorBetween(authorId, start, end);

            if (!agg.getAuthorExists()) {
                throw new NotFoundException("Author %s not found".formatted(authorId));
            }

            long units   = agg.getUnits() == null ? 0L : agg.getUnits();
            var gross    = agg.getGross() == null ? BigDecimal.ZERO : agg.getGross();

            return SalesSummary.of(units, gross);
        } catch (DataAccessException ex) {
            throw new PersistenceException(
                    "DB error during monthly sales summary", ex);
        }
    }
}