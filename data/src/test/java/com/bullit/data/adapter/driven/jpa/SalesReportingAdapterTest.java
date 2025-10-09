package com.bullit.data.adapter.driven.jpa;

import com.bullit.data.adapter.driven.adapter.SalesReportingAdapter;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.sales.SalesSummary;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.UUID;

import static java.math.BigDecimal.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class SalesReportingAdapterTest {

    private final SaleJpaRepository jpa = mock(SaleJpaRepository.class);
    private final SalesReportingAdapter adapter = new SalesReportingAdapter(jpa);

    @Test
    void monthlyAuthorSales_present_mapsProjectionToDomain() {
        var authorId = UUID.randomUUID();
        var ym = YearMonth.of(2025, 3);

        var agg = new SaleJpaRepository.MonthlyAggregation() {
            @Override
            public boolean getAuthorExists() { return true; }
            @Override public Long getUnits() { return 187L; }
            @Override public BigDecimal getGross() { return new BigDecimal("921.35"); }
        };

        var from = ym.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
        var to   = ym.plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();

        when(jpa.sumForAuthorBetween(authorId, from, to)).thenReturn(agg);

        // When
        SalesSummary s = adapter.monthlyAuthorSales(authorId, ym);

        // Then
        assertThat(s.getUnits()).isEqualTo(187L);
        assertThat(s.getGrossRevenue()).isEqualByComparingTo(new BigDecimal("921.35"));
        verify(jpa, times(1)).sumForAuthorBetween(authorId, from, to);
    }

    @Test
    void monthlyAuthorSales_empty_returnsZeroSummary() {
        var authorId = UUID.randomUUID();
        var ym = YearMonth.of(2025, 2);

        var from = ym.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
        var to   = ym.plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();

        var agg = new SaleJpaRepository.MonthlyAggregation() {
            @Override
            public boolean getAuthorExists() { return true; }
            @Override public Long getUnits() { return 0L; }
            @Override public BigDecimal getGross() { return ZERO; }
        };

        when(jpa.sumForAuthorBetween(authorId, from, to)).thenReturn(agg);

        SalesSummary s = adapter.monthlyAuthorSales(authorId, ym);

        assertThat(s.getUnits()).isEqualTo(0L);
        assertThat(s.getGrossRevenue()).isEqualByComparingTo(ZERO);
        verify(jpa, times(1)).sumForAuthorBetween(authorId, from, to);
    }

    @Test
    void monthlyAuthorSales_dataAccessException_wrapsAsPersistenceException() {
        var authorId = UUID.randomUUID();
        var ym = YearMonth.of(2025, 1);

        var from = instantStartOf(ym);
        var to   = instantStartOf(ym.plusMonths(1));

        when(jpa.sumForAuthorBetween(authorId, from, to))
                .thenThrow(new DataAccessResourceFailureException("db down"));

        assertThatThrownBy(() -> adapter.monthlyAuthorSales(authorId, ym))
                .isInstanceOf(DatabaseInteractionException.class)
                .hasMessageContaining("DB error during monthly sales summary");
    }

    private static Instant instantStartOf(YearMonth ym) {
        return ym.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
    }
}
