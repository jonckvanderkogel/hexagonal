package com.bullit.core.usecase;

import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.royalty.RoyaltyScheme;
import com.bullit.domain.model.royalty.RoyaltyTier;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.model.royalty.TierBreakdown;
import com.bullit.domain.model.sales.SalesSummary;
import com.bullit.domain.port.driven.SaleRepositoryPort;
import com.bullit.domain.port.driven.reporting.SalesReportingPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static java.math.BigDecimal.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class RoyaltyServiceImplTest {

    private SalesReportingPort reporting;
    private SaleRepositoryPort saleRepositoryPort;
    private final Clock fixed = Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);

    private final UUID author = UUID.randomUUID();
    private final YearMonth period = YearMonth.of(2025, 1);

    private static RoyaltyScheme progressiveScheme() {
        return RoyaltyScheme.of(
                List.of(
                        RoyaltyTier.of(100, new BigDecimal("0.10")),
                        RoyaltyTier.of(300, new BigDecimal("0.15")),
                        RoyaltyTier.of(Long.MAX_VALUE, new BigDecimal("0.20"))
                ),
                ZERO
        );
    }

    @BeforeEach
    void setUp() {
        reporting = mock(SalesReportingPort.class);
        saleRepositoryPort = mock(SaleRepositoryPort.class);
    }

    private static SalesSummary sales(long units, String grossEuros) {
        return SalesSummary.of(units, new BigDecimal(grossEuros));
    }

    @Test
    void monthlyReport_progressive_inTier2_only() {
        // 250 units, €1000 gross
        // Allocation: T1=100, T2=150, T3=0
        // Royalty = 1000*(100/250*0.10 + 150/250*0.15) = 1000*(0.04+0.09)=130
        when(reporting.monthlyAuthorSales(author, period)).thenReturn(sales(250, "1000"));

        var service = new RoyaltyServiceImpl(reporting, saleRepositoryPort, progressiveScheme(), fixed);

        RoyaltyReport rep = service.generateMonthlyReport(author, period);

        assertThat(rep.getAuthorId()).isEqualTo(author);
        assertThat(rep.getPeriod()).isEqualTo(period);
        assertThat(rep.getUnits()).isEqualTo(250);
        assertThat(rep.getGrossRevenue()).isEqualByComparingTo("1000");
        assertThat(rep.getRoyaltyDue()).isEqualByComparingTo("130");
        assertThat(rep.getEffectiveRate()).isEqualByComparingTo("0.13");
        assertThat(rep.getMinimumGuarantee()).isEqualByComparingTo("0");

        List<TierBreakdown> tiers = rep.getBreakdown();
        assertThat(tiers).hasSize(3);

        assertTier(tiers.get(0), 100, "0.10", "40");   // 1000 * (100/250) * 0.10 = 40
        assertTier(tiers.get(1), 150, "0.15", "90");   // 1000 * (150/250) * 0.15 = 90
        assertTier(tiers.get(2),   0, "0.20", "0");
    }

    @Test
    void monthlyReport_progressive_intoTier3() {
        // 500 units, €2000 gross
        // Allocation: T1=100, T2=200, T3=200
        // Royalty = 2000*(0.2*0.10 + 0.4*0.15 + 0.4*0.20)
        //         = 2000*(0.02 + 0.06 + 0.08) = 2000*0.16 = 320
        when(reporting.monthlyAuthorSales(author, period)).thenReturn(sales(500, "2000"));

        var service = new RoyaltyServiceImpl(reporting, saleRepositoryPort, progressiveScheme(), fixed);

        RoyaltyReport rep = service.generateMonthlyReport(author, period);

        assertThat(rep.getRoyaltyDue()).isEqualByComparingTo("320");
        assertThat(rep.getEffectiveRate()).isEqualByComparingTo("0.16");

        List<TierBreakdown> tiers = rep.getBreakdown();
        assertThat(tiers).hasSize(3);

        assertTier(tiers.get(0), 100, "0.10", "40");    // 2000*(100/500)*0.10 = 40
        assertTier(tiers.get(1), 200, "0.15", "120");   // 2000*(200/500)*0.15 = 120
        assertTier(tiers.get(2), 200, "0.20", "160");   // 2000*(200/500)*0.20 = 160
    }

    @Test
    void monthlyReport_minimumGuarantee_applies() {
        // Same as first test (royalty=130), but MG=200 → due=200, effectiveRate=0.20
        when(reporting.monthlyAuthorSales(author, period)).thenReturn(sales(250, "1000"));

        RoyaltyScheme scheme = RoyaltyScheme.of(
                progressiveScheme().getTiers(),
                new BigDecimal("200")
        );
        var service = new RoyaltyServiceImpl(reporting, saleRepositoryPort, scheme, fixed);

        RoyaltyReport rep = service.generateMonthlyReport(author, period);

        assertThat(rep.getRoyaltyDue()).isEqualByComparingTo("200");
        assertThat(rep.getEffectiveRate()).isEqualByComparingTo("0.20");
        assertThat(rep.getMinimumGuarantee()).isEqualByComparingTo("200");
    }

    @Test
    void monthlyReport_flatRate_singleTier() {
        // One open-ended tier @12%, 300 units, €1500 gross
        // Royalty = 1500 * 1.0 * 0.12 = 180
        when(reporting.monthlyAuthorSales(author, period)).thenReturn(sales(300, "1500"));

        RoyaltyScheme scheme = RoyaltyScheme.of(
                List.of(RoyaltyTier.of(Long.MAX_VALUE, new BigDecimal("0.12"))),
                ZERO
        );
        var service = new RoyaltyServiceImpl(reporting, saleRepositoryPort, scheme, fixed);

        RoyaltyReport rep = service.generateMonthlyReport(author, period);

        assertThat(rep.getRoyaltyDue()).isEqualByComparingTo("180");
        assertThat(rep.getEffectiveRate()).isEqualByComparingTo("0.12");
        assertThat(rep.getBreakdown()).hasSize(1);
        assertTier(rep.getBreakdown().getFirst(), 300, "0.12", "180");
    }

    @Test
    void monthlyReport_zeroUnits_zeroGross_returnsZeros_andHonorsMG() {
        when(reporting.monthlyAuthorSales(author, period)).thenReturn(sales(0, "0"));

        RoyaltyScheme scheme = RoyaltyScheme.of(progressiveScheme().getTiers(), new BigDecimal("50"));
        var service = new RoyaltyServiceImpl(reporting, saleRepositoryPort, scheme, fixed);

        RoyaltyReport rep = service.generateMonthlyReport(author, period);

        assertThat(rep.getUnits()).isEqualTo(0);
        assertThat(rep.getGrossRevenue()).isEqualByComparingTo("0");
        assertThat(rep.getRoyaltyDue()).isEqualByComparingTo("50"); // MG kicks in
        assertThat(rep.getEffectiveRate()).isEqualByComparingTo("0"); // gross=0 → rate 0
        assertThat(rep.getBreakdown())
                .allSatisfy(b -> {
                    assertThat(b.getUnitsInTier()).isEqualTo(0);
                    assertThat(b.getRoyaltyAmount()).isEqualByComparingTo("0");
                });
    }

    @Test
    void monthlyReport_rounding_path_isStable() {
        // 3 tiers: 50 @7%, 100 @11%, ∞ @17%
        // 123 units, €987.65
        // Allocation: 50 / 73 / 0
        when(reporting.monthlyAuthorSales(author, period))
                .thenReturn(sales(123, "987.65"));

        RoyaltyScheme scheme = RoyaltyScheme.of(
                List.of(
                        RoyaltyTier.of(50,  new BigDecimal("0.07")),
                        RoyaltyTier.of(150, new BigDecimal("0.11")),
                        RoyaltyTier.of(Long.MAX_VALUE, new BigDecimal("0.17"))
                ),
                ZERO
        );
        var service = new RoyaltyServiceImpl(reporting, saleRepositoryPort, scheme, fixed);

        RoyaltyReport rep = service.generateMonthlyReport(author, period);

        assertThat(rep.getRoyaltyDue())
                .isCloseTo(new BigDecimal("92.58"), within(new BigDecimal("0.01")));
        assertThat(rep.getEffectiveRate())
                .isCloseTo(new BigDecimal("0.0937"), within(new BigDecimal("0.01")));

        List<TierBreakdown> tiers = rep.getBreakdown();
        assertThat(tiers).hasSize(3);
        assertThat(tiers.get(0).getUnitsInTier()).isEqualTo(50);
        assertThat(tiers.get(1).getUnitsInTier()).isEqualTo(73);
        assertThat(tiers.get(2).getUnitsInTier()).isEqualTo(0);
    }

    private static void assertTier(TierBreakdown b, long units, String rate, String royalty) {
        assertThat(b.getUnitsInTier()).isEqualTo(units);
        assertThat(b.getAppliedRate()).isEqualByComparingTo(rate);
        assertThat(b.getRoyaltyAmount()).isEqualByComparingTo(royalty);
    }

    @Test
    void addSale_persists_viaSaleRepository() {
        var saleId = UUID.randomUUID();
        var bookId   = UUID.randomUUID();
        var units = 10;
        var amountEur = new BigDecimal("100.1");
        var sold = Instant.parse("2024-01-05T00:00:00Z");
        var sale   = Sale.rehydrate(saleId, bookId, units, amountEur, sold);
        when(saleRepositoryPort.addSale(any(Sale.class))).thenReturn(sale);

        var service = new RoyaltyServiceImpl(reporting, saleRepositoryPort, progressiveScheme(), fixed);

        var saved = service.createSale(bookId, units, amountEur);

        assertThat(saved.getBookId()).isEqualTo(bookId);
        assertThat(saved.getUnits()).isEqualTo(units);
        assertThat(saved.getAmountEur()).isEqualTo(amountEur);
        assertThat(saved.getSoldAt()).isEqualTo(sold);
        verify(saleRepositoryPort, times(1)).addSale(any(Sale.class));
    }
}