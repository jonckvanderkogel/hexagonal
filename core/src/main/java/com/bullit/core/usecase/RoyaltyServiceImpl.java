package com.bullit.core.usecase;

import com.bullit.domain.event.SaleEvent;
import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.royalty.RoyaltyScheme;
import com.bullit.domain.model.royalty.RoyaltyTier;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.model.royalty.TierBreakdown;
import com.bullit.domain.model.sales.SalesSummary;
import com.bullit.domain.model.stream.OutputStreamPort;
import com.bullit.domain.port.driven.SaleRepositoryPort;
import com.bullit.domain.port.driven.reporting.SalesReportingPort;
import com.bullit.domain.port.driving.RoyaltyServicePort;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.math.BigDecimal.ZERO;

public final class RoyaltyServiceImpl implements RoyaltyServicePort {
    private final SalesReportingPort salesReportingPort;
    private final SaleRepositoryPort saleRepositoryPort;
    private final OutputStreamPort<SaleEvent> outputStreamPort;
    private final RoyaltyScheme scheme;
    private final MathContext mc = new MathContext(12, RoundingMode.HALF_UP);
    private final Clock clock;

    public RoyaltyServiceImpl(SalesReportingPort salesReportingPort,
                              SaleRepositoryPort saleRepositoryPort,
                              OutputStreamPort<SaleEvent> outputStream,
                              RoyaltyScheme scheme,
                              Clock clock) {
        this.salesReportingPort = salesReportingPort;
        this.saleRepositoryPort = saleRepositoryPort;
        this.outputStreamPort = outputStream;
        this.scheme = scheme;
        this.clock = clock;
    }

    @Override
    public RoyaltyReport generateMonthlyReport(UUID authorId, YearMonth period) {
        final SalesSummary sales = salesReportingPort.monthlyAuthorSales(authorId, period);

        final long units = sales.getUnits();
        final BigDecimal gross = sales.getGrossRevenue();

        final var breakdown = tierBreakdown(units, gross, scheme.getTiers());

        final BigDecimal tierRoyalty = breakdown
                .stream()
                .map(TierBreakdown::getRoyaltyAmount)
                .reduce(ZERO, BigDecimal::add);

        final BigDecimal royaltyDue = tierRoyalty.max(scheme.getMinimumGuarantee());

        final BigDecimal effectiveRate =
                gross.signum() == 0 ? ZERO : royaltyDue.divide(gross, mc);

        return RoyaltyReport.of(
                authorId,
                period,
                units,
                gross,
                effectiveRate,
                royaltyDue,
                scheme.getMinimumGuarantee(),
                breakdown
        );
    }

    /**
     * Allocate units into tiers and compute royalty based on revenue share per tier.
     */
    private List<TierBreakdown> tierBreakdown(long totalUnits, BigDecimal gross, List<RoyaltyTier> tiers) {
        if (totalUnits == 0 || gross.signum() == 0) {
            return tiers.stream()
                    .map(t -> TierBreakdown.of(0, t.getRate(), ZERO))
                    .toList();
        }

        record Acc(long prevUpper, long remaining, List<TierBreakdown> out) {
        }

        var seed = new Acc(0L, totalUnits, List.of());

        var acc = tiers
                .stream()
                .reduce(
                        seed,
                        (a, t) -> {
                            long cap = Math.max(0L, t.getUpToUnits() - a.prevUpper());
                            long take = Math.min(a.remaining(), cap);

                            var share = new BigDecimal(take, mc)
                                    .divide(new BigDecimal(totalUnits, mc), mc);

                            var royalty = gross
                                    .multiply(share, mc)
                                    .multiply(t.getRate(), mc);

                            var nextList = new ArrayList<TierBreakdown>(a.out().size() + 1);
                            nextList.addAll(a.out());
                            nextList.add(TierBreakdown.of(take, t.getRate(), royalty));

                            return new Acc(
                                    a.prevUpper() + cap,
                                    a.remaining() - take,
                                    nextList
                            );
                        },
                        // Sequential stream; combiner unused but required
                        (a1, a2) -> a1.remaining() <= a2.remaining() ? a1 : a2
                );

        return acc.out();
    }

    @Override
    public Sale createSale(UUID bookId, int units, BigDecimal amountEur) {
        var sale = saleRepositoryPort.addSale(Sale.createNew(bookId, units, amountEur, clock));
        outputStreamPort.emit(Sale.toEvent(sale));
        return sale;
    }
}