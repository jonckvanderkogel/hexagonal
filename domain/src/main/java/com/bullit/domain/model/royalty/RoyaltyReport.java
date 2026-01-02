package com.bullit.domain.model.royalty;

import com.bullit.domain.event.RoyaltyReportEvent;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;

import java.math.BigDecimal;
import java.time.YearMonth;
import java.util.List;
import java.util.UUID;

import static com.bullit.domain.model.DomainValidator.assertValid;
import static com.bullit.domain.model.royalty.AvroUtil.scale;

public class RoyaltyReport {
    @NotNull(message = "Author ID is required.")
    private final UUID authorId;

    @NotNull(message = "Period is required")
    @PastOrPresent(message = "No idea what you are going to sell in the future.")
    private final YearMonth period;

    @Min(value = 0, message = "Units sold cannot be negative.")
    private final long units;

    @NotNull(message = "Gross revenue must not be empty.")
    @Min(value = 0, message = "You cannot have negative gross revenue.")
    private final BigDecimal grossRevenue;

    @NotNull(message = "Effective rate must not be empty.")
    @Min(value = 0, message = "Effective rate cannot be negative.")
    private final BigDecimal effectiveRate;

    @NotNull(message = "Royalty due cannot be empty.")
    @Min(value = 0, message = "You cannot get money from an author by selling books.")
    private final BigDecimal royaltyDue;

    @NotNull(message = "Minimum guarantee cannot be empty.")
    @Min(value = 0, message = "Oh come on, give the guy something.")
    private final BigDecimal minimumGuarantee;

    @NotEmpty(message = "You must have tiers defined.")
    private final List<TierBreakdown> breakdown;

    public UUID getAuthorId() {
        return authorId;
    }

    public YearMonth getPeriod() {
        return period;
    }

    public long getUnits() {
        return units;
    }

    public BigDecimal getGrossRevenue() {
        return grossRevenue;
    }

    public BigDecimal getEffectiveRate() {
        return effectiveRate;
    }

    public BigDecimal getRoyaltyDue() {
        return royaltyDue;
    }

    public BigDecimal getMinimumGuarantee() {
        return minimumGuarantee;
    }

    public List<TierBreakdown> getBreakdown() {
        return breakdown;
    }

    private RoyaltyReport(
            UUID authorId,
            YearMonth period,
            long units,
            BigDecimal grossRevenue,
            BigDecimal effectiveRate,
            BigDecimal royaltyDue,
            BigDecimal minimumGuarantee,
            List<TierBreakdown> breakdown
    ) {
        this.authorId = authorId;
        this.period = period;
        this.units = units;
        this.grossRevenue = grossRevenue;
        this.effectiveRate = effectiveRate;
        this.royaltyDue = royaltyDue;
        this.minimumGuarantee = minimumGuarantee;
        this.breakdown = breakdown;
    }

    public static RoyaltyReport of(
            UUID authorId,
            YearMonth period,
            long units,
            BigDecimal grossRevenue,
            BigDecimal effectiveRate,
            BigDecimal royaltyDue,
            BigDecimal minimumGuarantee,
            List<TierBreakdown> breakdown
    ) {
        return assertValid(
                new RoyaltyReport(
                        authorId,
                        period,
                        units,
                        grossRevenue,
                        effectiveRate,
                        royaltyDue,
                        minimumGuarantee,
                        breakdown
                )
        );
    }

    public static RoyaltyReportEvent toEvent(RoyaltyReport report) {
        return RoyaltyReportEvent.newBuilder()
                .setAuthorId(report.getAuthorId().toString())
                .setPeriod(report.getPeriod().toString())
                .setUnits(report.getUnits())
                .setGrossRevenue(scale(report.getGrossRevenue()))
                .setEffectiveRate(scale(report.getEffectiveRate()))
                .setRoyaltyDue(scale(report.getRoyaltyDue()))
                .setMinimumGuarantee(scale(report.getMinimumGuarantee()))
                .setBreakdown(
                        report
                                .getBreakdown()
                                .stream()
                                .map(TierBreakdown::toEvent)
                                .toList()
                )
                .build();
    }
}
