package com.bullit.domain.model.royalty;

import com.bullit.domain.event.TierBreakdownEvent;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

import static com.bullit.domain.model.DomainValidator.assertValid;
import static com.bullit.domain.model.royalty.AvroUtil.scale;

public class TierBreakdown {
    @Min(value = 0, message = "Cannot have negative units in tier.")
    private final long unitsInTier;

    @NotNull(message = "Applied rate cannot be empty.")
    @Min(value = 0, message = "Applied rate cannot be negative.")
    private final BigDecimal appliedRate;

    @NotNull(message = "Royalty amount cannot be empty.")
    @Min(value = 0, message = "Royalty amount cannot be negative.")
    private final BigDecimal royaltyAmount;

    public long getUnitsInTier() {
        return unitsInTier;
    }

    public BigDecimal getAppliedRate() {
        return appliedRate;
    }

    public BigDecimal getRoyaltyAmount() {
        return royaltyAmount;
    }

    private TierBreakdown(long unitsInTier, BigDecimal appliedRate, BigDecimal royaltyAmount) {
        this.unitsInTier = unitsInTier;
        this.appliedRate = appliedRate;
        this.royaltyAmount = royaltyAmount;
    }

    public static TierBreakdown of(long unitsInTier, BigDecimal appliedRate, BigDecimal royaltyAmount) {
        return assertValid(
                new TierBreakdown(unitsInTier, appliedRate, royaltyAmount)
        );
    }

    public static TierBreakdownEvent toEvent(TierBreakdown tierBreakdown) {
        return TierBreakdownEvent
                .newBuilder()
                .setUnitsInTier(tierBreakdown.getUnitsInTier())
                .setAppliedRate(scale(tierBreakdown.getAppliedRate()))
                .setRoyaltyAmount(scale(tierBreakdown.getRoyaltyAmount()))
                .build();
    }
}
