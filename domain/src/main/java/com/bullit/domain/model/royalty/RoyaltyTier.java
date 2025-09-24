package com.bullit.domain.model.royalty;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

import static com.bullit.domain.model.DomainValidator.assertValid;

public class RoyaltyTier implements Comparable<RoyaltyTier> {
    @Min(value = 1, message = "Tier upper bound must be >= 1")
    private final long upToUnits;

    @NotNull(message = "Tier rate is required")
    @Positive(message = "The rate must be something")
    private final BigDecimal rate;

    public long getUpToUnits() {
        return upToUnits;
    }

    public BigDecimal getRate() {
        return rate;
    }

    private RoyaltyTier(long upToUnits, BigDecimal rate) {
        this.upToUnits = upToUnits;
        this.rate = rate;
    }

    public static RoyaltyTier of(long upToUnits, BigDecimal rate) {
        return assertValid(
                new RoyaltyTier(upToUnits, rate)
        );
    }

    @Override
    public int compareTo(RoyaltyTier o) {
        return Long.compare(this.upToUnits, o.upToUnits);
    }
}
