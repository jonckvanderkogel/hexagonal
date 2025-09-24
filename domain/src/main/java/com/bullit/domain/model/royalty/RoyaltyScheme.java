package com.bullit.domain.model.royalty;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static com.bullit.domain.model.DomainValidator.assertValid;

public class RoyaltyScheme {
    @NotEmpty(message = "Tiers are required")
    private final List<RoyaltyTier> tiers;

    @NotNull(message = "Minimum guarantee cannot be empty.")
    @Min(value = 0, message = "Oh come on, give the guy something.")
    private final BigDecimal minimumGuarantee;

    public List<RoyaltyTier> getTiers() {
        return tiers;
    }

    public BigDecimal getMinimumGuarantee() {
        return minimumGuarantee;
    }

    private RoyaltyScheme(List<RoyaltyTier> tiers, BigDecimal minimumGuarantee) {
        this.tiers = tiers;
        this.minimumGuarantee = minimumGuarantee;
    }

    public static RoyaltyScheme of(List<RoyaltyTier> tiers, BigDecimal minimumGuarantee) {
        var sorted = tiers.stream().sorted().toList();

        var last = sorted.getLast();

        var normalized = last.getUpToUnits() == Long.MAX_VALUE
                ? sorted
                : updateLastElement(sorted);

        return assertValid(
                new RoyaltyScheme(normalized, minimumGuarantee)
        );
    }

    private static List<RoyaltyTier> append(List<RoyaltyTier> base, RoyaltyTier extra) {
        var list = new ArrayList<RoyaltyTier>(base.size() + 1);
        list.addAll(base);
        list.add(extra);
        return list;
    }

    private static List<RoyaltyTier> updateLastElement(List<RoyaltyTier> base) {
        var list = new ArrayList<>(base);
        var last = base.getLast();
        list.set(list.size() - 1, RoyaltyTier.of(Long.MAX_VALUE, last.getRate()));

        return list;
    }
}
