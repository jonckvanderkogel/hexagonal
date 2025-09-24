package com.bullit.domain.model.sales;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

import static com.bullit.domain.model.DomainValidator.assertValid;

public class SalesSummary {
    @Min(value = 0, message = "Units must be >= 0,")
    private final long units;

    @Min(value = 0, message = "You cannot have negative gross revenue.")
    @NotNull(message = "Gross revenue is required.")
    private final BigDecimal grossRevenue;

    public long getUnits() {
        return units;
    }

    public BigDecimal getGrossRevenue() {
        return grossRevenue;
    }

    private SalesSummary(long units, BigDecimal grossRevenue) {
        this.units = units;
        this.grossRevenue = grossRevenue;
    }

    public static SalesSummary of(long units, BigDecimal grossRevenue) {
        return assertValid(
                new SalesSummary(units, grossRevenue)
        );
    }
}
