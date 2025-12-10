package com.bullit.domain.model.royalty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import static com.bullit.domain.model.DomainValidator.assertValid;

public class Sale {
    @NotNull(message = "Sale id is required")
    private final UUID id;

    @NotNull(message = "Book id is required")
    private final UUID bookId;

    @Positive(message = "You can only sell a positive number of books")
    private final int units;

    @NotNull(message = "Sale amount is required")
    @Positive(message = "Sale amount must be higher than zero")
    private final BigDecimal amountEur;

    @NotNull
    @PastOrPresent(message = "SoldAt is required, and must be in the past or present")
    private final Instant soldAt;

    private Sale(UUID id,
                 UUID bookId,
                 int units,
                 BigDecimal amountEur,
                 Instant soldAt
    ) {
        this.id = id;
        this.bookId = bookId;
        this.units = units;
        this.amountEur = amountEur;
        this.soldAt = soldAt;
    }

    public static Sale createNew(UUID bookId,
                                 int units,
                                 BigDecimal amountEur,
                                 Clock clock
    ) {
        return assertValid(new Sale(
                UUID.randomUUID(),
                bookId,
                units,
                amountEur,
                clock.instant()
        ));
    }

    @JsonCreator
    public static Sale rehydrate(
            @JsonProperty("id") UUID id,
            @JsonProperty("bookId") UUID bookId,
            @JsonProperty("units") int units,
            @JsonProperty("amountEur") BigDecimal amountEur,
            @JsonProperty("soldAt") Instant soldAt
    ) {
        return assertValid(new Sale(
                id,
                bookId,
                units,
                amountEur,
                soldAt
        ));
    }

    public UUID getId() {
        return id;
    }

    public UUID getBookId() {
        return bookId;
    }

    public int getUnits() {
        return units;
    }

    public BigDecimal getAmountEur() {
        return amountEur;
    }

    public Instant getSoldAt() {
        return soldAt;
    }
}
