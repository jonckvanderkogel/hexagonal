package com.bullit.data.adapter.driven.jpa;

import com.bullit.domain.model.royalty.Sale;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "sales")
public class SaleEntity {
    @Id
    private UUID id;

    @Column(name = "book_id")
    private UUID bookId;

    @Column(name = "units", nullable = false)
    private int units;

    @Column(name = "amount_eur", nullable = false, precision = 12, scale = 2)
    private BigDecimal amountEur;

    @Column(name = "sold_at", nullable = false)
    private Instant soldAt;

    protected SaleEntity() {}

    public SaleEntity(UUID id, UUID bookId, int units, BigDecimal amountEur, Instant soldAt) {
        this.id = id;
        this.bookId = bookId;
        this.units = units;
        this.amountEur = amountEur;
        this.soldAt = soldAt;
    }

    public static SaleEntity toEntity(Sale sale) {
        return new SaleEntity(
                sale.getId(),
                sale.getBookId(),
                sale.getUnits(),
                sale.getAmountEur(),
                sale.getSoldAt()
        );
    }

    public static Sale toDomain(SaleEntity entity) {
        return Sale.rehydrate(
                entity.id,
                entity.bookId,
                entity.units,
                entity.amountEur,
                entity.soldAt
        );
    }
}