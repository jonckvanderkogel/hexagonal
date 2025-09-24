package com.bullit.data.adapter.driven.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "sales")
public class SaleEntity {
    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "book_id", nullable = false, updatable = false)
    private BookEntity book;

    @Column(name = "units", nullable = false)
    private int units;

    @Column(name = "amount_eur", nullable = false, precision = 12, scale = 2)
    private BigDecimal amountEur;

    @Column(name = "sold_at", nullable = false)
    private Instant soldAt;

    protected SaleEntity() {}

    public SaleEntity(UUID id, BookEntity book, int units, BigDecimal amountEur, Instant soldAt) {
        this.id = id;
        this.book = book;
        this.units = units;
        this.amountEur = amountEur;
        this.soldAt = soldAt;
    }

    public UUID getId() {
        return id;
    }

    public BookEntity getBook() {
        return book;
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