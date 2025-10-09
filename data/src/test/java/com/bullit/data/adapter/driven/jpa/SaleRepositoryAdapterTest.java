package com.bullit.data.adapter.driven.jpa;

import com.bullit.data.adapter.driven.adapter.SaleRepositoryAdapter;
import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.royalty.Sale;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

final class SaleRepositoryAdapterTest {

    private final EntityManager em = mock(EntityManager.class);
    private final Query nativeQuery = mock(Query.class);
    private final SaleRepositoryAdapter adapter = new SaleRepositoryAdapter(em);

    @Test
    void addSale_happyPath_mapsRowToDomain() {
        var saleId    = UUID.randomUUID();
        var bookId    = UUID.randomUUID();
        var units     = 10;
        var amount    = new BigDecimal("100.10");
        var soldAt    = Instant.parse("2024-01-05T00:00:00Z");
        var input     = Sale.rehydrate(saleId, bookId, units, amount, soldAt);

        Object[] row = new Object[] {
                saleId,
                bookId,
                units,
                amount,
                Timestamp.from(soldAt)
        };

        when(em.createNativeQuery(any(String.class))).thenReturn(nativeQuery);

        when(nativeQuery.setParameter(eq("id"),     eq(saleId))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("bookId"), eq(bookId))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("units"),  eq(units))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("amount"), eq(amount))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("soldAt"), eq(soldAt))).thenReturn(nativeQuery);
        when(nativeQuery.getSingleResult()).thenReturn(row);

        var saved = adapter.addSale(input);

        assertThat(saved.getId()).isEqualTo(saleId);
        assertThat(saved.getBookId()).isEqualTo(bookId);
        assertThat(saved.getUnits()).isEqualTo(units);
        assertThat(saved.getAmountEur()).isEqualTo(amount);
        assertThat(saved.getSoldAt()).isEqualTo(soldAt);

        verify(em, times(1)).createNativeQuery(any(String.class));
        verify(nativeQuery, times(1)).getSingleResult();
    }

    @Test
    void addSale_nonExistingBook_throwsNotFound() {
        var saleId = UUID.randomUUID();
        var bookId = UUID.randomUUID();
        var input  = Sale.rehydrate(saleId, bookId, 5, new BigDecimal("42.00"),
                Instant.parse("2024-02-01T00:00:00Z"));

        when(em.createNativeQuery(any(String.class))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(any(String.class), any())).thenReturn(nativeQuery);
        when(nativeQuery.getSingleResult()).thenThrow(new NoResultException("no row"));

        assertThatThrownBy(() -> adapter.addSale(input))
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining(bookId.toString());
    }

    @Test
    void addSale_persistenceIssue_wrapsAsPersistenceException() {
        var saleId = UUID.randomUUID();
        var bookId = UUID.randomUUID();
        var input  = Sale.rehydrate(saleId, bookId, 3, new BigDecimal("15.00"),
                Instant.parse("2024-03-01T00:00:00Z"));

        when(em.createNativeQuery(any(String.class))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(any(String.class), any())).thenReturn(nativeQuery);
        when(nativeQuery.getSingleResult()).thenThrow(new jakarta.persistence.PersistenceException("db down"));

        assertThatThrownBy(() -> adapter.addSale(input))
                .isInstanceOf(DatabaseInteractionException.class)
                .hasMessageContaining("DB error during save of sale");
    }
}