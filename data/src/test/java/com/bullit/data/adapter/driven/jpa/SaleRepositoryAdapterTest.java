package com.bullit.data.adapter.driven.jpa;

import com.bullit.data.adapter.driven.adapter.SaleRepositoryAdapter;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.model.royalty.Sale;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import org.hibernate.query.NativeQuery;
import org.hibernate.type.StandardBasicTypes;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class SaleRepositoryAdapterTest {

    private final EntityManager em = mock(EntityManager.class);
    private final SaleRepositoryAdapter adapter = new SaleRepositoryAdapter(em);

    @Test
    void addSale_happyPath_mapsRowToDomain() {
        var saleId = UUID.randomUUID();
        var bookId = UUID.randomUUID();
        var units = 10;
        var amount = new BigDecimal("100.10");
        var soldAt = Instant.parse("2024-01-05T00:00:00Z");
        var input = Sale.rehydrate(saleId, bookId, units, amount, soldAt);

        Query jpaQuery = mock(Query.class);
        @SuppressWarnings("unchecked")
        NativeQuery<Object[]> hibernateQuery = mock(NativeQuery.class);

        when(em.createNativeQuery(any(String.class))).thenReturn(jpaQuery);
        when(jpaQuery.unwrap(NativeQuery.class)).thenReturn(hibernateQuery);

        when(hibernateQuery.addScalar(eq("id"), eq(StandardBasicTypes.UUID))).thenReturn(hibernateQuery);
        when(hibernateQuery.addScalar(eq("book_id"), eq(StandardBasicTypes.UUID))).thenReturn(hibernateQuery);
        when(hibernateQuery.addScalar(eq("units"), eq(StandardBasicTypes.INTEGER))).thenReturn(hibernateQuery);
        when(hibernateQuery.addScalar(eq("amount_eur"), eq(StandardBasicTypes.BIG_DECIMAL))).thenReturn(hibernateQuery);
        when(hibernateQuery.addScalar(eq("sold_at"), eq(StandardBasicTypes.INSTANT))).thenReturn(hibernateQuery);

        when(hibernateQuery.setParameter(eq("id"), eq(saleId))).thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(eq("bookId"), eq(bookId))).thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(eq("units"), eq(units))).thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(eq("amount"), eq(amount))).thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(eq("soldAt"), eq(soldAt))).thenReturn(hibernateQuery);

        Object[] row = new Object[]{
                saleId,
                bookId,
                units,
                amount,
                soldAt
        };
        when(hibernateQuery.getSingleResult()).thenReturn(row);

        var saved = adapter.addSale(input);

        assertSoftly(s -> {
            s.assertThat(saved.getId()).isEqualTo(saleId);
            s.assertThat(saved.getBookId()).isEqualTo(bookId);
            s.assertThat(saved.getUnits()).isEqualTo(units);
            s.assertThat(saved.getAmountEur()).isEqualTo(amount);
            s.assertThat(saved.getSoldAt()).isEqualTo(soldAt);

            s.check(() -> verify(em).createNativeQuery(any(String.class)));
            s.check(() -> verify(jpaQuery).unwrap(NativeQuery.class));
            s.check(() -> verify(hibernateQuery).getSingleResult());
        });
    }

    @Test
    void addSale_nonExistingBook_throwsNotFound() {
        var saleId = UUID.randomUUID();
        var bookId = UUID.randomUUID();
        var input = Sale.rehydrate(
                saleId,
                bookId,
                5,
                new BigDecimal("42.00"),
                Instant.parse("2024-02-01T00:00:00Z")
        );

        Query jpaQuery = mock(Query.class);
        @SuppressWarnings("unchecked")
        NativeQuery<Object[]> hibernateQuery = mock(NativeQuery.class);

        when(em.createNativeQuery(any(String.class))).thenReturn(jpaQuery);
        when(jpaQuery.unwrap(NativeQuery.class)).thenReturn(hibernateQuery);

        when(hibernateQuery.addScalar(anyString(), any(org.hibernate.type.BasicTypeReference.class)))
                .thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(anyString(), any()))
                .thenReturn(hibernateQuery);
        when(hibernateQuery.getSingleResult())
                .thenThrow(new NoResultException("no row"));

        assertThatThrownBy(() -> adapter.addSale(input))
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining(bookId.toString());
    }

    @Test
    void addSale_persistenceIssue_wrapsAsDatabaseInteractionException() {
        var saleId = UUID.randomUUID();
        var bookId = UUID.randomUUID();
        var input = Sale.rehydrate(
                saleId,
                bookId,
                3,
                new BigDecimal("15.00"),
                Instant.parse("2024-03-01T00:00:00Z")
        );

        Query jpaQuery = mock(Query.class);
        @SuppressWarnings("unchecked")
        NativeQuery<Object[]> hibernateQuery = mock(NativeQuery.class);

        when(em.createNativeQuery(any(String.class))).thenReturn(jpaQuery);
        when(jpaQuery.unwrap(NativeQuery.class)).thenReturn(hibernateQuery);

        when(hibernateQuery.addScalar(anyString(), any(org.hibernate.type.BasicTypeReference.class)))
                .thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(anyString(), any()))
                .thenReturn(hibernateQuery);
        when(hibernateQuery.getSingleResult())
                .thenThrow(new PersistenceException("db down"));

        assertThatThrownBy(() -> adapter.addSale(input))
                .isInstanceOf(DatabaseInteractionException.class)
                .hasMessageContaining("DB error during save of sale");
    }
}