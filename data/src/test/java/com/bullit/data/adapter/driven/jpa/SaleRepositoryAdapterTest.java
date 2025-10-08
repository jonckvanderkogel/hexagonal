package com.bullit.data.adapter.driven.jpa;

import com.bullit.data.adapter.driven.adapter.SaleRepositoryAdapter;
import com.bullit.domain.error.PersistenceException;
import com.bullit.domain.model.royalty.Sale;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SaleRepositoryAdapterTest {
    private final SaleJpaRepository jpa = mock(SaleJpaRepository.class);
    private final SaleRepositoryAdapter adapter = new SaleRepositoryAdapter(jpa);

    @Test
    void addSale_mapsEntityAndBack() {
        var saleId = UUID.randomUUID();
        var bookId   = UUID.randomUUID();
        var units = 10;
        var amountEur = new BigDecimal("100.1");
        var sold = Instant.parse("2024-01-05T00:00:00Z");
        var domain   = Sale.rehydrate(saleId, bookId, units, amountEur, sold);

        var savedEntity = new SaleEntity(
                saleId,
                bookId,
                units,
                amountEur,
                sold
        );

        when(jpa.save(any(SaleEntity.class))).thenReturn(savedEntity);

        var saved = adapter.addSale(domain);

        assertThat(saved.getId()).isEqualTo(saleId);
        assertThat(saved.getBookId()).isEqualTo(bookId);
        assertThat(saved.getUnits()).isEqualTo(units);
        assertThat(saved.getAmountEur()).isEqualTo(amountEur);
        assertThat(saved.getSoldAt()).isEqualTo(sold);
        verify(jpa, times(1)).save(any(SaleEntity.class));
    }

    @Test
    void addSale_onDataAccessException_wrapsAsPersistenceException() {
        var saleId = UUID.randomUUID();
        var bookId   = UUID.randomUUID();
        var units = 10;
        var amountEur = new BigDecimal("100.1");
        var sold = Instant.parse("2024-01-05T00:00:00Z");
        var domain   = Sale.rehydrate(saleId, bookId, units, amountEur, sold);

        when(jpa.save(any(SaleEntity.class))).thenThrow(new DataAccessResourceFailureException("db down"));

        assertThatThrownBy(() -> adapter.addSale(domain))
                .isInstanceOf(PersistenceException.class)
                .hasMessageContaining("DB error during save of sale");
    }
}
