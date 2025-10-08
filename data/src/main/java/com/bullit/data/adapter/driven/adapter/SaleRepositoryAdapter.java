package com.bullit.data.adapter.driven.adapter;

import com.bullit.data.adapter.driven.jpa.SaleEntity;
import com.bullit.data.adapter.driven.jpa.SaleJpaRepository;
import com.bullit.domain.error.PersistenceException;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.port.outbound.SaleRepositoryPort;
import org.springframework.dao.DataAccessException;

public class SaleRepositoryAdapter implements SaleRepositoryPort {
    private final SaleJpaRepository repo;

    public SaleRepositoryAdapter(SaleJpaRepository repo) {
        this.repo = repo;
    }

    @Override
    public Sale addSale(Sale sale) {
        try {
            SaleEntity saved = repo.save(SaleEntity.toEntity(sale));
            return SaleEntity.toDomain(saved);
        } catch (DataAccessException ex) {
            throw new PersistenceException("DB error during save of sale: %s".formatted(ex.getMessage()), ex);
        }
    }
}
