package com.bullit.data.adapter.driven.adapter;

import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.port.driven.SaleRepositoryPort;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.PersistenceException;
import org.hibernate.query.NativeQuery;
import org.hibernate.type.StandardBasicTypes;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public final class SaleRepositoryAdapter implements SaleRepositoryPort {

    private static final String INSERT_WHERE_EXISTS_SQL = """
            INSERT INTO sales (id, book_id, units, amount_eur, sold_at)
            SELECT :id, :bookId, :units, :amount, :soldAt
            WHERE EXISTS (SELECT 1 FROM books WHERE id = :bookId)
            RETURNING id, book_id, units, amount_eur, sold_at
            """;

    private final EntityManager em;

    public SaleRepositoryAdapter(EntityManager em) {
        this.em = em;
    }

    @Override
    public Sale addSale(Sale sale) {
        try {
            final Object[] row = (Object[]) em
                    .createNativeQuery(INSERT_WHERE_EXISTS_SQL)
                    .unwrap(NativeQuery.class)
                    .addScalar("id", StandardBasicTypes.UUID)
                    .addScalar("book_id", StandardBasicTypes.UUID)
                    .addScalar("units", StandardBasicTypes.INTEGER)
                    .addScalar("amount_eur", StandardBasicTypes.BIG_DECIMAL)
                    .addScalar("sold_at", StandardBasicTypes.INSTANT)
                    .setParameter("id", sale.getId())
                    .setParameter("bookId", sale.getBookId())
                    .setParameter("units", sale.getUnits())
                    .setParameter("amount", sale.getAmountEur())
                    .setParameter("soldAt", sale.getSoldAt())
                    .getSingleResult();

            final UUID id = (UUID) row[0];
            final UUID bookId = (UUID) row[1];
            final int units = ((Number) row[2]).intValue();
            final BigDecimal amt = (BigDecimal) row[3];
            final Instant soldAt = ((Instant) row[4]);

            return Sale.rehydrate(id, bookId, units, amt, soldAt);

        } catch (NoResultException e) {
            throw new NotFoundException("Book %s not found".formatted(sale.getBookId()));
        } catch (PersistenceException e) {
            throw new DatabaseInteractionException("DB error during save of sale", e);
        }
    }
}