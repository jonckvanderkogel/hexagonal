package com.bullit.data.adapter.driven.jpa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Repository
public interface SaleJpaRepository extends JpaRepository<SaleEntity, UUID> {

    interface MonthlyAggregation {
        boolean getAuthorExists();
        Long getUnits();
        BigDecimal getGross();
    }

    @Query(value = """
            SELECT
              EXISTS (SELECT 1 FROM authors a WHERE a.id = :authorId) AS author_exists,
              COALESCE(SUM(s.units), 0)                               AS units,
              COALESCE(SUM(s.amount_eur), 0)   AS gross
            FROM sales s
            JOIN books b ON b.id = s.book_id
            WHERE b.author_id = :authorId
              AND s.sold_at >= :start
              AND s.sold_at <  :end
            """,
            nativeQuery = true)
    MonthlyAggregation sumForAuthorBetween(
            @Param("authorId") UUID authorId,
            @Param("start") Instant start,
            @Param("end") Instant end
    );
}