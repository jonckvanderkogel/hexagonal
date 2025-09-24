package com.bullit.web.adapter.driving.http;

import com.bullit.domain.model.library.Author;
import com.bullit.domain.model.library.Book;
import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.royalty.TierBreakdown;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public class Response {
    public record AuthorResponse(
            String id,
            String firstName,
            String lastName,
            List<BookResponse> books,
            Instant insertedAt
    ) {
        public static AuthorResponse fromDomain(Author a) {
            return new AuthorResponse(
                    a.getId().toString(),
                    a.getFirstName(),
                    a.getLastName(),
                    a.getBooks().stream().map(BookResponse::fromDomain).toList(),
                    a.getInsertedAt()
            );
        }
    }

    public record BookResponse(
            String id,
            String authorId,
            String title,
            Instant insertedAt
    ) {
        public static BookResponse fromDomain(Book b) {
            return new BookResponse(
                    b.getId().toString(),
                    b.getAuthorId().toString(),
                    b.getTitle(),
                    b.getInsertedAt()
            );
        }
    }

    public record ErrorResponse(String error) {}

    public record TierBreakdownResponse(
            long unitsInTier,
            BigDecimal appliedRate,
            BigDecimal royaltyAmount
    ) {
        public static TierBreakdownResponse fromDomain(TierBreakdown t) {
            return new TierBreakdownResponse(
                    t.getUnitsInTier(),
                    t.getAppliedRate(),
                    t.getRoyaltyAmount()
            );
        }
    }

    public record RoyaltyReportResponse(
            String authorId,
            String period,                // yyyy-MM
            long totalUnits,
            BigDecimal grossRevenue,
            BigDecimal effectiveRate,
            BigDecimal royaltyDue,
            BigDecimal minimumGuarantee,
            List<TierBreakdownResponse> breakdown
    ) {
        public static RoyaltyReportResponse fromDomain(RoyaltyReport r) {
            return new RoyaltyReportResponse(
                    r.getAuthorId().toString(),
                    r.getPeriod().toString(),
                    r.getUnits(),
                    r.getGrossRevenue(),
                    r.getEffectiveRate(),
                    r.getRoyaltyDue(),
                    r.getMinimumGuarantee(),
                    r.getBreakdown().stream().map(TierBreakdownResponse::fromDomain).toList()
            );
        }
    }
}
