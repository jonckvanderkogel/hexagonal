package com.bullit.core.usecase;

import com.bullit.domain.event.RoyaltyReportEvent;
import com.bullit.domain.event.SaleEvent;
import com.bullit.domain.model.library.Author;
import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.model.royalty.TierBreakdown;
import com.bullit.domain.model.stream.InputStreamPort;
import com.bullit.domain.model.stream.OutputStreamPort;
import com.bullit.domain.port.driven.AuthorRepositoryPort;
import com.bullit.domain.port.driving.RoyaltyServicePort;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

final class SaleToRoyaltyHandlerTest {
    private final InputStreamPort<SaleEvent> input = mock(InputStreamPort.class);
    private final OutputStreamPort<RoyaltyReportEvent> output = mock(OutputStreamPort.class);
    private final RoyaltyServicePort royaltyService = mock(RoyaltyServicePort.class);
    private final AuthorRepositoryPort authorRepository = mock(AuthorRepositoryPort.class);

    private final Clock fixedClock =
            Clock.fixed(Instant.parse("2025-01-15T10:30:00Z"), ZoneOffset.UTC);

    @Test
    void registersItselfWithInputStreamOnPostConstruct() {
        var handler = new SaleToRoyaltyHandler(
                input,
                output,
                royaltyService,
                authorRepository,
                fixedClock
        );

        handler.registerHandler();

        verify(input, times(1)).subscribe(handler);
        verifyNoMoreInteractions(input, output, royaltyService, authorRepository);
    }

    @Test
    void onSale_findsAuthor_generatesMonthlyReport_andEmitsIt() {
        UUID bookId = UUID.randomUUID();
        UUID authorId = UUID.randomUUID();

        Instant soldAt = Instant.parse("2025-01-10T12:00:00Z");
        Sale sale = Sale.rehydrate(
                UUID.randomUUID(),
                bookId,
                3,
                new BigDecimal("59.97"),
                soldAt
        );

        Author author = Author.rehydrate(
                authorId,
                "Douglas",
                "Adams",
                List.of(),
                Instant.parse("2024-01-01T00:00:00Z")
        );

        when(authorRepository.findByBookId(bookId)).thenReturn(author);

        YearMonth expectedPeriod = YearMonth.of(2025, 1);

        RoyaltyReport report =
                RoyaltyReport.of(
                        authorId,
                        expectedPeriod,
                        3L,
                        new BigDecimal("59.97"),
                        new BigDecimal("0.15"),
                        new BigDecimal("8.9955"),
                        new BigDecimal("5.00"),
                        List.of(
                                TierBreakdown.of(
                                        3L,
                                        new BigDecimal("0.15"),
                                        new BigDecimal("8.9955")
                                )
                        )
                );

        when(royaltyService.generateMonthlyReport(authorId, expectedPeriod))
                .thenReturn(report);

        var handler = new SaleToRoyaltyHandler(
                input,
                output,
                royaltyService,
                authorRepository,
                fixedClock
        );

        handler.handle(Sale.toEvent(sale));

        assertSoftly(s -> {
            s.check(() ->
                    verify(authorRepository, times(1))
                            .findByBookId(bookId)
            );
            s.check(() ->
                    verify(royaltyService, times(1))
                            .generateMonthlyReport(authorId, expectedPeriod)
            );
            s.check(() ->
                    verify(output, times(1))
                            .emit(RoyaltyReport.toEvent(report))
            );
        });

        verifyNoMoreInteractions(output, royaltyService);
    }
}