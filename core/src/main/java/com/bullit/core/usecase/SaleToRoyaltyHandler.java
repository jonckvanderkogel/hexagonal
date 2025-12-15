package com.bullit.core.usecase;

import com.bullit.domain.event.RoyaltyReportEvent;
import com.bullit.domain.event.SaleEvent;
import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.stream.InputStreamPort;
import com.bullit.domain.model.stream.OutputStreamPort;
import com.bullit.domain.model.stream.StreamHandler;
import com.bullit.domain.port.driven.AuthorRepositoryPort;
import com.bullit.domain.port.driving.RoyaltyServicePort;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.YearMonth;
import java.util.UUID;

public final class SaleToRoyaltyHandler implements StreamHandler<SaleEvent> {
    private static final Logger log = LoggerFactory.getLogger(SaleToRoyaltyHandler.class);

    private final InputStreamPort<SaleEvent> input;
    private final OutputStreamPort<RoyaltyReportEvent> output;
    private final RoyaltyServicePort royaltyService;
    private final AuthorRepositoryPort authorRepository;
    private final Clock clock;

    public SaleToRoyaltyHandler(
            InputStreamPort<SaleEvent> input,
            OutputStreamPort<RoyaltyReportEvent> output,
            RoyaltyServicePort royaltyService,
            AuthorRepositoryPort authorRepository,
            Clock clock) {
        this.input = input;
        this.output = output;
        this.royaltyService = royaltyService;
        this.authorRepository = authorRepository;
        this.clock = clock;
    }

    @PostConstruct
    void registerHandler() {
        input.subscribe(this);
    }

    @Override
    public void handle(SaleEvent event) {
        UUID bookId = UUID.fromString(event.getBookId());
        var author = authorRepository.findByBookId(bookId);

        YearMonth period = YearMonth
                .from(Instant.ofEpochMilli(event.getSoldAt()).atZone(clock.getZone()));


        RoyaltyReport report = royaltyService.generateMonthlyReport(
                author.getId(),
                period
        );

        RoyaltyReportEvent reportEvent = RoyaltyReport.toEvent(report);

        output.emit(reportEvent);
    }
}
