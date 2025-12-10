package com.bullit.core.usecase;

import com.bullit.domain.model.royalty.RoyaltyReport;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.model.stream.InputStreamPort;
import com.bullit.domain.port.driving.RoyaltyServicePort;
import com.bullit.domain.port.driven.AuthorRepositoryPort;
import com.bullit.domain.model.stream.StreamHandler;
import com.bullit.domain.model.stream.OutputStreamPort;
import jakarta.annotation.PostConstruct;

import java.time.Clock;
import java.time.YearMonth;

public final class SaleToRoyaltyHandler implements StreamHandler<Sale> {
    private final InputStreamPort<Sale> input;
    private final OutputStreamPort<RoyaltyReport> output;
    private final RoyaltyServicePort royaltyService;
    private final AuthorRepositoryPort authorRepository;
    private final Clock clock;

    public SaleToRoyaltyHandler(
            InputStreamPort<Sale> input,
            OutputStreamPort<RoyaltyReport> output,
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
    private void registerHandler() {
        input.subscribe(this);
    }

    @Override
    public void handle(Sale sale) {
        var author = authorRepository.findByBookId(sale.getBookId());

        YearMonth period = YearMonth.from(
                sale.getSoldAt().atZone(clock.getZone())
        );

        RoyaltyReport report = royaltyService.generateMonthlyReport(
                author.getId(),
                period
        );

        output.emit(report);
    }
}
