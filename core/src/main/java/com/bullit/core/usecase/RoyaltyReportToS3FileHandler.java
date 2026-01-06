package com.bullit.core.usecase;

import com.bullit.domain.event.RoyaltyReportEvent;
import com.bullit.domain.port.driven.file.FileOutputPort;
import com.bullit.domain.port.driven.stream.InputStreamPort;
import com.bullit.domain.port.driving.stream.StreamHandler;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.YearMonth;
import java.util.stream.Stream;

public final class RoyaltyReportToS3FileHandler implements StreamHandler<RoyaltyReportEvent> {

    private static final Logger log = LoggerFactory.getLogger(RoyaltyReportToS3FileHandler.class);

    private final InputStreamPort<RoyaltyReportEvent> input;
    private final FileOutputPort<RoyaltyReportEvent> output;

    public RoyaltyReportToS3FileHandler(
            InputStreamPort<RoyaltyReportEvent> input,
            FileOutputPort<RoyaltyReportEvent> output
    ) {
        this.input = input;
        this.output = output;
    }

    @PostConstruct
    void register() {
        log.info("RoyaltyReportToS3FileHandler registering stream handler");
        input.subscribe(this);
    }

    @Override
    public void handle(RoyaltyReportEvent event) {
        output.emit(Stream.of(event), objectKeyFor(event));
    }

    private String objectKeyFor(RoyaltyReportEvent event) {
        var authorId = event.getAuthorId();
        var period = YearMonth.parse(event.getPeriod());
        return"%s/%s.json".formatted(authorId, period);
    }
}
