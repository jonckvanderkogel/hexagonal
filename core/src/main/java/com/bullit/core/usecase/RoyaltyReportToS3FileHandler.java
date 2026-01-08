package com.bullit.core.usecase;

import com.bullit.domain.event.RoyaltyReportEvent;
import com.bullit.domain.event.TierBreakdownEvent;
import com.bullit.domain.port.driven.file.CsvRecordMapping;
import com.bullit.domain.port.driven.file.FileOutputPort;
import com.bullit.domain.port.driven.stream.InputStreamPort;
import com.bullit.domain.port.driving.stream.StreamHandler;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.YearMonth;
import java.util.List;
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
        output.emit(Stream.of(event), objectKeyFor(event), royaltyReportMapping());
    }

    private String objectKeyFor(RoyaltyReportEvent event) {
        var authorId = requiredText(event.getAuthorId(), "authorId");
        var period = YearMonth.parse(requiredText(event.getPeriod(), "period"));
        return "%s/%s.csv".formatted(authorId, period);
    }

    private CsvRecordMapping<RoyaltyReportEvent> royaltyReportMapping() {
        return CsvRecordMapping.of(
                List.of(
                        "authorId",
                        "period",
                        "units",
                        "grossRevenue",
                        "effectiveRate",
                        "royaltyDue",
                        "minimumGuarantee",
                        "tiers"
                ),
                e -> List.of(
                        requiredText(e.getAuthorId(), "authorId"),
                        requiredText(e.getPeriod(), "period"),
                        String.valueOf(e.getUnits()),
                        requiredText(String.valueOf(e.getGrossRevenue()), "grossRevenue"),
                        requiredText(String.valueOf(e.getEffectiveRate()), "effectiveRate"),
                        requiredText(String.valueOf(e.getRoyaltyDue()), "royaltyDue"),
                        requiredText(String.valueOf(e.getMinimumGuarantee()), "minimumGuarantee"),
                        requiredText(formatTiers(e.getBreakdown()), "tiers")
                )
        );
    }

    private String formatTiers(List<TierBreakdownEvent> tiers) {
        if (tiers == null || tiers.isEmpty()) return "";

        return tiers.stream()
                .map(this::formatTier)
                .reduce((a, b) -> a + "|" + b)
                .orElse("");
    }

    private String formatTier(TierBreakdownEvent t) {
        return "%d:%s:%s".formatted(
                t.getUnitsInTier(),
                String.valueOf(t.getAppliedRate()),
                String.valueOf(t.getRoyaltyAmount())
        );
    }

    private String requiredText(String value, String field) {
        if (value == null || value.isBlank() || value.equalsIgnoreCase("null")) {
            throw new IllegalArgumentException("Missing required value: " + field);
        }
        return value.trim();
    }
}