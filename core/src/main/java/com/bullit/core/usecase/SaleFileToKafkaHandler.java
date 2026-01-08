package com.bullit.core.usecase;

import com.bullit.domain.event.SaleEvent;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.port.driven.file.CsvRow;
import com.bullit.domain.port.driven.file.FileEnvelope;
import com.bullit.domain.port.driven.file.FileInputPort;
import com.bullit.domain.port.driven.stream.OutputStreamPort;
import com.bullit.domain.port.driving.file.FileHandler;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.bullit.domain.port.driven.file.CsvValues.requiredDecimal;
import static com.bullit.domain.port.driven.file.CsvValues.requiredInstant;
import static com.bullit.domain.port.driven.file.CsvValues.requiredInt;
import static com.bullit.domain.port.driven.file.CsvValues.requiredUuid;

public final class SaleFileToKafkaHandler implements FileHandler<Sale> {

    private static final Logger log = LoggerFactory.getLogger(SaleFileToKafkaHandler.class);

    private final FileInputPort<Sale> input;
    private final OutputStreamPort<SaleEvent> output;

    public SaleFileToKafkaHandler(
            FileInputPort<Sale> input,
            OutputStreamPort<SaleEvent> output
    ) {
        this.input = input;
        this.output = output;
    }

    @PostConstruct
    void register() {
        log.info("SaleFileToKafkaHandler registering file handler");
        input.subscribe(this, this::mapSale);
    }

    @Override
    public void handle(FileEnvelope<Sale> file) {
        log.info("Handling sale file: {}/{}", file.location().bucket(), file.location().objectKey());

        file.records()
                .map(Sale::toEvent)
                .forEach(output::emit);
    }

    private Sale mapSale(CsvRow row) {
        return Sale.rehydrate(
                requiredUuid(row, "id"),
                requiredUuid(row, "bookId"),
                requiredInt(row, "units"),
                requiredDecimal(row, "amountEur"),
                requiredInstant(row, "soldAt")
        );
    }
}