package com.bullit.core.usecase;

import com.bullit.domain.event.SaleEvent;
import com.bullit.domain.model.royalty.Sale;
import com.bullit.domain.port.driven.file.FileEnvelope;
import com.bullit.domain.port.driven.file.FileInputPort;
import com.bullit.domain.port.driven.stream.OutputStreamPort;
import com.bullit.domain.port.driving.file.FileHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public final class SaleFileToKafkaHandler implements FileHandler<Sale> {

    private static final Logger log = LoggerFactory.getLogger(SaleFileToKafkaHandler.class);

    private final FileInputPort<Sale> input;
    private final OutputStreamPort<SaleEvent> output;
    private final ObjectMapper mapper;

    public SaleFileToKafkaHandler(
            FileInputPort<Sale> input,
            OutputStreamPort<SaleEvent> output,
            ObjectMapper mapper
    ) {
        this.input = input;
        this.output = output;
        this.mapper = mapper;
    }

    @PostConstruct
    void register() {
        log.info("SaleFileToKafkaHandler registering file handler");
        input.subscribe(this);
    }

    @Override
    public void handle(FileEnvelope file) {
        log.info("Handling sale file: {}/{}", file.location().bucket(), file.location().objectKey());

        parseSales(file.lines())
                .map(Sale::toEvent)
                .forEach(output::emit);
    }

    private Stream<Sale> parseSales(Stream<String> lines) {
        return lines
                .filter(s -> !s.isBlank())
                .map(this::parseSaleJson);
    }

    private Sale parseSaleJson(String json) {
        try {
            return mapper.readValue(json, Sale.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid sale json line", e);
        }
    }
}
