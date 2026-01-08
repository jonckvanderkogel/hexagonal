package com.bullit.domain.port.driven.file;

import java.util.List;
import java.util.function.Function;

public record CsvRecordMapping<T>(
        List<String> header,
        Function<T, List<String>> toFields
) {
    public static <T> CsvRecordMapping<T> of(List<String> header, Function<T, List<String>> toFields) {
        return new CsvRecordMapping<>(header, toFields);
    }
}