package com.bullit.domain.port.driven.file;

@FunctionalInterface
public interface CsvRowMapper<T> {
    T map(CsvRow row);
}