package com.bullit.domain.port.driven.file;

import com.bullit.domain.port.driving.file.FileHandler;

@FunctionalInterface
public interface FileInputPort<T> {
    void subscribe(FileHandler<T> handler, CsvRowMapper<T> mapper);
}