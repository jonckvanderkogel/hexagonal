package com.bullit.domain.port.driven.file;

import java.util.stream.Stream;

@FunctionalInterface
public interface FileOutputPort<T> {
    void emit(Stream<T> contents, String objectKey, CsvRecordMapping<T> mapping);
}