package com.bullit.domain.port.driving.stream;

import java.util.List;

@FunctionalInterface
public interface BatchStreamHandler<T> {
    void handleBatch(List<T> events);
}