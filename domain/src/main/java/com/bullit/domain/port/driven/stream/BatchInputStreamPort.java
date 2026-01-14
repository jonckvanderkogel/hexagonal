package com.bullit.domain.port.driven.stream;

import com.bullit.domain.port.driving.stream.BatchStreamHandler;

@FunctionalInterface
public interface BatchInputStreamPort<T> {
    void subscribeBatch(BatchStreamHandler<T> handler);
}
