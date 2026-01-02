package com.bullit.domain.port.driven.stream;

import com.bullit.domain.port.driving.stream.StreamHandler;

@FunctionalInterface
public interface InputStreamPort<T> {
    void subscribe(StreamHandler<T> handler);
}
