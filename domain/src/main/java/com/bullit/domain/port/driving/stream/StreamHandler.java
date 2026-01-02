package com.bullit.domain.port.driving.stream;

@FunctionalInterface
public interface StreamHandler<T> {
    void handle(T element);
}
