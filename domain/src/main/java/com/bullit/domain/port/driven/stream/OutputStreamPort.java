package com.bullit.domain.port.driven.stream;

@FunctionalInterface
public interface OutputStreamPort<T> {
    void emit(T element);
}
