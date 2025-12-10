package com.bullit.domain.model.stream;

@FunctionalInterface
public interface OutputStreamPort<T> {
    void emit(T element);
}
