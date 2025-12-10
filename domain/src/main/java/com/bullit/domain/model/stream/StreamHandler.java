package com.bullit.domain.model.stream;

@FunctionalInterface
public interface StreamHandler<T> {
    void handle(T element);
}
