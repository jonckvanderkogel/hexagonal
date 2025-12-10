package com.bullit.domain.model.stream;

@FunctionalInterface
public interface InputStreamPort<T> {
    void subscribe(StreamHandler<T> handler);
}
