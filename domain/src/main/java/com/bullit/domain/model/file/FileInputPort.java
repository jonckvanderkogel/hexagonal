package com.bullit.domain.model.file;

@FunctionalInterface
public interface FileInputPort<T> {
    void subscribe(FileHandler<T> handler);
}
