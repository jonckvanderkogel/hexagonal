package com.bullit.domain.model.file;

@FunctionalInterface
public interface FileHandler<T> {
    void handle(FileEnvelope file);
}
