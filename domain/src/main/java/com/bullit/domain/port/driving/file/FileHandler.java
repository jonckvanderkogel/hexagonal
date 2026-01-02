package com.bullit.domain.port.driving.file;

import com.bullit.domain.port.driven.file.FileEnvelope;

@FunctionalInterface
public interface FileHandler<T> {
    void handle(FileEnvelope file);
}
