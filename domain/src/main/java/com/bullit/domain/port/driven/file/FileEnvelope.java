package com.bullit.domain.port.driven.file;

import java.util.stream.Stream;

public record FileEnvelope<T>(
        FileLocation location,
        Stream<T> records
) {
}