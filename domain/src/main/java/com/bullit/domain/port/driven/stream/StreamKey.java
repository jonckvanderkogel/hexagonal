package com.bullit.domain.port.driven.stream;

import java.util.function.Function;

public interface StreamKey<T> extends Function<T, String> {
    Class<T> payloadType();
}