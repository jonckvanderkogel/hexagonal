package com.bullit.domain.model.file;

import java.util.stream.Stream;

public record FileEnvelope(
        FileTarget target,
        Stream<String> lines
) {}
