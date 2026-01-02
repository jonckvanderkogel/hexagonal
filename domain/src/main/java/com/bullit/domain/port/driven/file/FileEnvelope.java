package com.bullit.domain.port.driven.file;

import java.util.stream.Stream;

public record FileEnvelope(
        FileTarget target,
        Stream<String> lines
) {}
