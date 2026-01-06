package com.bullit.domain.port.driven.file;

import java.util.stream.Stream;

public record FileEnvelope(
        FileLocation location,
        Stream<String> lines
) {}
