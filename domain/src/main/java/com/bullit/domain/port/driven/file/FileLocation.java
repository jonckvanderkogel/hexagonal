package com.bullit.domain.port.driven.file;

public record FileLocation(
        String bucket,
        String objectKey
) {}