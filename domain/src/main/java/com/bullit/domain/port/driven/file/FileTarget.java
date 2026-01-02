package com.bullit.domain.port.driven.file;

public record FileTarget(
        String bucket,
        String objectKey
) {}