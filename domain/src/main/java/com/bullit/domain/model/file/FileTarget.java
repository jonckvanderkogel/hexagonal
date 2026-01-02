package com.bullit.domain.model.file;

public record FileTarget(
        String bucket,
        String objectKey
) {}