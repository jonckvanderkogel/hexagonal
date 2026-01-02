package com.bullit.application.file;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "s3")
public record S3ClientProperties(
        String endpoint,
        String accessKey,
        String secretKey,
        boolean secure,
        String region
) {}