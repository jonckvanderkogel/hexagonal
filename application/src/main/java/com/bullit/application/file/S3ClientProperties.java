package com.bullit.application.file;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "s3")
public record S3ClientProperties(
        @NotBlank(message = "s3.endpoint is required")
        @Pattern(
                regexp = "^[a-zA-Z0-9.-]+$",
                message = "s3.endpoint must be a hostname (no scheme, no port)"
        )
        String endpoint,

        @Min(value = 1, message = "s3.port must be >= 1")
        @Max(value = 65535, message = "s3.port must be <= 65535")
        int port,

        boolean secure,

        String accessKey,
        String secretKey,

        @NotBlank(message = "s3.region is required")
        String region
) {
    public S3ClientProperties withCredentials(String accessKey, String secretKey) {
        return new S3ClientProperties(endpoint, port, secure, accessKey, secretKey, region);
    }

    @AssertTrue(message = "s3.access-key and s3.secret-key must either both be set or both be empty")
    public boolean credentialsAreConsistent() {
        return isBlank(accessKey) == isBlank(secretKey);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}