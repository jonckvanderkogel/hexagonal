package com.bullit.application.file;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "garage.admin")
public record GarageAdminProperties(
        @NotBlank(message = "garage.admin.host is required")
        String host,

        @Min(value = 1, message = "garage.admin.port must be >= 1")
        @Max(value = 65535, message = "garage.admin.port must be <= 65535")
        int port,

        boolean secure,

        @NotBlank(message = "garage.admin.token is required")
        String token
) {
    public String baseUrl() {
        var scheme = secure ? "https" : "http";
        return "%s://%s:%d".formatted(scheme, host, port);
    }
}