package com.bullit.application.file;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import static com.bullit.application.FunctionUtils.HOST_PATTERN;

@Validated
@ConfigurationProperties(prefix = "garage.admin")
public record GarageAdminProperties(
        @NotBlank(message = "garage.admin.host is required")
        @Pattern(
                regexp = HOST_PATTERN,
                message = "garage.admin.host must be a valid hostname or IP address (no scheme, no port, no path)"
        )
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