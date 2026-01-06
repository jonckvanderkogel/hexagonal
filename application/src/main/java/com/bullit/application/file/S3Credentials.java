package com.bullit.application.file;

import jakarta.validation.constraints.NotBlank;

import static com.bullit.domain.model.DomainValidator.assertValid;

public class S3Credentials {
    @NotBlank(message = "AccessKey is required")
    private final String accessKey;
    @NotBlank(message = "SecretKey is required")
    private final String secretKey;

    private S3Credentials(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public static S3Credentials of(String accessKey, String secretKey) {
        return assertValid(new S3Credentials(accessKey, secretKey));
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }
}