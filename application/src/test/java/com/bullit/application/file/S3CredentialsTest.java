package com.bullit.application.file;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3CredentialsTest {

    @Test
    void validCredentials_areCreatedSuccessfully() {
        var creds = S3Credentials.of("access-key", "secret-key");

        assertThat(creds.getAccessKey()).isEqualTo("access-key");
        assertThat(creds.getSecretKey()).isEqualTo("secret-key");
    }

    @Test
    void accessKey_isRequired() {
        assertThatThrownBy(() ->
                S3Credentials.of("", "secret-key")
        )
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(
                        ex -> assertThat(ex.getMessage())
                                .contains("AccessKey is required"));
    }

    @Test
    void secretKey_isRequired() {
        assertThatThrownBy(() ->
                S3Credentials.of("access-key", "")
        )
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(
                        ex -> assertThat(ex.getMessage())
                                .contains("SecretKey is required"));
    }

    @Test
    void bothKeys_areValidatedTogether() {
        assertThatThrownBy(() ->
                S3Credentials.of(" ", " ")
        )
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(ex -> {
                    var message = ex.getMessage();
                    assertThat(message).contains("AccessKey is required");
                    assertThat(message).contains("SecretKey is required");
                });
    }

    @Test
    void nullKeys_areRejected() {
        assertThatThrownBy(() ->
                S3Credentials.of(null, null)
        )
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(ex -> {
                    var message = ex.getMessage();
                    assertThat(message).contains("AccessKey is required");
                    assertThat(message).contains("SecretKey is required");
                });
    }
}