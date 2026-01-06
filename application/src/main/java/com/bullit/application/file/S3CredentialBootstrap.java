package com.bullit.application.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

@Component
public final class S3CredentialBootstrap {

    private static final Logger log = LoggerFactory.getLogger(S3CredentialBootstrap.class);

    private static final String ACCESS_KEY_ENV = "S3_ACCESS_KEY";
    private static final String SECRET_KEY_ENV = "S3_SECRET_KEY";
    private static final Path ENV_FILE = Path.of("./s3-credentials.env");

    private final S3ClientProperties props;
    private final GarageAdminClient admin;

    public S3CredentialBootstrap(S3ClientProperties props, GarageAdminClient admin) {
        this.props = Objects.requireNonNull(props, "props is required");
        this.admin = Objects.requireNonNull(admin, "admin is required");
    }

    public S3Credentials ensureS3Credentials() {
        return resolveCredentials()
                .orElse(createAndPersistCredentials());
    }

    private Optional<S3Credentials> resolveCredentials() {
        return Stream.of(credentialsFromSpringConfig(),
                        credentialsFromEnvironment()
                )
                .flatMap(Optional::stream)
                .findFirst();
    }

    private Optional<S3Credentials> credentialsFromSpringConfig() {
        return Optional.of(props)
                .filter(p -> hasText(p.accessKey()) && hasText(p.secretKey()))
                .map(p -> {
                    log.info("S3 credentials found in Spring configuration (s3.access-key / s3.secret-key)");
                    return S3Credentials.of(p.accessKey(), p.secretKey());
                });
    }

    private Optional<S3Credentials> credentialsFromEnvironment() {
        var ak = System.getenv(ACCESS_KEY_ENV);
        var sk = System.getenv(SECRET_KEY_ENV);

        return hasText(ak) && hasText(sk)
                ? Optional.of(logAndBuildEnvCredentials(ak, sk))
                : Optional.empty();
    }

    private S3Credentials logAndBuildEnvCredentials(String ak, String sk) {
        log.info("S3 credentials found in environment: {}/{}", ACCESS_KEY_ENV, SECRET_KEY_ENV);
        return S3Credentials.of(ak, sk);
    }

    private S3Credentials createAndPersistCredentials() {
        log.info("S3 credentials missing; creating via Garage admin API");
        var created = admin.createAccessKey(defaultKeyName());
        exposeForCurrentRunAndPersist(created);
        return created;
    }

    private void exposeForCurrentRunAndPersist(S3Credentials credentials) {
        exposeToCurrentProcess(credentials);
        persistEnvFile(credentials);
    }

    private String defaultKeyName() {
        return "hexagonal-app";
    }

    private void exposeToCurrentProcess(S3Credentials credentials) {
        System.setProperty(ACCESS_KEY_ENV, credentials.getAccessKey());
        System.setProperty(SECRET_KEY_ENV, credentials.getSecretKey());
        log.info("S3 credentials set as System properties for current run");
    }

    private void persistEnvFile(S3Credentials credentials) {
        try {
            var file = ENV_FILE.toAbsolutePath().normalize();
            var contents = """
                    %s=%s
                    %s=%s
                    """.formatted(
                    ACCESS_KEY_ENV, credentials.getAccessKey(),
                    SECRET_KEY_ENV, credentials.getSecretKey()
            );

            Files.createDirectories(file.getParent() == null ? Path.of(".") : file.getParent());
            Files.writeString(file, contents, StandardCharsets.UTF_8);

            log.info("Persisted S3 credentials to env file: {}", file);
            log.info("To reuse on restart, export them (or load the env file) before starting the app");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to persist S3 credentials env file", e);
        }
    }

    private static boolean hasText(String s) {
        return s != null && !s.isBlank();
    }
}