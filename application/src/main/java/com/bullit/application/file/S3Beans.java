package com.bullit.application.file;

import io.minio.MinioClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class S3Beans {

    @Bean
    public S3Credentials s3Credentials(S3CredentialBootstrap bootstrap) {
        return bootstrap.ensureS3Credentials();
    }

    @Bean
    public MinioClient minioClient(S3ClientProperties props, S3Credentials creds) {
        return S3ClientFactory.create(props.withCredentials(creds.getAccessKey(), creds.getSecretKey()));
    }
}
