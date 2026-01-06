package com.bullit.application.file;

import io.minio.MinioClient;

public final class S3ClientFactory {
    private S3ClientFactory() {}

    public static MinioClient create(S3ClientProperties props) {
        return MinioClient.builder()
                .endpoint(props.endpoint(), props.port(), props.secure())
                .credentials(props.accessKey(), props.secretKey())
                .region(props.region())
                .build();
    }
}
