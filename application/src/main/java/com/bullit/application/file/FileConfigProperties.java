package com.bullit.application.file;

import com.bullit.domain.model.file.FileHandler;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "files")
public record FileConfigProperties(
        List<InputConfig> inputs,
        List<OutputConfig> outputs,
        List<HandlerConfig> handlers
) {
    public record InputConfig(
            Class<?> payloadType,
            String bucket,
            String incomingPrefix,
            String handledPrefix,
            String errorPrefix,
            Duration pollInterval
    ) {}

    public record OutputConfig(
            Class<?> payloadType
    ) {}

    public record HandlerConfig(
            Class<? extends FileHandler<?>> handlerClass
    ) {}
}