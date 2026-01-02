package com.bullit.application.streaming;

import com.bullit.domain.port.driving.stream.StreamHandler;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "streams")
public record StreamConfigProperties(
        List<InputConfig> inputs,
        List<OutputConfig> outputs,
        List<HandlerConfig> handlers
) {
    public record InputConfig(
            Class<?> payloadType,
            String topic,
            String groupId
    ) {}

    public record OutputConfig(
            Class<?> payloadType,
            String topic
    ) {}

    public record HandlerConfig(
            Class<? extends StreamHandler<?>> handlerClass
    ) {}
}