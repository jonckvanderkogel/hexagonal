package com.bullit.application.streaming;

import com.bullit.domain.port.driven.stream.StreamKey;
import com.bullit.domain.port.driving.stream.BatchStreamHandler;
import com.bullit.domain.port.driving.stream.StreamHandler;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

@Validated
@ConfigurationProperties(prefix = "streams")
public record StreamConfigProperties(
        List<@Valid InputConfig> inputs,
        List<@Valid OutputConfig> outputs,
        List<@Valid HandlerConfig> handlers,
        List<@Valid BatchHandlerConfig> batchStreamHandlers
) {
    public List<InputConfig> inputsOrEmpty() {
        return inputs == null ? List.of() : inputs;
    }

    public List<OutputConfig> outputsOrEmpty() {
        return outputs == null ? List.of() : outputs;
    }

    public List<HandlerConfig> handlersOrEmpty() {
        return handlers == null ? List.of() : handlers;
    }

    public List<BatchHandlerConfig> batchStreamHandlersOrEmpty() {
        return batchStreamHandlers == null ? List.of() : batchStreamHandlers;
    }

    public record InputConfig(
            @NotNull(message = "streams.inputs[].payload-type is required")
            Class<?> payloadType,

            @NotBlank(message = "streams.inputs[].topic is required")
            String topic,

            @NotBlank(message = "streams.inputs[].group-id is required")
            String groupId,

            @Max(value = 50000, message = "streams.inputs[].partition-queue-capacity must be between 1 and 50.000")
            int partitionQueueCapacity,

            @Max(value = 10000, message = "streams.inputs[].max-batch-size must be between 1 and 10.000")
            int maxBatchSize
    ) {
        public InputConfig {
            partitionQueueCapacity = partitionQueueCapacity <= 0 ? 1000 : partitionQueueCapacity;
            maxBatchSize = maxBatchSize <= 0 ? 1000 : maxBatchSize;
        }

        @AssertTrue(message = "streams.inputs[].max-batch-size cannot be larger than streams.inputs[].partition-queue-capacity")
        public boolean isBatchSizeValid() {
            return maxBatchSize <= partitionQueueCapacity;
        }
    }

    public record OutputConfig(
            @NotNull(message = "streams.outputs[].payload-type is required")
            Class<?> payloadType,

            @NotBlank(message = "streams.outputs[].topic is required")
            String topic,

            Class<? extends StreamKey<?>> key
    ) {
    }

    public record HandlerConfig(
            @NotNull(message = "streams.handlers[].handler-class is required")
            Class<? extends StreamHandler<?>> handlerClass
    ) {
    }
    public record BatchHandlerConfig(
            @NotNull(message = "streams.batch-handlers[].handler-class is required")
            Class<? extends BatchStreamHandler<?>> handlerClass
    ) {
    }
}