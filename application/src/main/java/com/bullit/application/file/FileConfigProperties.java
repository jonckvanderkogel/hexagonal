package com.bullit.application.file;

import com.bullit.domain.port.driving.file.FileHandler;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

@Validated
@ConfigurationProperties(prefix = "files")
public record FileConfigProperties(
        List<@Valid InputConfig> inputs,
        List<@Valid OutputConfig> outputs,
        List<@Valid HandlerConfig> handlers
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

    public record InputConfig(
            @NotNull(message = "files.inputs[].payload-type is required")
            Class<?> payloadType,

            @NotBlank(message = "files.inputs[].bucket is required")
            String bucket,

            String incomingPrefix,
            String handledPrefix,
            String errorPrefix,
            Duration pollInterval
    ) {
        public InputConfig {
            incomingPrefix = canonicalPrefix(incomingPrefix);
            handledPrefix = canonicalPrefix(handledPrefix);
            errorPrefix = canonicalPrefix(errorPrefix);
            pollInterval = pollInterval == null ? Duration.ofSeconds(1) : pollInterval;
        }

        @AssertTrue(message = """
            files.inputs[] prefixes are invalid:
            - if incoming-prefix is set, handled-prefix and error-prefix must be set
            - handled-prefix and error-prefix must differ from incoming-prefix and from each other
            - if incoming-prefix is empty, handled-prefix and error-prefix must be empty
            """)
        public boolean prefixesAreConsistent() {
            if (incomingPrefix.isEmpty()) {
                return handledPrefix.isEmpty() && errorPrefix.isEmpty();
            }
            if (handledPrefix.isEmpty() || errorPrefix.isEmpty()) return false;

            return !incomingPrefix.equals(handledPrefix)
                    && !incomingPrefix.equals(errorPrefix)
                    && !handledPrefix.equals(errorPrefix);
        }

        @AssertTrue(message = "files.inputs[] prefixes must be empty or end with '/'")
        public boolean prefixesMustEndWithSlashWhenSet() {
            return isEmptyOrEndsWithSlash(incomingPrefix)
                    && isEmptyOrEndsWithSlash(handledPrefix)
                    && isEmptyOrEndsWithSlash(errorPrefix);
        }

        @AssertTrue(message = "files.inputs[] prefixes must not start with '/'")
        public boolean prefixesMustNotStartWithSlash() {
            return startsWithSlash(incomingPrefix)
                    && startsWithSlash(handledPrefix)
                    && startsWithSlash(errorPrefix);
        }

        @AssertTrue(message = "files.inputs[].poll-interval must be positive")
        public boolean pollIntervalMustBePositive() {
            return !pollInterval.isZero() && !pollInterval.isNegative();
        }

        private static String canonicalPrefix(String s) {
            return s == null ? "" : s.trim();
        }

        private static boolean isEmptyOrEndsWithSlash(String s) {
            return s.isEmpty() || s.endsWith("/");
        }

        private static boolean startsWithSlash(String s) {
            return !s.startsWith("/");
        }
    }

    public record OutputConfig(
            @NotNull(message = "files.outputs[].payload-type is required")
            Class<?> payloadType,

            @NotBlank(message = "files.outputs[].bucket is required")
            String bucket
    ) {
    }

    public record HandlerConfig(
            @NotNull(message = "files.handlers[].handler-class is required")
            Class<? extends FileHandler<?>> handlerClass
    ) {
    }
}