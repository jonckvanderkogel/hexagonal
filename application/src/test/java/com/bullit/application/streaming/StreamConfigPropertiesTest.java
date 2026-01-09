package com.bullit.application.streaming;

import com.bullit.domain.port.driven.stream.StreamKey;
import com.bullit.domain.port.driving.stream.StreamHandler;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.bullit.application.TestUtils.anyMessageContains;
import static com.bullit.application.TestUtils.assertNoViolations;
import static com.bullit.application.TestUtils.validate;
import static org.assertj.core.api.Assertions.assertThat;

class StreamConfigPropertiesTest {

    @Test
    void validConfig_hasNoViolations() {
        var config = new StreamConfigProperties(
                List.of(new StreamConfigProperties.InputConfig(String.class, "in-topic", "group-1")),
                List.of(new StreamConfigProperties.OutputConfig(String.class, "out-topic", TestStreamKey.class)),
                List.of(new StreamConfigProperties.HandlerConfig(TestStreamHandler.class))
        );

        assertNoViolations(validate(config));
    }

    @Test
    void inputsOrEmpty_returnsEmptyListWhenInputsNull() {
        var config = new StreamConfigProperties(null, List.of(), List.of());

        assertThat(config.inputsOrEmpty()).isEmpty();
    }

    @Test
    void outputsOrEmpty_returnsEmptyListWhenOutputsNull() {
        var config = new StreamConfigProperties(List.of(), null, List.of());

        assertThat(config.outputsOrEmpty()).isEmpty();
    }

    @Test
    void handlersOrEmpty_returnsEmptyListWhenHandlersNull() {
        var config = new StreamConfigProperties(List.of(), List.of(), null);

        assertThat(config.handlersOrEmpty()).isEmpty();
    }

    @Test
    void inputConfig_payloadType_isRequired() {
        var config = new StreamConfigProperties(
                List.of(new StreamConfigProperties.InputConfig(null, "topic", "group")),
                List.of(),
                List.of()
        );

        var violations = validate(config);

        anyMessageContains(violations, "streams.inputs[].payload-type is required");
    }

    @Test
    void inputConfig_topic_isRequired() {
        var config = new StreamConfigProperties(
                List.of(new StreamConfigProperties.InputConfig(String.class, "", "group")),
                List.of(),
                List.of()
        );

        var violations = validate(config);

        anyMessageContains(violations, "streams.inputs[].topic is required");
    }

    @Test
    void inputConfig_groupId_isRequired() {
        var config = new StreamConfigProperties(
                List.of(new StreamConfigProperties.InputConfig(String.class, "topic", "")),
                List.of(),
                List.of()
        );

        var violations = validate(config);

        anyMessageContains(violations, "streams.inputs[].group-id is required");
    }

    @Test
    void outputConfig_payloadType_isRequired() {
        var config = new StreamConfigProperties(
                List.of(),
                List.of(new StreamConfigProperties.OutputConfig(null, "topic", null)),
                List.of()
        );

        var violations = validate(config);

        anyMessageContains(violations, "streams.outputs[].payload-type is required");
    }

    @Test
    void outputConfig_topic_isRequired() {
        var config = new StreamConfigProperties(
                List.of(),
                List.of(new StreamConfigProperties.OutputConfig(String.class, "", null)),
                List.of()
        );

        var violations = validate(config);

        anyMessageContains(violations, "streams.outputs[].topic is required");
    }

    @Test
    void outputConfig_key_isOptional() {
        var config = new StreamConfigProperties(
                List.of(),
                List.of(new StreamConfigProperties.OutputConfig(String.class, "topic", null)),
                List.of()
        );

        assertNoViolations(validate(config));
    }

    @Test
    void handlerConfig_handlerClass_isRequired() {
        var config = new StreamConfigProperties(
                List.of(),
                List.of(),
                List.of(new StreamConfigProperties.HandlerConfig(null))
        );

        var violations = validate(config);

        anyMessageContains(violations, "streams.handlers[].handler-class is required");
    }

    static final class TestStreamKey implements StreamKey<String> {
        @Override
        public Class<String> payloadType() {
            return null; // no-op
        }

        @Override
        public String apply(String s) {
            return ""; // no-op
        }
    }

    static final class TestStreamHandler implements StreamHandler<String> {
        @Override
        public void handle(String event) {
            // no-op
        }
    }
}