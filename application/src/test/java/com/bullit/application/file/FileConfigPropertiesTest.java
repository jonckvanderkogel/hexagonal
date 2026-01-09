package com.bullit.application.file;

import com.bullit.domain.port.driven.file.FileEnvelope;
import com.bullit.domain.port.driving.file.FileHandler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static com.bullit.application.TestUtils.anyMessageContains;
import static com.bullit.application.TestUtils.assertNoViolations;
import static com.bullit.application.TestUtils.validate;
import static org.assertj.core.api.Assertions.assertThat;

class FileConfigPropertiesTest {

    @Test
    void inputConfig_trimsBucket_andCanonicalizesNullPrefixesToEmpty() {
        var cfg = new FileConfigProperties.InputConfig(
                Object.class,
                "  my-bucket  ",
                null,
                null,
                null,
                Duration.ofSeconds(1)
        );

        assertThat(cfg.bucket()).isEqualTo("my-bucket");
        assertThat(cfg.incomingPrefix()).isEqualTo("");
        assertThat(cfg.handledPrefix()).isEqualTo("");
        assertThat(cfg.errorPrefix()).isEqualTo("");

        assertNoViolations(validate(cfg));
    }

    @Test
    void inputConfig_requiresPayloadType_bucket_andPollInterval() {
        var cfg = new FileConfigProperties.InputConfig(
                null,
                null,
                null,
                null,
                null,
                null
        );

        var violations = validate(cfg);

        anyMessageContains(violations, "files.inputs[].payload-type is required");
        anyMessageContains(violations, "files.inputs[].bucket is required");
        anyMessageContains(violations, "files.inputs[].poll-interval is required");
    }

    @Test
    void inputConfig_pollInterval_mustBePositive() {
        var zero = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "",
                "",
                "",
                Duration.ZERO
        );

        var negative = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "",
                "",
                "",
                Duration.ofSeconds(-1)
        );

        anyMessageContains(validate(zero), "files.inputs[].poll-interval must be positive");
        anyMessageContains(validate(negative), "files.inputs[].poll-interval must be positive");
    }

    @Test
    void inputConfig_prefixes_whenIncomingSet_thenHandledAndErrorMustBeSet() {
        var cfg = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "incoming/",
                "",
                "",
                Duration.ofSeconds(1)
        );

        anyMessageContains(validate(cfg), "files.inputs[] prefixes are invalid:");
    }

    @Test
    void inputConfig_prefixes_whenIncomingEmpty_thenHandledAndErrorMustBeEmpty() {
        var cfg1 = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "",
                "handled/",
                "",
                Duration.ofSeconds(1)
        );

        var cfg2 = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "",
                "",
                "error/",
                Duration.ofSeconds(1)
        );

        anyMessageContains(validate(cfg1), "files.inputs[] prefixes are invalid:");
        anyMessageContains(validate(cfg2), "files.inputs[] prefixes are invalid:");
    }

    @Test
    void inputConfig_prefixes_mustDiffer() {
        var incomingEqualsHandled = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "incoming/",
                "incoming/",
                "error/",
                Duration.ofSeconds(1)
        );

        var incomingEqualsError = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "incoming/",
                "handled/",
                "incoming/",
                Duration.ofSeconds(1)
        );

        var handledEqualsError = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "incoming/",
                "same/",
                "same/",
                Duration.ofSeconds(1)
        );

        anyMessageContains(validate(incomingEqualsHandled), "files.inputs[] prefixes are invalid:");
        anyMessageContains(validate(incomingEqualsError), "files.inputs[] prefixes are invalid:");
        anyMessageContains(validate(handledEqualsError), "files.inputs[] prefixes are invalid:");
    }

    @Test
    void inputConfig_prefixes_mustEndWithSlashWhenSet() {
        var cfg = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "incoming",   // missing trailing slash
                "handled/",   // ok
                "error/",     // ok
                Duration.ofSeconds(1)
        );

        anyMessageContains(validate(cfg), "files.inputs[] prefixes must be empty or end with '/'");
    }

    @Test
    void inputConfig_prefixes_mustNotStartWithSlash() {
        var cfg1 = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "/incoming/",
                "handled/",
                "error/",
                Duration.ofSeconds(1)
        );

        var cfg2 = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "incoming/",
                "/handled/",
                "error/",
                Duration.ofSeconds(1)
        );

        var cfg3 = new FileConfigProperties.InputConfig(
                Object.class,
                "bucket",
                "incoming/",
                "handled/",
                "/error/",
                Duration.ofSeconds(1)
        );

        anyMessageContains(validate(cfg1), "files.inputs[] prefixes must not start with '/'");
        anyMessageContains(validate(cfg2), "files.inputs[] prefixes must not start with '/'");
        anyMessageContains(validate(cfg3), "files.inputs[] prefixes must not start with '/'");
    }

    @Test
    void outputConfig_trimsBucket_andRequiresFields() {
        var valid = new FileConfigProperties.OutputConfig(
                Object.class,
                "  out-bucket  "
        );

        assertThat(valid.bucket()).isEqualTo("out-bucket");
        assertNoViolations(validate(valid));

        var invalid = new FileConfigProperties.OutputConfig(
                null,
                "   "
        );

        var violations = validate(invalid);
        anyMessageContains(violations, "files.outputs[].payload-type is required");
        anyMessageContains(violations, "files.outputs[].bucket is required");
    }

    @Test
    void handlerConfig_requiresHandlerClass() {
        var cfg = new FileConfigProperties.HandlerConfig(null);
        anyMessageContains(validate(cfg), "files.handlers[].handler-class is required");
    }

    @Test
    void rootConfig_validatesNestedElements() {
        var root = new FileConfigProperties(
                List.of(new FileConfigProperties.InputConfig(
                        null,
                        null,
                        "incoming/",
                        "",          // invalid because incoming set => handled+error required
                        "",
                        Duration.ZERO // invalid: must be positive
                )),
                List.of(new FileConfigProperties.OutputConfig(
                        null,
                        "   "
                )),
                List.of(new FileConfigProperties.HandlerConfig(
                        null
                ))
        );

        var violations = validate(root);

        anyMessageContains(violations, "files.inputs[].payload-type is required");
        anyMessageContains(violations, "files.inputs[].bucket is required");
        anyMessageContains(violations, "files.inputs[].poll-interval must be positive");
        anyMessageContains(violations, "files.inputs[] prefixes are invalid:");

        anyMessageContains(violations, "files.outputs[].payload-type is required");
        anyMessageContains(violations, "files.outputs[].bucket is required");

        anyMessageContains(violations, "files.handlers[].handler-class is required");
    }

    @Test
    void rootConfig_allowsNullLists_andProvidesOrEmptyHelpers() {
        var root = new FileConfigProperties(null, null, null);

        assertThat(root.inputsOrEmpty()).isEmpty();
        assertThat(root.outputsOrEmpty()).isEmpty();
        assertThat(root.handlersOrEmpty()).isEmpty();

        assertNoViolations(validate(root));
    }

    @SuppressWarnings("unused")
    private static final class DummyHandler implements FileHandler<Object> {
        @Override
        public void handle(FileEnvelope<Object> file) {
            // no-op
        }
    }
}