package com.bullit.application.file;

import org.junit.jupiter.api.Test;

import static com.bullit.application.TestUtils.anyMessageContains;
import static com.bullit.application.TestUtils.assertNoViolations;
import static com.bullit.application.TestUtils.validate;

class S3ClientPropertiesTest {

    @Test
    void validConfig_hasNoViolations() {
        assertNoViolations(validate(new S3ClientProperties(
                "localhost",
                3900,
                false,
                "access",
                "secret",
                "garage"
        )));
    }

    @Test
    void endpoint_isRequired() {
        anyMessageContains(
                validate(new S3ClientProperties(
                        "",
                        3900,
                        false,
                        "access",
                        "secret",
                        "garage"
                )),
                "s3.endpoint is required"
        );
    }

    @Test
    void endpoint_mustBeHostname_withoutSchemeOrPort() {
        anyMessageContains(
                validate(new S3ClientProperties(
                        "http://localhost",
                        3900,
                        false,
                        "access",
                        "secret",
                        "garage"
                )),
                "s3.endpoint must be a valid hostname or IP address (no scheme, no port, no path)"
        );

        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost:3900",
                        3900,
                        false,
                        "access",
                        "secret",
                        "garage"
                )),
                "s3.endpoint must be a valid hostname or IP address (no scheme, no port, no path)"
        );

        anyMessageContains(
                validate(new S3ClientProperties(
                        "local host",
                        3900,
                        false,
                        "access",
                        "secret",
                        "garage"
                )),
                "s3.endpoint must be a valid hostname or IP address (no scheme, no port, no path)"
        );

        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost/api",
                        3900,
                        false,
                        "access",
                        "secret",
                        "garage"
                )),
                "s3.endpoint must be a valid hostname or IP address (no scheme, no port, no path)"
        );
    }

    @Test
    void port_mustBeWithinRange() {
        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost",
                        0,
                        false,
                        "access",
                        "secret",
                        "garage"
                )),
                "s3.port must be >= 1"
        );

        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost",
                        65536,
                        false,
                        "access",
                        "secret",
                        "garage"
                )),
                "s3.port must be <= 65535"
        );
    }

    @Test
    void region_isRequired() {
        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost",
                        3900,
                        false,
                        "access",
                        "secret",
                        ""
                )),
                "s3.region is required"
        );
    }

    @Test
    void credentials_mustEitherBothBeSet_orBothBeEmpty() {
        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost",
                        3900,
                        false,
                        "access",
                        null,
                        "garage"
                )),
                "s3.access-key and s3.secret-key must either both be set or both be empty"
        );

        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost",
                        3900,
                        false,
                        null,
                        "secret",
                        "garage"
                )),
                "s3.access-key and s3.secret-key must either both be set or both be empty"
        );

        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost",
                        3900,
                        false,
                        "   ",
                        "secret",
                        "garage"
                )),
                "s3.access-key and s3.secret-key must either both be set or both be empty"
        );

        anyMessageContains(
                validate(new S3ClientProperties(
                        "localhost",
                        3900,
                        false,
                        "access",
                        "   ",
                        "garage"
                )),
                "s3.access-key and s3.secret-key must either both be set or both be empty"
        );
    }

    @Test
    void withCredentials_returnsNewInstanceWithSameBaseConfig_andNewKeys() {
        var base = new S3ClientProperties(
                "localhost",
                3900,
                true,
                null,
                null,
                "garage"
        );

        var updated = base.withCredentials("ak", "sk");

        assertNoViolations(validate(updated));

        org.assertj.core.api.SoftAssertions.assertSoftly(s -> {
            s.assertThat(updated.endpoint()).isEqualTo("localhost");
            s.assertThat(updated.port()).isEqualTo(3900);
            s.assertThat(updated.secure()).isTrue();
            s.assertThat(updated.region()).isEqualTo("garage");
            s.assertThat(updated.accessKey()).isEqualTo("ak");
            s.assertThat(updated.secretKey()).isEqualTo("sk");

            s.assertThat(base.accessKey()).isNull();
            s.assertThat(base.secretKey()).isNull();
        });
    }

    @Test
    void credentials_canBeBothEmpty() {
        assertNoViolations(validate(new S3ClientProperties(
                "localhost",
                3900,
                false,
                null,
                null,
                "garage"
        )));

        assertNoViolations(validate(new S3ClientProperties(
                "localhost",
                3900,
                false,
                "",
                "",
                "garage"
        )));

        assertNoViolations(validate(new S3ClientProperties(
                "localhost",
                3900,
                false,
                "   ",
                "   ",
                "garage"
        )));
    }
}