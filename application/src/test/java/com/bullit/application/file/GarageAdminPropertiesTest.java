package com.bullit.application.file;

import org.junit.jupiter.api.Test;

import static com.bullit.application.TestUtils.anyMessageContains;
import static com.bullit.application.TestUtils.assertNoViolations;
import static com.bullit.application.TestUtils.validate;
import static org.assertj.core.api.Assertions.assertThat;

class GarageAdminPropertiesTest {

    @Test
    void validConfig_hasNoViolations_andBuildsHttpBaseUrlWhenInsecure() {
        var props = new GarageAdminProperties(
                "localhost",
                3903,
                false,
                "secret-token"
        );

        assertNoViolations(validate(props));
        assertThat(props.baseUrl()).isEqualTo("http://localhost:3903");
    }

    @Test
    void validConfig_buildsHttpsBaseUrlWhenSecure() {
        var props = new GarageAdminProperties(
                "garage.local",
                443,
                true,
                "secret-token"
        );

        assertNoViolations(validate(props));
        assertThat(props.baseUrl()).isEqualTo("https://garage.local:443");
    }

    @Test
    void host_mustBeNotBlank() {
        var props = new GarageAdminProperties(
                "   ",
                3903,
                false,
                "secret-token"
        );

        anyMessageContains(validate(props), "garage.admin.host is required");
    }

    @Test
    void token_mustBeNotBlank() {
        var props = new GarageAdminProperties(
                "localhost",
                3903,
                false,
                "   "
        );

        anyMessageContains(validate(props), "garage.admin.token is required");
    }

    @Test
    void port_mustBeAtLeast1() {
        var props = new GarageAdminProperties(
                "localhost",
                0,
                false,
                "secret-token"
        );

        anyMessageContains(validate(props), "garage.admin.port must be >= 1");
    }

    @Test
    void port_mustBeAtMost65535() {
        var props = new GarageAdminProperties(
                "localhost",
                65536,
                false,
                "secret-token"
        );

        anyMessageContains(validate(props), "garage.admin.port must be <= 65535");
    }

    @Test
    void canShowMultipleViolationsAtOnce() {
        var props = new GarageAdminProperties(
                "",
                0,
                true,
                ""
        );

        var violations = validate(props);

        anyMessageContains(violations, "garage.admin.host is required");
        anyMessageContains(violations, "garage.admin.port must be >= 1");
        anyMessageContains(violations, "garage.admin.token is required");
    }

    @Test
    void host_allowsHostnameSingleLabel_fqdn_ipv4_andBracketedIpv6() {
        assertNoViolations(validate(new GarageAdminProperties("garage", 3903, false, "t")));
        assertNoViolations(validate(new GarageAdminProperties("garage.local", 3903, false, "t")));
        assertNoViolations(validate(new GarageAdminProperties("127.0.0.1", 3903, false, "t")));
        assertNoViolations(validate(new GarageAdminProperties("[2001:db8::1]", 3903, false, "t")));
    }

    @Test
    void host_mustNotContainSchemePortOrPath() {
        anyMessageContains(
                validate(new GarageAdminProperties("http://localhost", 3903, false, "t")),
                "garage.admin.host must be a valid hostname or IP address"
        );

        anyMessageContains(
                validate(new GarageAdminProperties("localhost:3903", 3903, false, "t")),
                "garage.admin.host must be a valid hostname or IP address"
        );

        anyMessageContains(
                validate(new GarageAdminProperties("localhost/api", 3903, false, "t")),
                "garage.admin.host must be a valid hostname or IP address"
        );
    }

    @Test
    void host_mustRejectInvalidCharactersOrWhitespace() {
        anyMessageContains(
                validate(new GarageAdminProperties(" local host ", 3903, false, "t")),
                "garage.admin.host must be a valid hostname or IP address"
        );

        anyMessageContains(
                validate(new GarageAdminProperties("local_host", 3903, false, "t")),
                "garage.admin.host must be a valid hostname or IP address"
        );
    }
}