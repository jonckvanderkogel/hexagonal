package com.bullit.application;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@DirtiesContext
@TestExecutionListeners(
        value = { DbUnitTestExecutionListener.class },
        mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
@DbUnitConfiguration
@DatabaseSetup("/dbunit/authorHttpDataset.xml")
class AuthorHttpIT extends AbstractIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String base(String path) {
        return "http://localhost:" + port + path;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class AuthorJson {
        public String id;
        public String name;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class ErrorJson {
        public String error;
    }

    @Test
    void create_then_get_by_id() {
        // Create
        var createReq = Map.of("name", "Douglas Adams");
        ResponseEntity<AuthorJson> created = rest.postForEntity(base("/authors"), createReq, AuthorJson.class);

        Assertions.assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        Assertions.assertThat(created.getBody()).isNotNull();
        var id = created.getBody().id;
        Assertions.assertThat(created.getBody().name).isEqualTo("Douglas Adams");

        // Get
        ResponseEntity<AuthorJson> got = rest.getForEntity(base("/authors/{id}"), AuthorJson.class, id);
        Assertions.assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
        Assertions.assertThat(got.getBody()).isNotNull();
        Assertions.assertThat(got.getBody().id).isEqualTo(id);
        Assertions.assertThat(got.getBody().name).isEqualTo("Douglas Adams");
    }

    @Test
    void get_preloaded_author_from_dbunit_dataset() {
        var existingId = UUID.fromString("22222222-2222-2222-2222-222222222222");

        ResponseEntity<AuthorJson> got = rest.getForEntity(base("/authors/{id}"), AuthorJson.class, existingId);
        Assertions.assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
        Assertions.assertThat(got.getBody()).isNotNull();
        Assertions.assertThat(got.getBody().id).isEqualTo(existingId.toString());
        Assertions.assertThat(got.getBody().name).isEqualTo("Preloaded Via DBUnit");
    }

    @Test
    void get_unknown_author_returns_404() {
        var missing = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

        ResponseEntity<ErrorJson> res = rest.getForEntity(base("/authors/{id}"), ErrorJson.class, missing);
        Assertions.assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        Assertions.assertThat(res.getBody()).isNotNull();
        Assertions.assertThat(res.getBody().error).isNotBlank();
    }

    @Test
    void create_with_empty_name_returns_400() {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        var request = new HttpEntity<>(Map.of("name", ""), headers);

        ResponseEntity<ErrorJson> res =
                rest.postForEntity(URI.create(base("/authors")), request, ErrorJson.class);

        Assertions.assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        Assertions.assertThat(res.getBody()).isNotNull();
        Assertions.assertThat(res.getBody().error).isNotBlank();
    }
}