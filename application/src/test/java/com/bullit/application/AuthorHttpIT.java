package com.bullit.application;

import com.bullit.web.adapter.driving.http.Response.AuthorResponse;
import com.bullit.web.adapter.driving.http.Response.BookResponse;
import com.bullit.web.adapter.driving.http.Response.ErrorResponse;
import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestExecutionListeners;

import java.net.URI;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@TestExecutionListeners(
        value = {DbUnitTestExecutionListener.class},
        mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
@DbUnitConfiguration
@DatabaseSetup("/dbunit/authorHttpDataset.xml")
@Import(AuthorHttpIT.TestConfig.class)
class AuthorHttpIT extends AbstractIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String base(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void createAuthor_then_get_by_id() {
        var createReq = Map.of("firstName", "Douglas", "lastName", "Adams");
        ResponseEntity<AuthorResponse> created = rest.postForEntity(base("/authors"), createReq, AuthorResponse.class);

        assertSoftly(s -> {
            s.assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            s.assertThat(created.getBody()).isNotNull();
            var id = created.getBody().id();
            s.assertThat(created.getBody().firstName()).isEqualTo("Douglas");
            s.assertThat(created.getBody().lastName()).isEqualTo("Adams");

            ResponseEntity<AuthorResponse> got = rest.getForEntity(base("/authors/{id}"), AuthorResponse.class, id);
            s.assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
            s.assertThat(got.getBody()).isNotNull();

            s.assertThat(got.getBody().id()).isEqualTo(id);
            s.assertThat(got.getBody().firstName()).isEqualTo("Douglas");
            s.assertThat(got.getBody().lastName()).isEqualTo("Adams");
            s.assertThat(got.getBody().books()).isEmpty();
            s.assertThat(got.getBody().insertedAt())
                    .isEqualTo(
                            LocalDateTime.of(2024, 8, 13, 9, 0, 0)
                                    .toInstant(ZoneOffset.UTC)
                    );
        });
    }

    @Test
    void get_preloaded_author_from_dbunit_dataset() {
        var existingId = UUID.fromString("22222222-2222-2222-2222-222222222222");

        ResponseEntity<AuthorResponse> got = rest.getForEntity(base("/authors/{id}"), AuthorResponse.class, existingId);

        assertSoftly(s -> {
            s.assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
            s.assertThat(got.getBody()).isNotNull();
            s.assertThat(got.getBody().id()).isEqualTo(existingId.toString());
            s.assertThat(got.getBody().firstName()).isEqualTo("Preloaded");
            s.assertThat(got.getBody().lastName()).isEqualTo("ViaDBUnit");
            s.assertThat(got.getBody().books()).extracting(BookResponse::title).contains("Preloaded Book");
            s.assertThat(got.getBody().insertedAt())
                    .isEqualTo(
                            LocalDateTime.of(2024, 1, 1, 0, 0, 0)
                                    .toInstant(ZoneOffset.UTC)
                    );
        });
    }

    @Test
    void get_unknown_author_returns_404() {
        var missing = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

        ResponseEntity<ErrorResponse> res = rest.getForEntity(base("/authors/{id}"), ErrorResponse.class, missing);

        assertSoftly(s -> {
            s.assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
            s.assertThat(res.getBody()).isNotNull();
            s.assertThat(res.getBody().error()).isNotBlank();
        });
    }

    @Test
    void createAuthor_with_empty_names_returns_400() {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        var request = new HttpEntity<>(Map.of("firstName", "", "lastName", ""), headers);

        ResponseEntity<ErrorResponse> res =
                rest.postForEntity(URI.create(base("/authors")), request, ErrorResponse.class);

        assertSoftly(s -> {
            s.assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            s.assertThat(res.getBody()).isNotNull();
            s.assertThat(res.getBody().error()).isNotBlank();
        });
    }

    @Test
    void addBookToAuthor_then_get_by_id() {
        var authorId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        var title = "Hitchhiker's Guide to the Galaxy";

        var createReq = Map.of("title", title);
        ResponseEntity<BookResponse> created = rest.postForEntity(base("/authors/{id}/books"), createReq, BookResponse.class, authorId);

        assertSoftly(s -> {
            s.assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            s.assertThat(created.getBody()).isNotNull();
            var id = created.getBody().id();
            s.assertThat(id).isNotEmpty();
            s.assertThat(created.getBody().title()).isEqualTo(title);
            s.assertThat(created.getBody().authorId()).isEqualTo(authorId.toString());
            s.assertThat(created.getBody().insertedAt())
                    .isEqualTo(
                            LocalDateTime.of(2024, 8, 13, 9, 0, 0)
                                    .toInstant(ZoneOffset.UTC)
                    );

            ResponseEntity<AuthorResponse> gotAuthor = rest.getForEntity(base("/authors/{id}"), AuthorResponse.class, authorId);
            s.assertThat(gotAuthor.getStatusCode()).isEqualTo(HttpStatus.OK);
            s.assertThat(gotAuthor.getBody()).isNotNull();
            s.assertThat(gotAuthor.getBody().books().size()).isEqualTo(2);
        });
    }

    @Test
    void addBookToNonExistentAuthor_returns404() {
        var nonExistentAuthorId = UUID.fromString("44444444-4444-4444-4444-444444444444");
        var title = "Hitchhiker's Guide to the Galaxy";

        var createReq = Map.of("title", title);

        ResponseEntity<ErrorResponse> created = rest.postForEntity(base("/authors/{id}/books"), createReq, ErrorResponse.class, nonExistentAuthorId);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        public Clock clock() {
            return Clock
                    .fixed(
                            LocalDateTime.of(2024, 8, 13, 9, 0, 0)
                                    .toInstant(ZoneOffset.UTC),
                            ZoneOffset.UTC
                    );
        }
    }
}