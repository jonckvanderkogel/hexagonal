package com.bullit.web.adapter.driving.http;

import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.model.library.Author;
import com.bullit.domain.port.driving.LibraryServicePort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.function.HandlerFilterFunction;
import org.springframework.web.servlet.function.ServerResponse;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


final class AuthorHttpHandlerTest extends AbstractHttpTest {
    private final LibraryServicePort service = mock(LibraryServicePort.class);

    private AuthorHttpHandler handler;
    private HandlerFilterFunction<ServerResponse, ServerResponse> errorFilter;

    @BeforeEach
    void setUp() {
        handler = new AuthorHttpHandler(service);
        errorFilter = new HttpErrorFilter();
    }

    @Test
    void create_returns201_with_payload() throws Exception {
        var id = UUID.randomUUID();
        when(service.createAuthor("Douglas", "Adams"))
                .thenReturn(Author.rehydrate(id, "Douglas", "Adams", emptyList(), Instant.parse("2024-01-01T00:00:00Z")));

        var req = postJson("/authors", """
                {"firstName":"Douglas","lastName":"Adams"}
                """);
        var res = errorFilter.filter(req, handler::createAuthor);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(201);
            s.assertThat(body).contains("\"id\":\"" + id + "\"");
            s.assertThat(body).contains("\"firstName\":\"Douglas\"");
            s.assertThat(body).contains("\"lastName\":\"Adams\"");
            s.assertThat(body).contains("\"insertedAt\":\"2024-01-01T00:00:00Z\"");
        });
    }

    @Test
    void create_validationError_returns400_via_filter() throws Exception {
        var req = postJson("/authors", """
                {"firstName":"","lastName":""}
                """);

        var res = errorFilter.filter(req, handler::createAuthor);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(400);
            s.assertThat(body).contains("\"error\":\"Invalid request:");
            s.check(() -> verifyNoInteractions(service));
        });
    }

    @Test
    void create_malformedJson_returns_500_unexpected_via_filter() throws Exception {
        var req = postJson("/authors", "{\"firstName\":"); // malformed JSON

        var res = errorFilter.filter(req, handler::createAuthor);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(500);
            s.assertThat(body).contains("\"error\":\"Unexpected error\"");
            s.check(() -> verifyNoInteractions(service));
        });
    }

    @Test
    void getById_ok_returns200() throws Exception {
        var id = UUID.randomUUID();
        when(service.getById(id))
                .thenReturn(Author.rehydrate(id, "Arthur", "Dent", emptyList(), Instant.parse("2024-01-01T00:00:00Z")));

        var req = getWithPathVars(
                "/authors/{id}",
                Map.of("id", id.toString())
        );

        var res = errorFilter.filter(req, handler::getAuthorById);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(200);
            s.assertThat(body).contains("\"id\":\"" + id + "\"");
            s.assertThat(body).contains("\"firstName\":\"Arthur\"");
            s.assertThat(body).contains("\"lastName\":\"Dent\"");
        });
    }

    @Test
    void getById_notFound_returns404_via_filter() throws Exception {
        var id = UUID.randomUUID();
        when(service.getById(id)).thenThrow(new NotFoundException("not found"));

        var req = getWithPathVars(
                "/authors/{id}",
                Map.of("id", id.toString())
        );

        var res = errorFilter.filter(req, handler::getAuthorById);

        var body = writeToString(res);

        assertSoftly(s -> {
            s.assertThat(status(res)).isEqualTo(404);
            s.assertThat(body).contains("\"error\":\"Invalid resource identifier: not found\"");
        });
    }
}