package com.bullit.web;

import com.bullit.domain.error.NotFoundError;
import com.bullit.domain.error.PersistenceError;
import com.bullit.domain.error.ValidationError;
import com.bullit.domain.model.Author;
import com.bullit.domain.port.AuthorService;
import io.vavr.control.Either;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


final class AuthorHttpHandlerTest {

    private final AuthorService authorService = mock(AuthorService.class);
    private AuthorHttpHandler handler;
    private List<HttpMessageConverter<?>> converters;

    @BeforeEach
    void setUp() {
        handler = new AuthorHttpHandler(authorService);
        converters = List.of(new MappingJackson2HttpMessageConverter());
    }

    @Test
    void create_returns201_with_payload() throws Exception {
        var id = UUID.randomUUID();
        when(authorService.create("Douglas Adams"))
                .thenReturn(Either.right(Author.rehydrate(id, "Douglas Adams")));

        var req = postJson("/authors", "{\"name\":\"Douglas Adams\"}");
        var res = handler.create(req);

        var body = writeToString(res);
        assertThat(status(res)).isEqualTo(201);
        assertThat(body).contains("\"id\":\"" + id + "\"");
        assertThat(body).contains("\"name\":\"Douglas Adams\"");
    }

    @Test
    void create_validationError_returns400() throws Exception {
        when(authorService.create(""))
                .thenReturn(Either.left(new ValidationError.AuthorValidationError("Name is required")));

        var req = postJson("/authors", "{\"name\":\"\"}");
        var res = handler.create(req);

        var body = writeToString(res);
        assertThat(status(res)).isEqualTo(400);
        assertThat(body).contains("\"error\":\"Name is required\"");
    }

    @Test
    void create_malformedJson_returns400_and_does_not_call_service() throws Exception {
        var req = postJson("/authors", "{\"name\":"); // malformed JSON
        var res = handler.create(req);

        var body = writeToString(res);
        assertThat(status(res)).isEqualTo(400);
        assertThat(body).contains("\"error\":\"Invalid request body\"");
        verifyNoInteractions(authorService);
    }

    @Test
    void getById_ok_returns200() throws Exception {
        var id = UUID.randomUUID();
        when(authorService.getById(id))
                .thenReturn(Either.right(Author.rehydrate(id, "Arthur Dent")));

        var req = getRequestWithId(id);
        var res = handler.getById(req);

        var body = writeToString(res);
        assertThat(status(res)).isEqualTo(200);
        assertThat(body).contains("\"id\":\"" + id + "\"");
        assertThat(body).contains("\"name\":\"Arthur Dent\"");
    }

    @Test
    void getById_notFound_returns404() throws Exception {
        var id = UUID.randomUUID();
        when(authorService.getById(id))
                .thenReturn(Either.left(new NotFoundError.AuthorNotFoundError("not found")));

        var req = getRequestWithId(id);
        var res = handler.getById(req);

        var body = writeToString(res);
        assertThat(status(res)).isEqualTo(404);
        assertThat(body).contains("\"error\":\"not found\"");
    }

    @Test
    void getById_persistenceError_returns500() throws Exception {
        var id = UUID.randomUUID();
        when(authorService.getById(id))
                .thenReturn(Either.left(new PersistenceError.AuthorPersistenceError("db down")));

        var req = getRequestWithId(id);
        var res = handler.getById(req);

        var body = writeToString(res);
        assertThat(status(res)).isEqualTo(500);
        assertThat(body).contains("\"error\":\"db down\"");
    }

    private ServerRequest postJson(String path, String json) {
        var servlet = new MockHttpServletRequest("POST", path);
        servlet.setContentType(MediaType.APPLICATION_JSON_VALUE);
        servlet.setContent(json.getBytes(StandardCharsets.UTF_8));
        return ServerRequest.create(servlet, converters);
    }

    private ServerRequest getRequestWithId(UUID id) {
        var servlet = new MockHttpServletRequest("GET", "/authors/" + id);
        servlet.setAttribute(
                RouterFunctions.URI_TEMPLATE_VARIABLES_ATTRIBUTE,
                Map.of("id", id.toString())
        );
        return ServerRequest.create(servlet, converters);
    }

    private int status(ServerResponse response) {
        return response.statusCode().value();
    }

    private String writeToString(ServerResponse response) throws Exception {
        var servletReq = new MockHttpServletRequest();
        var servletRes = new MockHttpServletResponse();

        ServerResponse.Context context = () -> converters;

        response.writeTo(servletReq, servletRes, context);
        return servletRes.getContentAsString();
    }
}