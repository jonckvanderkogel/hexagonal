package com.bullit.web;

import com.bullit.domain.model.Author;
import com.bullit.domain.error.NotFoundError;
import com.bullit.domain.error.PersistenceError;
import com.bullit.domain.error.ValidationError;
import com.bullit.domain.port.AuthorService;
import io.vavr.control.Either;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

final class AuthorControllerTest {

    private final AuthorService authorService = mock(AuthorService.class);
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        var controller = new AuthorController(authorService);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void create_returns201() throws Exception {
        var id = UUID.randomUUID();
        when(authorService.create("Douglas Adams"))
                .thenReturn(Either.right(Author.rehydrate(id, "Douglas Adams")));

        mvc.perform(post("/authors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Douglas Adams\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.name").value("Douglas Adams"));
    }

    @Test
    void create_validationError_returns400() throws Exception {
        when(authorService.create("")).thenReturn(Either.left(new ValidationError.AuthorValidationError("Name is required")));

        mvc.perform(post("/authors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Name is required"));
    }

    @Test
    void getById_ok_returns200() throws Exception {
        var id = UUID.randomUUID();
        when(authorService.getById(id))
                .thenReturn(Either.right(Author.rehydrate(id, "Arthur Dent")));

        mvc.perform(get("/authors/{id}", id.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.name").value("Arthur Dent"));
    }

    @Test
    void getById_notFound_returns404() throws Exception {
        var id = UUID.randomUUID();
        when(authorService.getById(id))
                .thenReturn(Either.left(new NotFoundError.AuthorNotFoundError("not found")));

        mvc.perform(get("/authors/{id}", id.toString()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("not found"));
    }

    @Test
    void getById_persistenceError_returns500() throws Exception {
        var id = UUID.randomUUID();
        when(authorService.getById(id))
                .thenReturn(Either.left(new PersistenceError.AuthorPersistenceError("db down")));

        mvc.perform(get("/authors/{id}", id.toString()))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("db down"));
    }
}