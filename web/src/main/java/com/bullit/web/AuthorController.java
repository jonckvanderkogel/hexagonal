package com.bullit.web;

import com.bullit.domain.model.Author;
import com.bullit.domain.error.AppError;
import com.bullit.domain.error.NotFoundError;
import com.bullit.domain.error.PersistenceError;
import com.bullit.domain.error.ValidationError;
import com.bullit.domain.port.AuthorService;
import io.vavr.control.Either;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/authors")
public final class AuthorController {

    private final AuthorService authorService;

    public AuthorController(AuthorService authorService) {
        this.authorService = authorService;
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody CreateAuthorRequest body) {
        Either<AppError, Author> result = authorService.create(body.name());
        return result.fold(this::toError, this::toOkOrCreated);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable("id") UUID id) {
        Either<AppError, Author> result = authorService.getById(id);
        return result.fold(
                this::toError,
                a -> ResponseEntity.ok(
                        new AuthorResponse(
                                a.id().toString(),
                                a.name()
                        )
                )
        );
    }

    private ResponseEntity<ErrorResponse> toError(AppError error) {
        return switch (error) {
            case ValidationError e ->
                    ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse(e.message()));
            case NotFoundError e -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse(e.message()));
            case PersistenceError e ->
                    ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ErrorResponse(e.message()));
        };
    }

    private ResponseEntity<AuthorResponse> toOkOrCreated(Author author) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new AuthorResponse(author.id().toString(), author.name()));
    }
}