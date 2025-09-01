package com.bullit.web;

import com.bullit.domain.error.AppError;
import com.bullit.domain.error.NotFoundError;
import com.bullit.domain.error.PersistenceError;
import com.bullit.domain.error.ValidationError;
import com.bullit.domain.model.Author;
import com.bullit.domain.port.AuthorService;
import io.vavr.control.Either;
import io.vavr.control.Try;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.util.UUID;

import static com.bullit.web.util.HttpUtil.parseRequestBody;

public final class AuthorHttpHandler {

    private final AuthorService authorService;

    public AuthorHttpHandler(AuthorService authorService) {
        this.authorService = authorService;
    }

    public ServerResponse create(ServerRequest request) {
        return parseRequestBody(request, CreateAuthorRequest.class)
                .flatMap(body -> authorService.create(body.name()))
                .fold(this::toError, this::toCreated);
    }

    public ServerResponse getById(ServerRequest req) {
        return parseUuid(req.pathVariable("id"))
                .flatMap(authorService::getById)
                .fold(this::toError, this::toOk);
    }

    private Either<AppError, UUID> parseUuid(String raw) {
        return Try.of(() -> UUID.fromString(raw))
                .toEither()
                .mapLeft(_ -> new ValidationError.MalformedRequestError("Invalid UUID: " + raw));
    }

    private ServerResponse toCreated(Author a) {
        return ServerResponse.status(HttpStatus.CREATED)
                .body(new AuthorResponse(a.id().toString(), a.name()));
    }

    private ServerResponse toOk(Author a) {
        return ServerResponse.ok()
                .body(new AuthorResponse(a.id().toString(), a.name()));
    }

    private ServerResponse toError(AppError error) {
        return switch (error) {
            case ValidationError e ->
                    ServerResponse.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse(e.message()));
            case NotFoundError e ->
                    ServerResponse.status(HttpStatus.NOT_FOUND).body(new ErrorResponse(e.message()));
            case PersistenceError e ->
                    ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ErrorResponse(e.message()));
        };
    }
}