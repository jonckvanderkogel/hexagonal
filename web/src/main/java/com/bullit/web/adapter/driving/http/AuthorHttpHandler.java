package com.bullit.web.adapter.driving.http;

import com.bullit.domain.port.driving.LibraryServicePort;
import com.bullit.web.adapter.driving.http.Request.AddBookRequest;
import com.bullit.web.adapter.driving.http.Request.CreateAuthorRequest;
import com.bullit.web.adapter.driving.http.Response.AuthorResponse;
import com.bullit.web.adapter.driving.http.Response.BookResponse;
import jakarta.servlet.ServletException;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.io.IOException;

import static com.bullit.web.adapter.driving.http.util.HttpUtil.parseAndValidateBody;
import static com.bullit.web.adapter.driving.http.util.HttpUtil.parseUuid;

public final class AuthorHttpHandler {

    private final LibraryServicePort service;

    public AuthorHttpHandler(LibraryServicePort service) {
        this.service = service;
    }

    public ServerResponse createAuthor(ServerRequest request) throws ServletException, IOException {
        var dto = parseAndValidateBody(request, CreateAuthorRequest.class);
        var created = service.createAuthor(dto.firstName().trim(), dto.lastName().trim());
        return ServerResponse
                .status(HttpStatus.CREATED)
                .body(AuthorResponse.fromDomain(created));
    }

    public ServerResponse addBookToAuthor(ServerRequest request) throws ServletException, IOException {
        var id = parseUuid(request.pathVariable("id"));
        var dto = parseAndValidateBody(request, AddBookRequest.class);
        var created = service.addBook(id, dto.title().trim());
        return ServerResponse
                .status(HttpStatus.CREATED)
                .body(BookResponse.fromDomain(created));
    }

    public ServerResponse getAuthorById(ServerRequest request) {
        var id = parseUuid(request.pathVariable("id"));
        var author = service.getById(id);
        return ServerResponse
                .ok()
                .body(AuthorResponse.fromDomain(author));
    }
}