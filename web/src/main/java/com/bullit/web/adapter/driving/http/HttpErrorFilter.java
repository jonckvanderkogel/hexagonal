package com.bullit.web.adapter.driving.http;

import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.PersistenceException;
import com.bullit.web.adapter.driving.http.Response.ErrorResponse;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.HandlerFilterFunction;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

public final class HttpErrorFilter implements HandlerFilterFunction<ServerResponse, ServerResponse> {

    private static final Logger log = LoggerFactory.getLogger(HttpErrorFilter.class);

    @Nonnull
    public ServerResponse filter(
            @Nonnull ServerRequest request,
            @Nonnull HandlerFunction<ServerResponse> next) {
        try {
            return next.handle(request);
        } catch (IllegalArgumentException e) {
            log.warn("400 Bad Request [{} {}]: {}", request.method(), request.uri(), e.getMessage());
            return ServerResponse
                    .status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("Invalid request: %s".formatted(e.getMessage())));
        } catch (NotFoundException e) {
            log.info("404 Not Found [{} {}]: {}", request.method(), request.uri(), e.getMessage());
            return ServerResponse
                    .status(HttpStatus.NOT_FOUND)
                    .body(new ErrorResponse("Invalid resource identifier: %s".formatted(e.getMessage())));
        } catch (PersistenceException e) {
            log.error("500 Persistence error [{} {}]", request.method(), request.uri(), e);
            return ServerResponse
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse(e.getMessage()));
        } catch (Exception e) {
            log.error("500 Unexpected error [{} {}]", request.method(), request.uri(), e);
            return ServerResponse
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("Unexpected error"));
        }
    }
}