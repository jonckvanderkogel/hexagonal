package com.bullit.web;

import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.PersistenceException;
import com.bullit.web.Response.ErrorResponse;
import jakarta.annotation.Nonnull;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.HandlerFilterFunction;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

public final class HttpErrorFilter implements HandlerFilterFunction<ServerResponse, ServerResponse> {

    @Nonnull
    public ServerResponse filter(
            @Nonnull ServerRequest request,
            @Nonnull HandlerFunction<ServerResponse> next) throws Exception {
        try {
            return next.handle(request);
        } catch (IllegalArgumentException e) {
            return ServerResponse
                    .status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("Invalid request: %s".formatted(e.getMessage())));
        } catch (NotFoundException e) {
            return ServerResponse
                    .status(HttpStatus.NOT_FOUND)
                    .body(new ErrorResponse("Invalid resource identifier: %s".formatted(e.getMessage())));
        } catch (PersistenceException e) {
            return ServerResponse
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse(e.getMessage()));
        } catch (Exception e) {
            return ServerResponse
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("Unexpected error"));
        }
    }
}