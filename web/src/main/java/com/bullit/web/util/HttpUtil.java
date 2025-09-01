package com.bullit.web.util;

import com.bullit.domain.error.AppError;
import com.bullit.domain.error.ValidationError;
import io.vavr.control.Either;
import io.vavr.control.Try;
import org.springframework.web.servlet.function.ServerRequest;

public class HttpUtil {
    private HttpUtil() {
    }

    public static <T> Either<AppError, T> parseRequestBody(ServerRequest request, Class<T> clazz) {
        return Try.of(() -> request.body(clazz))
                .toEither()
                .mapLeft(ex -> new ValidationError.MalformedRequestError("Invalid request body"));
    }
}
