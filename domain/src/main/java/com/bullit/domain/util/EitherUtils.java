package com.bullit.domain.util;

import com.bullit.domain.error.AppError;
import io.vavr.control.Either;

public class EitherUtils {
    private EitherUtils() {}
    public static <L extends AppError, R> Either<AppError, R> widenLeft(Either<L, R> e) {
        return e.mapLeft(l -> (AppError) l);
    }
}
