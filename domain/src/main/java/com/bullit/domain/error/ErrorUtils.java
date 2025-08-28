package com.bullit.domain.error;

import io.vavr.collection.Seq;

import java.util.function.Function;

public final class ErrorUtils {
    private ErrorUtils() {}

    public static <E extends AppError> E collapseErrors(Seq<? extends AppError> errors,
                                                        Function<String, E> collapseFun) {
        return collapseFun
                .apply(
                        errors
                                .map(AppError::message)
                                .mkString("; ")
                );
    }
}
