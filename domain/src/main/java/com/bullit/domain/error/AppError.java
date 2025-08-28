package com.bullit.domain.error;

public sealed interface AppError
        permits NotFoundError, PersistenceError, ValidationError {
    String message();
}
