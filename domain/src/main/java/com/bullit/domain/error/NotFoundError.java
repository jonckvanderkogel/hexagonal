package com.bullit.domain.error;

public sealed interface NotFoundError extends AppError {
    record AuthorNotFoundError(String message) implements NotFoundError {}
}
