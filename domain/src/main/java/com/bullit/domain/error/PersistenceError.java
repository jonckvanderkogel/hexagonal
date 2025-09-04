package com.bullit.domain.error;

public sealed interface PersistenceError extends AppError {
    record AuthorPersistenceError(String message) implements PersistenceError {}
    record LogPersistenceError(String message) implements PersistenceError {}
}
