package com.bullit.domain.error;

public sealed interface ValidationError extends AppError {
    record AuthorValidationError(String message) implements ValidationError {}
    record MalformedRequestError(String message) implements ValidationError {}
}
