package com.bullit.domain.model;

import com.bullit.domain.error.ValidationError;
import com.bullit.domain.error.ValidationError.AuthorValidationError;
import io.vavr.control.Either;
import io.vavr.control.Validation;

import java.util.UUID;

import static com.bullit.domain.util.ErrorUtils.collapseErrors;

public final class Author {
    private final UUID id;
    private final String name;

    private Author(UUID id, String name) {
        this.id = id;
        this.name = name;
    }

    public UUID id() { return id; }
    public String name() { return name; }

    public static Either<ValidationError, Author> createNew(String rawName) {
        return validateName(rawName, 100)
                .map(name -> new Author(UUID.randomUUID(), name))
                .toEither();
    }

    public static Either<ValidationError, Author> rehydrate(UUID id, String rawName) {
        return validateRehydration(id, rawName)
                .toEither();
    }

    private static Validation<ValidationError, Author> validateRehydration(UUID id, String rawName) {
        return Validation.combine(
                        validateId(id),
                        validateName(rawName, 100)
                )
                .ap(Author::new)
                .mapError( errs -> collapseErrors(errs, AuthorValidationError::new));
    }

    private static Validation<ValidationError, UUID> validateId(UUID id) {
        return (id != null)
                ? Validation.valid(id)
                : Validation.invalid(new AuthorValidationError("Id cannot be null"));
    }

    private static Validation<ValidationError, String> validateName(String raw, int maxLength) {
        return Validation.combine(
                        notBlank(raw, "Name is required"),
                        maxLength(raw, maxLength, "Name must be ≤ " + maxLength + " characters")
                )
                .ap((__1, __2) -> raw.trim())
                .mapError(errs -> collapseErrors(errs, AuthorValidationError::new));
    }

    private static Validation<ValidationError, String> notBlank(String value, String message) {
        return (value != null && !value.isBlank())
                ? Validation.valid(value)
                : Validation.invalid(new AuthorValidationError(message));
    }

    private static Validation<ValidationError, String> maxLength(String value, int max, String message) {
        return (value != null && value.trim().length() <= max)
                ? Validation.valid(value)
                : Validation.invalid(new AuthorValidationError(message));
    }
}