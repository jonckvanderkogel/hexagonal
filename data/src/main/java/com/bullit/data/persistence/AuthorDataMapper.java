package com.bullit.data.persistence;

import com.bullit.domain.error.ValidationError;
import com.bullit.domain.model.Author;
import io.vavr.control.Either;

public final class AuthorDataMapper {
    private AuthorDataMapper() {}

    public static Either<ValidationError, Author> toDomain(AuthorEntity e) {
        return Author.rehydrate(e.getId(), e.getName());
    }
}