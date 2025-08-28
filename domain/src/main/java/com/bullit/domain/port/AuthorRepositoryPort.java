package com.bullit.domain.port;

import com.bullit.domain.error.AppError;
import com.bullit.domain.error.PersistenceError;
import com.bullit.domain.model.Author;
import io.vavr.control.Either;

import java.util.UUID;

public interface AuthorRepositoryPort {
    Either<PersistenceError, Author> save(Author author);
    Either<AppError, Author> findById(UUID id);
}