package com.bullit.domain.port;

import com.bullit.domain.error.AppError;
import com.bullit.domain.model.Author;
import io.vavr.control.Either;

import java.util.UUID;

public interface AuthorService {
    Either<AppError, Author> create(String name);
    Either<AppError, Author> getById(UUID id);
}