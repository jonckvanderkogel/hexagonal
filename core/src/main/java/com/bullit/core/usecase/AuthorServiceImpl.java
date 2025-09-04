package com.bullit.core.usecase;

import com.bullit.domain.model.Author;
import com.bullit.domain.error.AppError;
import com.bullit.domain.port.AuthorRepositoryPort;
import com.bullit.domain.port.AuthorServicePort;
import io.vavr.control.Either;
import java.util.UUID;

import static com.bullit.domain.util.EitherUtils.widenLeft;

public final class AuthorServiceImpl implements AuthorServicePort {

    private final AuthorRepositoryPort repo;

    public AuthorServiceImpl(AuthorRepositoryPort repo) {
        this.repo = repo;
    }

    @Override
    public Either<AppError, Author> create(String name) {
        return widenLeft(Author.createNew(name))
                .flatMap(repo::save);
    }

    @Override
    public Either<AppError, Author> getById(UUID id) {
        return repo.findById(id);
    }
}