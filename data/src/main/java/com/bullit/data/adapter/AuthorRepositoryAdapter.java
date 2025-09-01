package com.bullit.data.adapter;

import com.bullit.domain.model.Author;
import com.bullit.domain.error.AppError;
import com.bullit.domain.error.NotFoundError.AuthorNotFoundError;
import com.bullit.domain.error.PersistenceError;
import com.bullit.domain.error.PersistenceError.AuthorPersistenceError;
import com.bullit.domain.port.AuthorRepositoryPort;
import com.bullit.data.persistence.AuthorDataMapper;
import com.bullit.data.persistence.AuthorJpaRepository;
import io.vavr.control.Either;
import io.vavr.control.Try;

import java.util.UUID;

public class AuthorRepositoryAdapter implements AuthorRepositoryPort {

    private final AuthorJpaRepository repo;

    public AuthorRepositoryAdapter(AuthorJpaRepository repo) {
        this.repo = repo;
    }

    @Override
    public Either<PersistenceError, Author> save(Author author) {
        return Try.of(() -> repo.save(AuthorDataMapper.toEntity(author)))
                .toEither()
                .map(AuthorDataMapper::toDomain)
                .mapLeft(ex -> new AuthorPersistenceError(ex.getMessage()));
    }

    @Override
    public Either<AppError, Author> findById(UUID id) {
        return Try.of(() -> repo.findById(id))
                .toEither()
                .mapLeft(ex -> (AppError) new AuthorPersistenceError(ex.getMessage()))
                .flatMap(opt -> opt
                        .<Either<AppError, Author>>map(e -> Either.right(AuthorDataMapper.toDomain(e)))
                        .orElseGet(() -> Either.left(new AuthorNotFoundError("Author %s not found".formatted(id))))
                );
    }
}