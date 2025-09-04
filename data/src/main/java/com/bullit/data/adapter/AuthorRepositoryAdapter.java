package com.bullit.data.adapter;

import com.bullit.data.persistence.AuthorEntity;
import com.bullit.domain.model.Author;
import com.bullit.domain.error.AppError;
import com.bullit.domain.error.NotFoundError.AuthorNotFoundError;
import com.bullit.domain.error.PersistenceError.AuthorPersistenceError;
import com.bullit.domain.port.AuthorRepositoryPort;
import com.bullit.data.persistence.AuthorDataMapper;
import com.bullit.data.persistence.AuthorJpaRepository;
import io.vavr.control.Either;
import io.vavr.control.Try;

import java.util.Optional;
import java.util.UUID;

import static com.bullit.domain.util.EitherUtils.widenLeft;
import static io.vavr.control.Either.left;

public class AuthorRepositoryAdapter implements AuthorRepositoryPort {

    private final AuthorJpaRepository repo;

    public AuthorRepositoryAdapter(AuthorJpaRepository repo) {
        this.repo = repo;
    }

    @Override
    public Either<AppError, Author> save(Author author) {
        return fromDb(Try.of(() -> repo.save(new AuthorEntity(author.id(), author.name()))))
                .flatMap(e -> widenLeft(AuthorDataMapper.toDomain(e)));
    }

    @Override
    public Either<AppError, Author> findById(UUID id) {
        return fromDb(Try.of(() -> repo.findById(id)))
                .flatMap(opt -> fromOptional(opt, id));
    }

    private <T> Either<AppError, T> fromDb(Try<T> t) {
        return t.toEither()
                .mapLeft(ex -> new AuthorPersistenceError(ex.getMessage()));
    }

    private Either<AppError, Author> fromOptional(Optional<AuthorEntity> opt, UUID id) {
        return opt.map(v -> widenLeft(AuthorDataMapper.toDomain(v)))
                .orElseGet(() -> left(new AuthorNotFoundError("Author %s not found".formatted(id))));
    }
}