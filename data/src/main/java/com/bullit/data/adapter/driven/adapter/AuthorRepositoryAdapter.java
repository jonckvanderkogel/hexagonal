package com.bullit.data.adapter.driven.adapter;

import com.bullit.data.adapter.driven.jpa.AuthorEntity;
import com.bullit.data.adapter.driven.jpa.AuthorJpaRepository;
import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.library.Author;
import com.bullit.domain.port.driven.AuthorRepositoryPort;
import org.springframework.dao.DataAccessException;

import java.util.UUID;
import java.util.function.Function;

public class AuthorRepositoryAdapter implements AuthorRepositoryPort {
    private final AuthorJpaRepository repo;

    public AuthorRepositoryAdapter(AuthorJpaRepository repo) {
        this.repo = repo;
    }

    private <T> Author dbInteraction(Function<T, AuthorEntity> fun, T arg, String errorMessage) {
        try {
            var applied = fun.apply(arg);
            return AuthorEntity.toDomain(applied);
        } catch (DataAccessException ex) {
            throw new DatabaseInteractionException(errorMessage, ex);
        }
    }

    @Override
    public Author save(Author author) {
        return dbInteraction(
                a -> repo.save(AuthorEntity.toEntity(a)),
                author,
                "DB error during save of author"
        );
    }

    @Override
    public Author findById(UUID id) {
        return dbInteraction(
                i -> repo.findById(i)
                        .orElseThrow(() -> new NotFoundException(
                                "Author %s not found".formatted(id))),
                id,
                "DB error during findById"
        );
    }

    @Override
    public Author findByBookId(UUID bookId) {
        return dbInteraction(
                e -> repo.findByBooksId(e)
                        .orElseThrow(() -> new NotFoundException(
                                "Author for book %s not found".formatted(bookId))),
                bookId,
                "DB error during findByBookId"
        );
    }
}