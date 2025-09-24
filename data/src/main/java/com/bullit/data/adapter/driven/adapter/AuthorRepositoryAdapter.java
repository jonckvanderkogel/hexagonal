package com.bullit.data.adapter.driven.adapter;

import com.bullit.data.adapter.driven.jpa.AuthorEntity;
import com.bullit.data.adapter.driven.jpa.AuthorJpaRepository;
import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.PersistenceException;
import com.bullit.domain.model.library.Author;
import com.bullit.domain.port.outbound.AuthorRepositoryPort;
import org.springframework.dao.DataAccessException;

import java.util.UUID;

public class AuthorRepositoryAdapter implements AuthorRepositoryPort {

    private final AuthorJpaRepository repo;

    public AuthorRepositoryAdapter(AuthorJpaRepository repo) {
        this.repo = repo;
    }

    @Override
    public Author save(Author author) {
        try {
            AuthorEntity saved = repo.save(AuthorEntity.toEntity(author));
            return AuthorEntity.toDomain(saved);
        } catch (DataAccessException ex) {
            throw new PersistenceException("DB error during save of author: %s".formatted(ex.getMessage()), ex);
        }
    }

    @Override
    public Author findById(UUID id) {
        try {
            var entity = repo.findById(id)
                    .orElseThrow(() -> new NotFoundException("Author %s not found".formatted(id)));
            return AuthorEntity.toDomain(entity);
        } catch (DataAccessException ex) {
            throw new PersistenceException("DB error during findById: %s".formatted(ex.getMessage()), ex);
        }
    }
}