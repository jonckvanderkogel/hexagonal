package com.bullit.data.adapter.driven.adapter;

import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.library.Book;
import com.bullit.domain.port.driven.BookRepositoryPort;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.PersistenceException;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

public final class BookRepositoryAdapter implements BookRepositoryPort {

    private static final String INSERT_WHERE_AUTHOR_EXISTS = """
            INSERT INTO books (id, author_id, title, inserted_at)
            SELECT :id, :authorId, :title, :insertedAt
            WHERE EXISTS (SELECT 1 FROM authors WHERE id = :authorId)
            RETURNING id, author_id, title, inserted_at
            """;

    private final EntityManager em;

    public BookRepositoryAdapter(EntityManager em) {
        this.em = em;
    }

    @Override
    public Book addBook(Book book) {
        try {
            final Object[] row = (Object[]) em
                    .createNativeQuery(INSERT_WHERE_AUTHOR_EXISTS)
                    .setParameter("id", book.getId())
                    .setParameter("authorId", book.getAuthorId())
                    .setParameter("title", book.getTitle())
                    .setParameter("insertedAt", book.getInsertedAt())
                    .getSingleResult();

            final UUID id = (UUID) row[0];
            final UUID authorId = (UUID) row[1];
            final String title = (String) row[2];
            final Instant created = ((Timestamp) row[3]).toInstant();

            return Book.rehydrate(id, authorId, title, created);

        } catch (NoResultException e) {
            throw new NotFoundException("Author %s not found".formatted(book.getAuthorId()));
        } catch (PersistenceException e) {
            throw new DatabaseInteractionException("DB error during save of book", e);
        }
    }
}