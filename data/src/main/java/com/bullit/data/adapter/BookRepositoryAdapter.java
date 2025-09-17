package com.bullit.data.adapter;

import com.bullit.data.persistence.BookEntity;
import com.bullit.data.persistence.BookJpaRepository;
import com.bullit.domain.error.PersistenceException;
import com.bullit.domain.model.Book;
import com.bullit.domain.port.BookRepositoryPort;
import org.springframework.dao.DataAccessException;

public class BookRepositoryAdapter implements BookRepositoryPort {
    private final BookJpaRepository repo;

    public BookRepositoryAdapter(BookJpaRepository repo) {
        this.repo = repo;
    }

    @Override
    public Book addBook(Book book) {
        try {
            BookEntity saved = repo.save(BookEntity.toEntity(book));
            return BookEntity.toDomain(saved);
        } catch (DataAccessException ex) {
            throw new PersistenceException("DB error during save of book: %s".formatted(ex.getMessage()), ex);
        }
    }
}
