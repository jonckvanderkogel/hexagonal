package com.bullit.data.adapter.driven.adapter;

import com.bullit.data.adapter.driven.jpa.BookEntity;
import com.bullit.data.adapter.driven.jpa.BookJpaRepository;
import com.bullit.domain.error.PersistenceException;
import com.bullit.domain.model.library.Book;
import com.bullit.domain.port.outbound.BookRepositoryPort;
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
            throw new PersistenceException("DB error during save of book", ex);
        }
    }
}
