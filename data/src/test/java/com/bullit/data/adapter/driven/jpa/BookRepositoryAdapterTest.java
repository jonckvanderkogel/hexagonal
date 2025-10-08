package com.bullit.data.adapter.driven.jpa;

import com.bullit.data.adapter.driven.adapter.BookRepositoryAdapter;
import com.bullit.domain.error.PersistenceException;
import com.bullit.domain.model.library.Book;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class BookRepositoryAdapterTest {

    private final BookJpaRepository jpa = mock(BookJpaRepository.class);
    private final BookRepositoryAdapter adapter = new BookRepositoryAdapter(jpa);

    @Test
    void addBook_mapsEntityAndBack() {
        var authorId = UUID.randomUUID();
        var bookId   = UUID.randomUUID();
        var inserted = Instant.parse("2024-01-05T00:00:00Z");
        var domain   = Book.rehydrate(bookId, authorId, "Preloaded Book", inserted);

        var authorEntity = new AuthorEntity(
                authorId,
                "First",
                "Last",
                List.of(),
                Instant.parse("2024-01-01T00:00:00Z")
        );
        var savedEntity = new BookEntity(bookId, authorEntity, "Preloaded Book", inserted);

        when(jpa.save(any(BookEntity.class))).thenReturn(savedEntity);

        var saved = adapter.addBook(domain);

        assertThat(saved.getId()).isEqualTo(bookId);
        assertThat(saved.getAuthorId()).isEqualTo(authorId);
        assertThat(saved.getTitle()).isEqualTo("Preloaded Book");
        assertThat(saved.getInsertedAt()).isEqualTo(inserted);
        verify(jpa, times(1)).save(any(BookEntity.class));
    }

    @Test
    void addBook_onDataAccessException_wrapsAsPersistenceException() {
        var authorId = UUID.randomUUID();
        var domain   = Book.rehydrate(UUID.randomUUID(), authorId, "Boom", Instant.parse("2024-01-05T00:00:00Z"));

        when(jpa.save(any(BookEntity.class))).thenThrow(new DataAccessResourceFailureException("db down"));

        assertThatThrownBy(() -> adapter.addBook(domain))
                .isInstanceOf(PersistenceException.class)
                .hasMessageContaining("DB error during save of book");
    }
}
