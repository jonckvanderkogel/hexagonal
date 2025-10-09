package com.bullit.data.adapter.driven.jpa;

import com.bullit.data.adapter.driven.adapter.BookRepositoryAdapter;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.library.Book;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

final class BookRepositoryAdapterTest {

    private final EntityManager em = mock(EntityManager.class);
    private final Query nativeQuery = mock(Query.class);
    private final BookRepositoryAdapter adapter = new BookRepositoryAdapter(em);

    @Test
    void addBook_mapsRowToDomain() {
        var authorId = UUID.randomUUID();
        var bookId   = UUID.randomUUID();
        var inserted = Instant.parse("2024-01-05T00:00:00Z");
        var domain   = Book.rehydrate(bookId, authorId, "Preloaded Book", inserted);

        when(em.createNativeQuery(any())).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("id"),        eq(bookId))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("authorId"),  eq(authorId))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("title"),     eq("Preloaded Book"))).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("insertedAt"),eq(inserted))).thenReturn(nativeQuery);

        Object[] row = new Object[] {
                bookId,
                authorId,
                "Preloaded Book",
                Timestamp.from(inserted)
        };
        when(nativeQuery.getSingleResult()).thenReturn(row);

        var saved = adapter.addBook(domain);

        assertThat(saved.getId()).isEqualTo(bookId);
        assertThat(saved.getAuthorId()).isEqualTo(authorId);
        assertThat(saved.getTitle()).isEqualTo("Preloaded Book");
        assertThat(saved.getInsertedAt()).isEqualTo(inserted);

        verify(em, times(1)).createNativeQuery(any());
        verify(nativeQuery, times(1)).getSingleResult();
    }

    @Test
    void addBook_onJpaPersistenceException_wrapsAsDatabaseInteractionException() {
        var authorId = UUID.randomUUID();
        var domain   = Book.rehydrate(UUID.randomUUID(), authorId, "Boom", Instant.parse("2024-01-05T00:00:00Z"));

        when(em.createNativeQuery(any())).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("id"),        any())).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("authorId"),  any())).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("title"),     any())).thenReturn(nativeQuery);
        when(nativeQuery.setParameter(eq("insertedAt"),any())).thenReturn(nativeQuery);
        when(nativeQuery.getSingleResult()).thenThrow(new PersistenceException("db down"));

        assertThatThrownBy(() -> adapter.addBook(domain))
                .isInstanceOf(DatabaseInteractionException.class)
                .hasMessageContaining("DB error during save of book");
    }
}