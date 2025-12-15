package com.bullit.data.adapter.driven.jpa;

import com.bullit.data.adapter.driven.adapter.BookRepositoryAdapter;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.library.Book;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Query;
import org.hibernate.query.NativeQuery;
import org.hibernate.type.BasicTypeReference;
import org.hibernate.type.StandardBasicTypes;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class BookRepositoryAdapterTest {

    private final EntityManager em = mock(EntityManager.class);
    private final Query nativeQuery = mock(Query.class);
    private final BookRepositoryAdapter adapter = new BookRepositoryAdapter(em);

    @Test
    void addBook_mapsRowToDomain() {
        var authorId = UUID.randomUUID();
        var bookId = UUID.randomUUID();
        var inserted = Instant.parse("2024-01-05T00:00:00Z");
        var domain = Book.rehydrate(bookId, authorId, "Preloaded Book", inserted);

        Query jpaQuery = mock(Query.class);
        @SuppressWarnings("unchecked")
        NativeQuery<Object[]> hibernateQuery = mock(NativeQuery.class);

        when(em.createNativeQuery(any())).thenReturn(jpaQuery);
        when(jpaQuery.unwrap(NativeQuery.class)).thenReturn(hibernateQuery);

        when(hibernateQuery
                .addScalar(eq("id"), eq(StandardBasicTypes.UUID)))
                .thenReturn(hibernateQuery);
        when(hibernateQuery
                .addScalar(eq("author_id"), eq(StandardBasicTypes.UUID)))
                .thenReturn(hibernateQuery);
        when(hibernateQuery
                .addScalar(eq("title"), eq(StandardBasicTypes.STRING)))
                .thenReturn(hibernateQuery);
        when(hibernateQuery
                .addScalar(eq("inserted_at"), eq(StandardBasicTypes.INSTANT)))
                .thenReturn(hibernateQuery);

        when(hibernateQuery.setParameter(eq("id"), eq(bookId))).thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(eq("authorId"), eq(authorId))).thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(eq("title"), eq("Preloaded Book"))).thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(eq("insertedAt"), eq(inserted))).thenReturn(hibernateQuery);

        Object[] row = new Object[]{
                bookId,
                authorId,
                "Preloaded Book",
                inserted
        };
        when(hibernateQuery.getSingleResult()).thenReturn(row);

        var saved = adapter.addBook(domain);

        assertSoftly(s -> {
            s.assertThat(saved.getId()).isEqualTo(bookId);
            s.assertThat(saved.getAuthorId()).isEqualTo(authorId);
            s.assertThat(saved.getTitle()).isEqualTo("Preloaded Book");
            s.assertThat(saved.getInsertedAt()).isEqualTo(inserted);

            s.check(() -> verify(em).createNativeQuery(any()));
            s.check(() -> verify(jpaQuery).unwrap(NativeQuery.class));
            s.check(() -> verify(hibernateQuery).getSingleResult());
        });
    }

    @Test
    void addBook_onJpaPersistenceException_wrapsAsDatabaseInteractionException() {
        var authorId = UUID.randomUUID();
        var domain = Book.rehydrate(
                UUID.randomUUID(),
                authorId,
                "Boom",
                Instant.parse("2024-01-05T00:00:00Z")
        );

        Query jpaQuery = mock(Query.class);
        @SuppressWarnings("unchecked")
        NativeQuery<Object[]> hibernateQuery = mock(NativeQuery.class);

        when(em.createNativeQuery(any())).thenReturn(jpaQuery);
        when(jpaQuery.unwrap(NativeQuery.class)).thenReturn(hibernateQuery);

        when(
                hibernateQuery
                        .addScalar(anyString(), any(BasicTypeReference.class)))
                .thenReturn(hibernateQuery);
        when(hibernateQuery.setParameter(anyString(), any()))
                .thenReturn(hibernateQuery);
        when(hibernateQuery.getSingleResult())
                .thenThrow(new PersistenceException("db down"));

        assertThatThrownBy(() -> adapter.addBook(domain))
                .isInstanceOf(DatabaseInteractionException.class)
                .hasMessageContaining("DB error during save of book");
    }
}