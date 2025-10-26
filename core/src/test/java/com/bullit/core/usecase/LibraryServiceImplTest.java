package com.bullit.core.usecase;

import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.library.Author;
import com.bullit.domain.model.library.Book;
import com.bullit.domain.port.driven.AuthorRepositoryPort;
import com.bullit.domain.port.driven.BookRepositoryPort;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

final class LibraryServiceImplTest {

    private final AuthorRepositoryPort authorRepo = mock(AuthorRepositoryPort.class);
    private final BookRepositoryPort bookRepo = mock(BookRepositoryPort.class);
    private final Clock fixed = Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);

    private final LibraryServiceImpl service = new LibraryServiceImpl(authorRepo, bookRepo, fixed);

    @Test
    void createAuthor_happyPath_persists() {
        when(authorRepo.save(any(Author.class))).thenAnswer(inv -> inv.getArgument(0));

        Author created = service.createAuthor("Douglas", "Adams");

        assertThat(created.getFirstName()).isEqualTo("Douglas");
        assertThat(created.getLastName()).isEqualTo("Adams");
        assertThat(created.getInsertedAt()).isEqualTo(Instant.parse("2024-01-01T00:00:00Z"));
        verify(authorRepo, times(1)).save(any(Author.class));
    }

    @Test
    void createAuthor_validationFailure_throws_and_doesNotHitRepo() {
        assertThatThrownBy(() -> service.createAuthor("", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("required");
        verifyNoInteractions(authorRepo);
    }

    @Test
    void getById_happyPath_returnsAuthor() {
        var id = UUID.randomUUID();
        var author = Author.rehydrate(id, "Arthur", "Dent", emptyList(), Instant.now());
        when(authorRepo.findById(id)).thenReturn(author);

        Author result = service.getById(id);

        assertThat(result.getId()).isEqualTo(id);
    }

    @Test
    void getById_repoThrows_propagates() {
        var id = UUID.randomUUID();
        when(authorRepo.findById(id)).thenThrow(new DatabaseInteractionException("DB down", new RuntimeException("x")));

        assertThatThrownBy(() -> service.getById(id))
                .isInstanceOf(DatabaseInteractionException.class)
                .hasMessageContaining("DB down");
    }

    @Test
    void addBook_persists_viaBookRepository() {
        var authorId = UUID.randomUUID();
        var book = Book.rehydrate(UUID.randomUUID(), authorId, "H2G2", Instant.parse("2024-01-01T00:00:00Z"));
        when(bookRepo.addBook(any(Book.class))).thenReturn(book);

        Book saved = service.addBook(authorId, "H2G2");

        assertThat(saved.getAuthorId()).isEqualTo(authorId);
        assertThat(saved.getTitle()).isEqualTo("H2G2");
        verify(bookRepo, times(1)).addBook(any(Book.class));
    }
}