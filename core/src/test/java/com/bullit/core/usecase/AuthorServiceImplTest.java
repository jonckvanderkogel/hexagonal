package com.bullit.core.usecase;

import com.bullit.domain.model.Author;
import com.bullit.domain.error.PersistenceError;
import com.bullit.domain.port.AuthorRepositoryPort;
import io.vavr.control.Either;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;

final class AuthorServiceImplTest {

    private final AuthorRepositoryPort repo = mock(AuthorRepositoryPort.class);
    private final AuthorServiceImpl service = new AuthorServiceImpl(repo);

    @Test
    void create_happyPath_persists() {
        var name = "Douglas Adams";
        when(repo.save(any(Author.class)))
                .thenAnswer(inv -> Either.right((Author) inv.getArgument(0)));

        var result = service.create(name);

        assertThat(result.isRight()).isTrue();
        var author = result.get();
        assertThat(author.name()).isEqualTo(name);
        verify(repo, times(1)).save(any(Author.class));
    }

    @Test
    void create_validationFailure_doesNotHitRepo() {
        var result = service.create(""); // invalid -> ValidationError
        assertThat(result.isLeft()).isTrue();
        assertThat(result.getLeft().message()).contains("Name");
        verifyNoInteractions(repo);
    }

    @Test
    void getById_happyPath_returnsAuthor() {
        var id = UUID.randomUUID();
        var author = Author.rehydrate(id, "Arthur Dent").get();
        when(repo.findById(id))
                .thenReturn(
                        Either.right(author)
                );

        var result = service.getById(id);

        assertThat(result.isRight()).isTrue();
        assertThat(result.get().id()).isEqualTo(id);
    }

    @Test
    void getById_repoError_propagatesLeft() {
        var id = UUID.randomUUID();
        when(repo.findById(id))
                .thenReturn(
                        Either.left(
                                new PersistenceError.AuthorPersistenceError("DB down")
                        )
                );

        var result = service.getById(id);

        assertThat(result.isLeft()).isTrue();
        assertThat(result.getLeft().message()).contains("DB down");
    }
}