package com.bullit.data.adapter;

import com.bullit.domain.model.Author;
import com.bullit.domain.error.AppError;
import com.bullit.domain.error.NotFoundError;
import com.bullit.domain.error.PersistenceError;
import com.bullit.data.persistence.AuthorEntity;
import com.bullit.data.persistence.AuthorJpaRepository;
import io.vavr.control.Either;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

final class AuthorRepositoryAdapterTest {

    private final AuthorJpaRepository jpa = mock(AuthorJpaRepository.class);
    private final AuthorRepositoryAdapter adapter = new AuthorRepositoryAdapter(jpa);

    @Test
    void save_mapsEntityAndBack() {
        var author = Author.rehydrate(UUID.randomUUID(), "Trillian");
        var entity = new AuthorEntity(author.id(), author.name());
        when(jpa.save(any())).thenReturn(entity);

        Either<? extends AppError, Author> result = adapter.save(author);

        assertThat(result.isRight()).isTrue();
        assertThat(result.get().name()).isEqualTo("Trillian");
        verify(jpa, times(1)).save(any());
    }

    @Test
    void save_onException_returnsPersistenceError() {
        var author = Author.rehydrate(UUID.randomUUID(), "Zaphod");
        when(jpa.save(any())).thenThrow(new RuntimeException("boom"));

        var result = adapter.save(author);

        assertThat(result.isLeft()).isTrue();
        assertThat(result.getLeft()).isInstanceOf(PersistenceError.class);
    }

    @Test
    void findById_present_mapsToDomain() {
        var id = UUID.randomUUID();
        when(jpa.findById(id)).thenReturn(Optional.of(new AuthorEntity(id, "Ford Prefect")));

        var result = adapter.findById(id);

        assertThat(result.isRight()).isTrue();
        assertThat(result.get().name()).isEqualTo("Ford Prefect");
    }

    @Test
    void findById_missing_returnsNotFound() {
        var id = UUID.randomUUID();
        when(jpa.findById(id)).thenReturn(Optional.empty());

        var result = adapter.findById(id);

        assertThat(result.isLeft()).isTrue();
        assertThat(result.getLeft()).isInstanceOf(NotFoundError.class);
    }

    @Test
    void findById_exception_returnsPersistenceError() {
        var id = UUID.randomUUID();
        when(jpa.findById(id)).thenThrow(new RuntimeException("db down"));

        var result = adapter.findById(id);

        assertThat(result.isLeft()).isTrue();
        assertThat(result.getLeft()).isInstanceOf(PersistenceError.class);
    }
}