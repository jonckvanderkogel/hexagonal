package com.bullit.data.adapter.driven.jpa;

import com.bullit.data.adapter.driven.adapter.AuthorRepositoryAdapter;
import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.DatabaseInteractionException;
import com.bullit.domain.model.library.Author;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class AuthorRepositoryAdapterTest {

    private final AuthorJpaRepository jpa = mock(AuthorJpaRepository.class);
    private final AuthorRepositoryAdapter adapter = new AuthorRepositoryAdapter(jpa);

    @Test
    void save_mapsEntityAndBack() {
        var author = Author.rehydrate(UUID.randomUUID(), "Trillian", "Astra", emptyList(), Instant.parse("2024-01-01T00:00:00Z"));
        var entity = new AuthorEntity(author.getId(), author.getFirstName(), author.getLastName(), emptyList(), author.getInsertedAt());
        when(jpa.save(any())).thenReturn(entity);

        var result = adapter.save(author);

        assertSoftly(s -> {
            s.assertThat(result.getFirstName()).isEqualTo("Trillian");
            s.assertThat(result.getLastName()).isEqualTo("Astra");
            s.check(() -> verify(jpa, times(1)).save(any()));
        });
    }

    @Test
    void save_onDataAccessException_wrapsAsPersistenceException() {
        var author = Author.rehydrate(UUID.randomUUID(), "Zaphod", "Beeblebrox", emptyList(), Instant.parse("2024-01-01T00:00:00Z"));
        when(jpa.save(any())).thenThrow(new DataAccessResourceFailureException("boom"));

        assertThatThrownBy(() -> adapter.save(author))
                .isInstanceOf(DatabaseInteractionException.class)
                .hasMessageContaining("DB error during save");
    }

    @Test
    void findById_present_mapsToDomain() {
        var id = UUID.randomUUID();
        var entity = new AuthorEntity(id, "Ford", "Prefect", java.util.List.of(), Instant.parse("2024-01-01T00:00:00Z"));
        when(jpa.findById(id)).thenReturn(Optional.of(entity));

        var result = adapter.findById(id);

        assertSoftly(s -> {
            s.assertThat(result.getFirstName()).isEqualTo("Ford");
            s.assertThat(result.getLastName()).isEqualTo("Prefect");
        });
    }

    @Test
    void findById_missing_throwsNotFound() {
        var id = UUID.randomUUID();
        when(jpa.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> adapter.findById(id))
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining(id.toString());
    }

    @Test
    void findById_dataAccessException_wrapsAsPersistenceException() {
        var id = UUID.randomUUID();
        when(jpa.findById(id)).thenThrow(new DataAccessResourceFailureException("db down"));

        assertThatThrownBy(() -> adapter.findById(id))
                .isInstanceOf(DatabaseInteractionException.class)
                .hasMessageContaining("DB error during findById");
    }
}