package com.bullit.data.adapter;

import com.bullit.data.persistence.AuthorEntity;
import com.bullit.data.persistence.AuthorJpaRepository;
import com.bullit.domain.error.NotFoundException;
import com.bullit.domain.error.PersistenceException;
import com.bullit.domain.model.Author;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

        assertThat(result.getFirstName()).isEqualTo("Trillian");
        assertThat(result.getLastName()).isEqualTo("Astra");
        verify(jpa, times(1)).save(any());
    }

    @Test
    void save_onDataAccessException_wrapsAsPersistenceException() {
        var author = Author.rehydrate(UUID.randomUUID(), "Zaphod", "Beeblebrox", emptyList(), Instant.parse("2024-01-01T00:00:00Z"));
        when(jpa.save(any())).thenThrow(new DataAccessResourceFailureException("boom"));

        assertThatThrownBy(() -> adapter.save(author))
                .isInstanceOf(PersistenceException.class)
                .hasMessageContaining("DB error during save");
    }

    @Test
    void findById_present_mapsToDomain() {
        var id = UUID.randomUUID();
        var entity = new AuthorEntity(id, "Ford", "Prefect", java.util.List.of(), Instant.parse("2024-01-01T00:00:00Z"));
        when(jpa.findById(id)).thenReturn(Optional.of(entity));

        var result = adapter.findById(id);

        assertThat(result.getFirstName()).isEqualTo("Ford");
        assertThat(result.getLastName()).isEqualTo("Prefect");
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
                .isInstanceOf(PersistenceException.class)
                .hasMessageContaining("DB error during findById");
    }
}