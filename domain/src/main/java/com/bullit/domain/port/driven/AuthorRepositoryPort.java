package com.bullit.domain.port.driven;

import com.bullit.domain.model.library.Author;

import java.util.UUID;

public interface AuthorRepositoryPort {
    Author save(Author author);
    Author findById(UUID id);
}