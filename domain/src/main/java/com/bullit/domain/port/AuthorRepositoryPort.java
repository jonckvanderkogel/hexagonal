package com.bullit.domain.port;

import com.bullit.domain.model.Author;

import java.util.UUID;

public interface AuthorRepositoryPort {
    Author save(Author author);
    Author findById(UUID id);
}