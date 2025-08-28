package com.bullit.data.persistence;

import com.bullit.domain.model.Author;

public final class AuthorDataMapper {
    private AuthorDataMapper() {}

    public static AuthorEntity toEntity(Author a) {
        return new AuthorEntity(a.id(), a.name());
    }

    public static Author toDomain(AuthorEntity e) {
        return Author.rehydrate(e.getId(), e.getName());
    }
}