package com.bullit.domain.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Size;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import static com.bullit.domain.model.DomainValidator.assertValid;

public final class Book {
    @NotNull(message = "Book id is required")
    private final UUID id;

    @NotNull(message = "Author id is required")
    private final UUID authorId;

    @Size(max = 100, message = "Book title can be 200 characters at most")
    @NotBlank(message = "Book title is required")
    private final String title;

    @NotNull
    @PastOrPresent(message = "InsertedAt is required, and must be in the past or present")
    private final Instant insertedAt;

    private Book(UUID id, UUID authorId, String title, Instant insertedAt) {
        this.id = id;
        this.authorId = authorId;
        this.title = title;
        this.insertedAt = insertedAt;
    }

    public static Book createNew(UUID authorId,
                                 String title,
                                 Clock clock
    ) {
        return assertValid(new Book(
                UUID.randomUUID(),
                authorId,
                title,
                clock.instant()
        ));
    }

    public static Book rehydrate(UUID id,
                                 UUID authorId,
                                 String title,
                                 Instant insertedAt
    ) {
        return assertValid(new Book(
                id,
                authorId,
                title,
                insertedAt
        ));
    }

    public UUID getId() {
        return id;
    }

    public UUID getAuthorId() {
        return authorId;
    }

    public String getTitle() {
        return title;
    }

    public Instant getInsertedAt() {
        return insertedAt;
    }
}
