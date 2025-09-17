package com.bullit.domain.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Size;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.bullit.domain.model.DomainValidator.assertValid;
import static java.util.Collections.emptyList;

public final class Author {
    @NotNull(message = "Author id is required")
    private final UUID id;

    @Size(max = 100, message = "Author first name can be 100 characters at most")
    @NotBlank(message = "Author first name is required")
    private final String firstName;

    @Size(max = 100, message = "Author last name can be 100 characters at most")
    @NotBlank(message = "Author last name is required")
    private final String lastName;

    @NotNull(message = "List of books can be empty but must not be null")
    private final List<Book> books;

    @NotNull
    @PastOrPresent(message = "InsertedAt is required, and must be in the past or present")
    private final Instant insertedAt;

    private Author(UUID id, String firstName, String lastName, List<Book> books, Instant insertedAt) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.books = books;
        this.insertedAt = insertedAt;
    }

    public UUID getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public List<Book> getBooks() {
        return books;
    }

    public Instant getInsertedAt() {
        return insertedAt;
    }

    public static Author createNew(String firstName,
                                   String lastName,
                                   Clock clock
    ) {
        return createNew(
                firstName,
                lastName,
                emptyList(),
                clock
        );
    }

    public static Author createNew(String firstName,
                                   String lastName,
                                   List<Book> books,
                                   Clock clock
    ) {
        return assertValid(new Author(
                UUID.randomUUID(),
                firstName,
                lastName,
                books,
                clock.instant()
        ));
    }

    public static Author rehydrate(UUID id,
                                   String firstName,
                                   String lastName,
                                   List<Book> books,
                                   Instant insertedAt
    ) {
        return assertValid(new Author(
                id,
                firstName,
                lastName,
                books,
                insertedAt
        ));
    }
}