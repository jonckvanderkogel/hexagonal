package com.bullit.data.persistence;

import com.bullit.domain.model.Author;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "authors")
public class AuthorEntity {

    @Id
    private UUID id;

    @Column(name = "first_name", nullable = false, length = 100)
    private String firstName;

    @Column(name = "last_name", nullable = false, length = 100)
    private String lastName;

    @Column(name = "inserted_at", nullable = false)
    private Instant insertedAt;

    @OneToMany(mappedBy = "author", fetch = FetchType.EAGER)
    private List<BookEntity> books = new ArrayList<>();

    protected AuthorEntity() {}

    public AuthorEntity(UUID id, String firstName, String lastName, List<BookEntity> books, Instant insertedAt) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.books = books;
        this.insertedAt = insertedAt;
    }

    public UUID getId() { return id; }
    public String getFirstName() { return firstName; }
    public String getLastName() { return lastName; }
    public Instant getInsertedAt() { return insertedAt; }
    public List<BookEntity> getBooks() { return books; }

    public static AuthorEntity toEntity(Author a) {
        var books = a
                .getBooks()
                .stream()
                .map(BookEntity::toEntity)
                .toList();

        return new AuthorEntity(a.getId(), a.getFirstName(), a.getLastName(), books, a.getInsertedAt());
    }

    public static Author toDomain(AuthorEntity e) {
        var books = e
                .getBooks()
                .stream()
                .map(BookEntity::toDomain)
                .toList();

        return Author.rehydrate(
                e.getId(),
                e.getFirstName(),
                e.getLastName(),
                books,
                e.getInsertedAt()
        );
    }
}