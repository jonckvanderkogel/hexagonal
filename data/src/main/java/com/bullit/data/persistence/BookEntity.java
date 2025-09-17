package com.bullit.data.persistence;

import com.bullit.domain.model.Book;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "books")
public class BookEntity {

    @Id
    private UUID id;

    @JoinColumn(name = "author_id", insertable = false, updatable = false)
    @ManyToOne(fetch = FetchType.EAGER)
    private AuthorEntity author;

    @Column(name = "author_id")
    private UUID authorId;

    @Column(name = "title", nullable = false, length = 200)
    private String title;

    @Column(name = "inserted_at", nullable = false)
    private Instant insertedAt;

    protected BookEntity() {}

    public BookEntity(UUID id, AuthorEntity author, String title, Instant insertedAt) {
        this.id = id;
        this.author = author;
        this.title = title;
        this.insertedAt = insertedAt;
    }

    public BookEntity(UUID id, UUID authorId, String title, Instant insertedAt) {
        this.id = id;
        this.authorId = authorId;
        this.title = title;
        this.insertedAt = insertedAt;
    }

    public UUID getId() { return id; }
    public AuthorEntity getAuthor() { return author; }
    public String getTitle() { return title; }
    public Instant getInsertedAt() { return insertedAt; }

    public static BookEntity toEntity(Book book) {
        return new BookEntity(
                book.getId(),
                book.getAuthorId(),
                book.getTitle(),
                book.getInsertedAt()
        );
    }

    public static Book toDomain(BookEntity entity) {
        UUID authorId =
                entity.getAuthor() != null
                        ? entity.getAuthor().getId()
                        : entity.authorId;

        return Book.rehydrate(
                entity.getId(),
                authorId,
                entity.getTitle(),
                entity.getInsertedAt()
        );
    }
}
