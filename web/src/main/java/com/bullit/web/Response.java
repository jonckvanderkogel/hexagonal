package com.bullit.web;

import com.bullit.domain.model.Author;
import com.bullit.domain.model.Book;

import java.time.Instant;
import java.util.List;

public class Response {
    public record AuthorResponse(
            String id,
            String firstName,
            String lastName,
            List<BookResponse> books,
            Instant insertedAt
    ) {
        public static AuthorResponse fromDomain(Author a) {
            return new AuthorResponse(
                    a.getId().toString(),
                    a.getFirstName(),
                    a.getLastName(),
                    a.getBooks().stream().map(BookResponse::fromDomain).toList(),
                    a.getInsertedAt()
            );
        }
    }

    public record BookResponse(
            String id,
            String authorId,
            String title,
            Instant insertedAt
    ) {
        public static BookResponse fromDomain(Book b) {
            return new BookResponse(
                    b.getId().toString(),
                    b.getAuthorId().toString(),
                    b.getTitle(),
                    b.getInsertedAt()
            );
        }
    }

    public record ErrorResponse(String error) {}
}
