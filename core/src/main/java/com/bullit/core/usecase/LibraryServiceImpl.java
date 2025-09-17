package com.bullit.core.usecase;

import com.bullit.domain.model.Author;
import com.bullit.domain.model.Book;
import com.bullit.domain.port.AuthorRepositoryPort;
import com.bullit.domain.port.BookRepositoryPort;
import com.bullit.domain.port.LibraryServicePort;

import java.time.Clock;
import java.util.UUID;

public final class LibraryServiceImpl implements LibraryServicePort {

    private final AuthorRepositoryPort authorRepositoryPort;
    private final BookRepositoryPort bookRepositoryPort;
    private final Clock clock;

    public LibraryServiceImpl(
            AuthorRepositoryPort authorRepositoryPort,
            BookRepositoryPort bookRepositoryPort,
            Clock clock
    ) {
        this.authorRepositoryPort = authorRepositoryPort;
        this.bookRepositoryPort = bookRepositoryPort;
        this.clock = clock;
    }


    @Override
    public Author createAuthor(String firstName, String lastName) {
        return authorRepositoryPort.save(Author.createNew(firstName, lastName, clock));
    }

    @Override
    public Author getById(UUID id) {
        return authorRepositoryPort.findById(id);
    }

    @Override
    public Book addBook(UUID authorId, String title) {
        return bookRepositoryPort.addBook(Book.createNew(authorId, title, clock));
    }
}