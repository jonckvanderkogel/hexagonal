package com.bullit.domain.port.driving;

import com.bullit.domain.model.library.Author;
import com.bullit.domain.model.library.Book;

import java.util.UUID;

public interface LibraryServicePort {
    Author createAuthor(String firstName, String lastName);
    Author getById(UUID id);
    Book addBook(UUID authorId, String title);
}