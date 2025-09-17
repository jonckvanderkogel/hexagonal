package com.bullit.domain.port;

import com.bullit.domain.model.Author;
import com.bullit.domain.model.Book;

import java.util.UUID;

public interface LibraryServicePort {
    Author createAuthor(String firstName, String lastName);
    Author getById(UUID id);
    Book addBook(UUID authorId, String title);
}