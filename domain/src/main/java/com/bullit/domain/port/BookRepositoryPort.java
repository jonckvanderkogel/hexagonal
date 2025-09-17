package com.bullit.domain.port;

import com.bullit.domain.model.Book;

public interface BookRepositoryPort {
    Book addBook(Book book);
}
