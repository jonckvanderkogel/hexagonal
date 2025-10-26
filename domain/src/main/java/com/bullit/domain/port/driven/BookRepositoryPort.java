package com.bullit.domain.port.driven;

import com.bullit.domain.model.library.Book;

public interface BookRepositoryPort {
    Book addBook(Book book);
}
