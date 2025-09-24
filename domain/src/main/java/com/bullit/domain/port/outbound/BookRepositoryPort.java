package com.bullit.domain.port.outbound;

import com.bullit.domain.model.library.Book;

public interface BookRepositoryPort {
    Book addBook(Book book);
}
