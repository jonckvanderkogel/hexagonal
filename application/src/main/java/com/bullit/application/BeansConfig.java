package com.bullit.application;

import com.bullit.data.adapter.AuthorRepositoryAdapter;
import com.bullit.data.adapter.BookRepositoryAdapter;
import com.bullit.data.persistence.AuthorJpaRepository;
import com.bullit.data.persistence.BookJpaRepository;
import com.bullit.domain.port.AuthorRepositoryPort;
import com.bullit.domain.port.BookRepositoryPort;
import com.bullit.domain.port.LibraryServicePort;
import com.bullit.core.usecase.LibraryServiceImpl;
import com.bullit.web.AuthorHttpHandler;
import com.bullit.web.HttpErrorFilter;
import jakarta.validation.Validator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.function.HandlerFilterFunction;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerResponse;

import java.time.Clock;

@Configuration
public class BeansConfig {

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }

    @Bean
    public LibraryServicePort libraryService(
            AuthorRepositoryPort authorRepositoryPort,
            BookRepositoryPort bookRepositoryPort,
            Clock clock
    ) {
        return new LibraryServiceImpl(authorRepositoryPort, bookRepositoryPort, clock);
    }

    @Bean
    public AuthorRepositoryAdapter authorRepositoryAdapter(AuthorJpaRepository jpaRepository) {
        return new AuthorRepositoryAdapter(jpaRepository);
    }

    @Bean
    public BookRepositoryAdapter bookRepositoryAdapter(BookJpaRepository jpaRepository) {
        return new BookRepositoryAdapter(jpaRepository);
    }

    @Bean
    public AuthorHttpHandler authorHttpHandler(LibraryServicePort libraryServicePort, Validator validator) {
        return new AuthorHttpHandler(libraryServicePort, validator);
    }

    @Bean
    public HandlerFilterFunction<ServerResponse, ServerResponse> globalHttpErrorFilter() {
        return new HttpErrorFilter();
    }

    @Bean
    public RouterFunction<ServerResponse> routes(AuthorHttpHandler handler,
                                                 HandlerFilterFunction<ServerResponse, ServerResponse> errorFilter) {
        return RouterFunctions.route()
                .POST("/authors", handler::createAuthor)
                .POST("/authors/{id}/books", handler::addBookToAuthor)
                .GET("/authors/{id}", handler::getAuthorById)
                .filter(errorFilter)
                .build();
    }
}
