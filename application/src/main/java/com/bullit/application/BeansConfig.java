package com.bullit.application;

import com.bullit.data.adapter.AuthorRepositoryAdapter;
import com.bullit.data.persistence.AuthorJpaRepository;
import com.bullit.domain.port.AuthorRepositoryPort;
import com.bullit.domain.port.AuthorService;
import com.bullit.core.usecase.AuthorServiceImpl;
import com.bullit.web.AuthorHttpHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerResponse;

@Configuration
public class BeansConfig {

    @Bean
    public AuthorService authorService(AuthorRepositoryPort repo) {
        return new AuthorServiceImpl(repo);
    }

    @Bean
    public AuthorRepositoryAdapter authorRepositoryAdapter(AuthorJpaRepository jpaRepository) {
        return new AuthorRepositoryAdapter(jpaRepository);
    }

    @Bean
    public AuthorHttpHandler authorHttpHandler(AuthorService authorService) {
        return new AuthorHttpHandler(authorService);
    }

    @Bean
    public RouterFunction<ServerResponse> routes(AuthorHttpHandler handler) {
        return RouterFunctions.route()
                .POST("/authors", handler::create)
                .GET("/authors/{id}", handler::getById)
                .build();
    }
}
