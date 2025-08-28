package com.bullit.application;

import com.bullit.domain.port.AuthorRepositoryPort;
import com.bullit.domain.port.AuthorService;
import com.bullit.core.usecase.AuthorServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeansConfig {

    @Bean
    public AuthorService authorService(AuthorRepositoryPort repo) {
        return new AuthorServiceImpl(repo);
    }
}
