package com.bullit.application;

import com.bullit.core.usecase.LibraryServiceImpl;
import com.bullit.core.usecase.RoyaltyServiceImpl;
import com.bullit.data.adapter.driven.adapter.AuthorRepositoryAdapter;
import com.bullit.data.adapter.driven.adapter.BookRepositoryAdapter;
import com.bullit.data.adapter.driven.adapter.SalesReportingAdapter;
import com.bullit.data.adapter.driven.jpa.AuthorJpaRepository;
import com.bullit.data.adapter.driven.jpa.BookJpaRepository;
import com.bullit.data.adapter.driven.jpa.SaleJpaRepository;
import com.bullit.domain.model.royalty.RoyaltyScheme;
import com.bullit.domain.model.royalty.RoyaltyTier;
import com.bullit.domain.port.inbound.LibraryServicePort;
import com.bullit.domain.port.inbound.RoyaltyServicePort;
import com.bullit.domain.port.outbound.AuthorRepositoryPort;
import com.bullit.domain.port.outbound.BookRepositoryPort;
import com.bullit.domain.port.outbound.reporting.SalesReportingPort;
import com.bullit.web.adapter.driving.http.AuthorHttpHandler;
import com.bullit.web.adapter.driving.http.HttpErrorFilter;
import com.bullit.web.adapter.driving.http.RoyaltyHttpHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.function.HandlerFilterFunction;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerResponse;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;

@Configuration
public class BeansConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
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
    public RoyaltyServicePort royaltyServicePort(
            SalesReportingPort salesReportingPort,
            RoyaltyScheme royaltyScheme
    ) {
        return new RoyaltyServiceImpl(salesReportingPort, royaltyScheme);
    }

    @Bean
    public RoyaltyScheme royaltyScheme() {
        return RoyaltyScheme.of(
                List.of(
                        RoyaltyTier.of(1000, BigDecimal.valueOf(0.1)),
                        RoyaltyTier.of(2500, BigDecimal.valueOf(0.15)),
                        RoyaltyTier.of(5000, BigDecimal.valueOf(0.2)),
                        RoyaltyTier.of(10000, BigDecimal.valueOf(0.25))
                ),
                new BigDecimal("100")
        );
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
    public SalesReportingAdapter salesReportingAdapter(SaleJpaRepository jpaRepository) {
        return new SalesReportingAdapter(jpaRepository);
    }

    @Bean
    public AuthorHttpHandler authorHttpHandler(LibraryServicePort libraryServicePort) {
        return new AuthorHttpHandler(libraryServicePort);
    }

    @Bean RoyaltyHttpHandler royaltyHttpHandler(RoyaltyServicePort royaltyServicePort) {
        return new RoyaltyHttpHandler(royaltyServicePort);
    }

    @Bean
    public HandlerFilterFunction<ServerResponse, ServerResponse> globalHttpErrorFilter() {
        return new HttpErrorFilter();
    }

    @Bean
    public RouterFunction<ServerResponse> routes(AuthorHttpHandler authorHandler,
                                                 RoyaltyHttpHandler royaltyHandler,
                                                 HandlerFilterFunction<ServerResponse, ServerResponse> errorFilter) {
        return RouterFunctions.route()
                .POST("/authors", authorHandler::createAuthor)
                .POST("/authors/{id}/books", authorHandler::addBookToAuthor)
                .GET("/authors/{id}", authorHandler::getAuthorById)
                .GET("/authors/{id}/royalties/{period}", royaltyHandler::getMonthlyRoyalty)
                .filter(errorFilter)
                .build();
    }
}
