package com.bullit.application;

import com.bullit.domain.model.Author;
import com.bullit.domain.model.Book;
import com.bullit.domain.port.LibraryServicePort;
import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@TestExecutionListeners(
        value = {DbUnitTestExecutionListener.class},
        mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
@DbUnitConfiguration
@DatabaseSetup("/dbunit/authorServiceTestDataset.xml")
@Import(LibraryServiceIT.TestConfig.class)
class LibraryServiceIT extends AbstractIntegrationTest {

    @Autowired
    private LibraryServicePort libraryServicePort;

    @Test
    void createAndFindAuthor() {
        Author created = libraryServicePort.createAuthor("Douglas", "Adams");
        assertThat(created.getFirstName()).isEqualTo("Douglas");
        assertThat(created.getLastName()).isEqualTo("Adams");

        Author found = libraryServicePort.getById(created.getId());
        assertThat(found.getId()).isEqualTo(created.getId());
        assertThat(found.getFirstName()).isEqualTo("Douglas");
        assertThat(found.getLastName()).isEqualTo("Adams");
        assertThat(found.getInsertedAt())
                .isEqualTo(
                        LocalDateTime.of(2024, 8, 13, 9, 0, 0)
                                .toInstant(ZoneOffset.UTC)
                );
    }

    @Test
    void getExistingAuthorById() {
        var existingId = UUID.fromString("11111111-1111-1111-1111-111111111111");

        Author result = libraryServicePort.getById(existingId);
        assertThat(result.getFirstName()).isEqualTo("Preloaded");
        assertThat(result.getLastName()).isEqualTo("Author");
        assertThat(result.getBooks()).hasSize(1);
        assertThat(result.getBooks().getFirst().getTitle()).isEqualTo("Fixture Book");
    }

    @Test
    void addBook_adds_and_is_visible_on_fetch() {
        Author created = libraryServicePort.createAuthor("Terry", "Pratchett");

        Book savedBook = libraryServicePort.addBook(created.getId(), "Small Gods");
        assertThat(savedBook.getAuthorId()).isEqualTo(created.getId());
        assertThat(savedBook.getTitle()).isEqualTo("Small Gods");

        Author fetched = libraryServicePort.getById(created.getId());
        assertThat(fetched.getBooks()).extracting(Book::getTitle).contains("Small Gods");
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        public Clock clock() {
            return Clock
                    .fixed(
                            LocalDateTime.of(2024, 8, 13, 9, 0, 0)
                                    .toInstant(ZoneOffset.UTC),
                            ZoneOffset.UTC
                    );
        }
    }
}