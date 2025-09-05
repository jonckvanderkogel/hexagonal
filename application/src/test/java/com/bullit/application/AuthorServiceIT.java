package com.bullit.application;

import com.bullit.domain.port.AuthorServicePort;
import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;

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
class AuthorServiceIT extends AbstractIntegrationTest {

    @Autowired
    private AuthorServicePort authorServicePort;

    @Test
    void createAndFindAuthor() {
        // create
        var created = authorServicePort.create("Douglas Adams");
        assertThat(created.isRight()).isTrue();

        var saved = created.get();
        assertThat(saved.name()).isEqualTo("Douglas Adams");

        var found = authorServicePort.getById(saved.id());
        assertThat(found.isRight()).isTrue();
        assertThat(found.get().id()).isEqualTo(saved.id());
    }

    @Test
    void getExistingAuthorFromDbUnitDataset() {
        var existingId = UUID.fromString("11111111-1111-1111-1111-111111111111");

        var result = authorServicePort.getById(existingId);
        assertThat(result.isRight()).isTrue();
        assertThat(result.get().name()).isEqualTo("Preloaded Author");
    }
}