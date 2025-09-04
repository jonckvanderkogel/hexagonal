package com.bullit.application;

import com.bullit.domain.port.AuthorServicePort;
import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;

import java.util.UUID;

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
        Assertions.assertThat(created.isRight()).isTrue();

        var saved = created.get();
        Assertions.assertThat(saved.name()).isEqualTo("Douglas Adams");

        var found = authorServicePort.getById(saved.id());
        Assertions.assertThat(found.isRight()).isTrue();
        Assertions.assertThat(found.get().id()).isEqualTo(saved.id());
    }

    @Test
    void getExistingAuthorFromDbUnitDataset() {
        var existingId = UUID.fromString("11111111-1111-1111-1111-111111111111");

        var result = authorServicePort.getById(existingId);
        Assertions.assertThat(result.isRight()).isTrue();
        Assertions.assertThat(result.get().name()).isEqualTo("Preloaded Author");
    }
}