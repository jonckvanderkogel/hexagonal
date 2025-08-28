package com.bullit.application;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

@Testcontainers
@ActiveProfiles("test")
@Import(AbstractIntegrationTest.DbUnitConfig.class)
public abstract class AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    @Container
    public static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17.4");

    @DynamicPropertySource
    static void registerPostgresProperties(DynamicPropertyRegistry registry) {
        log.info("Database: {} Port: {} User: {}",
                postgres.getDatabaseName(), postgres.getFirstMappedPort(), postgres.getUsername());

        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add("spring.liquibase.url", postgres::getJdbcUrl);
        registry.add("spring.liquibase.user", postgres::getUsername);
        registry.add("spring.liquibase.password", postgres::getPassword);
    }

    @TestConfiguration
    static class DbUnitConfig {
        @Bean("dbUnitDatabaseConfig")
        public DatabaseConfigBean dbUnitDatabaseConfig() {
            DatabaseConfigBean bean = new DatabaseConfigBean();
            bean.setDatatypeFactory(new PostgresqlDataTypeFactory());
            return bean;
        }

        @Bean("dbUnitDatabaseConnection")
        @Primary
        public DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection(
                DataSource dataSource,
                DatabaseConfigBean dbUnitDatabaseConfig) {

            DatabaseDataSourceConnectionFactoryBean factory = new DatabaseDataSourceConnectionFactoryBean();
            factory.setDataSource(dataSource);
            factory.setDatabaseConfig(dbUnitDatabaseConfig);
            return factory;
        }
    }
}