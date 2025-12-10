package com.bullit.application;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;

@ActiveProfiles("test")
@Import(AbstractIntegrationTest.TestConfig.class)
public abstract class AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    @TestConfiguration
    static class TestConfig {
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