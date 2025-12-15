package com.bullit.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = "com.bullit")
@EnableJpaRepositories(basePackages = "com.bullit.data")
@EntityScan(basePackages = "com.bullit.data")
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
