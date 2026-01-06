package com.bullit.application;

import com.bullit.application.file.FileConfigProperties;
import com.bullit.application.streaming.StreamConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public final class HandlerBootstrap {

    private static final Logger log = LoggerFactory.getLogger(HandlerBootstrap.class);

    private final AutowireCapableBeanFactory factory;
    private final FileConfigProperties fileConfig;
    private final StreamConfigProperties streamConfig;

    public HandlerBootstrap(
            AutowireCapableBeanFactory factory,
            FileConfigProperties fileConfig,
            StreamConfigProperties streamConfig
    ) {
        this.factory = factory;
        this.fileConfig = fileConfig;
        this.streamConfig = streamConfig;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrapHandlers() {
        log.info("Bootstrapping integration handlers");

        streamConfig.handlersOrEmpty().forEach(cfg -> {
            log.info("Bootstrapping stream handler for class: {}", cfg.handlerClass());
            factory.createBean(cfg.handlerClass());
        });

        fileConfig.handlersOrEmpty().forEach(cfg -> {
            log.info("Bootstrapping file handler for class: {}", cfg.handlerClass());
            factory.createBean(cfg.handlerClass());
        });

        log.info("Integration handlers bootstrapped");
    }
}