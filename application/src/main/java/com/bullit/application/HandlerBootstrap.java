package com.bullit.application;

import com.bullit.application.file.FileConfigProperties;
import com.bullit.application.streaming.StreamConfigProperties;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public final class HandlerBootstrap {

    private final AutowireCapableBeanFactory factory;
    private final ConfigurableListableBeanFactory beanFactory;
    private final FileConfigProperties fileConfig;
    private final StreamConfigProperties streamConfig;

    public HandlerBootstrap(
            AutowireCapableBeanFactory factory,
            ConfigurableListableBeanFactory beanFactory,
            FileConfigProperties fileConfig,
            StreamConfigProperties streamConfig
    ) {
        this.factory = factory;
        this.beanFactory = beanFactory;
        this.fileConfig = fileConfig;
        this.streamConfig = streamConfig;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrapHandlers() {
        streamConfig.handlersOrEmpty()
                .forEach(cfg -> register(cfg.handlerClass()));

        streamConfig.batchStreamHandlersOrEmpty()
                .forEach(cfg -> register(cfg.handlerClass()));

        fileConfig.handlersOrEmpty()
                .forEach(cfg -> register(cfg.handlerClass()));
    }

    private <T> void register(Class<T> clazz) {
        var instance = factory.createBean(clazz);
        beanFactory.registerSingleton(clazz.getName(), instance);
    }
}