package com.bullit.application.streaming;

import com.bullit.domain.model.stream.InputStreamPort;
import com.bullit.domain.model.stream.OutputStreamPort;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;

@Configuration
@EnableConfigurationProperties({
        StreamConfigProperties.class,
        KafkaClientProperties.class
})
public class StreamBootstrap {
    private static final Logger log = LoggerFactory.getLogger(StreamBootstrap.class);

    private final ApplicationContext context;
    private final StreamConfigProperties config;
    private final KafkaClientProperties kafkaProps;
    private final AutowireCapableBeanFactory factory;
    private final ObjectMapper mapper;

    public StreamBootstrap(StreamConfigProperties config,
                           KafkaClientProperties kafkaProps,
                           AutowireCapableBeanFactory factory,
                           ApplicationContext context,
                           ObjectMapper mapper) {
        this.config = config;
        this.kafkaProps = kafkaProps;
        this.factory = factory;
        this.context = context;
        this.mapper = mapper;
    }

    @PostConstruct
    public void bootstrapStreams() {
        log.info("Bootstrapping streams");
        createOutputStreams();
        createInputStreams();
        createHandlers();
    }

    private void createOutputStreams() {
        BeanDefinitionRegistry registry =
                (BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

        config.outputs().forEach(cfg -> {
            log.info("Bootstrapping output stream for topic: {}", cfg.topic());
            Class<?> payloadType = cfg.payloadType();
            String beanName = "outputStream:" + payloadType.getName();

            ResolvableType resolvableType = ResolvableType
                    .forClassWithGenerics(OutputStreamPort.class, payloadType);

            RootBeanDefinition beanDef = new RootBeanDefinition(KafkaOutputStream.class);
            beanDef.setTargetType(resolvableType);
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.topic());
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(kafkaProps);
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(mapper);

            registry.registerBeanDefinition(beanName, beanDef);
        });
    }

    private void createInputStreams() {
        BeanDefinitionRegistry registry =
                (BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

        config.inputs().forEach(cfg -> {
            log.info("Bootstrapping input stream for topic: {}", cfg.topic());
            String beanName = "inputStream:" + cfg.payloadType().getName();

            ResolvableType type = ResolvableType
                    .forClassWithGenerics(InputStreamPort.class, cfg.payloadType());

            RootBeanDefinition beanDef = new RootBeanDefinition(KafkaInputStream.class);
            beanDef.setTargetType(type);
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.topic());
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.groupId());
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(kafkaProps);
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.payloadType());
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(mapper);

            registry.registerBeanDefinition(beanName, beanDef);
        });
    }

    private void createHandlers() {
        config.handlers().forEach(cfg -> {
            log.info("Bootstrapping handler for class: {}", cfg.handlerClass());
            factory.createBean(cfg.handlerClass());
        });
    }
}