package com.bullit.application.streaming;

import com.bullit.domain.model.stream.InputStreamPort;
import com.bullit.domain.model.stream.OutputStreamPort;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
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

    public StreamBootstrap(StreamConfigProperties config,
                           KafkaClientProperties kafkaProps,
                           AutowireCapableBeanFactory factory,
                           ApplicationContext context) {
        this.config = config;
        this.kafkaProps = kafkaProps;
        this.factory = factory;
        this.context = context;
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
            var payloadType = cfg.payloadType();
            var beanName = "outputStream:" + payloadType.getName();

            var resolvableType = ResolvableType
                    .forClassWithGenerics(OutputStreamPort.class, payloadType);

            var kafkaProducer = new KafkaProducer<>(kafkaProps.buildProducerProperties());

            var beanDef = new RootBeanDefinition(KafkaOutputStream.class);
            beanDef.setTargetType(resolvableType);
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.topic());
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(kafkaProducer);

            registry.registerBeanDefinition(beanName, beanDef);
        });
    }

    private void createInputStreams() {
        BeanDefinitionRegistry registry =
                (BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

        config.inputs().forEach(cfg -> {
            log.info("Bootstrapping input stream for topic: {}", cfg.topic());

            var beanName = "inputStream:" + cfg.payloadType().getName();

            var type = ResolvableType
                    .forClassWithGenerics(InputStreamPort.class, cfg.payloadType());

            var consumer = new KafkaConsumer<>(kafkaProps.buildConsumerProperties(cfg.groupId()));

            var beanDef = new RootBeanDefinition(KafkaInputStream.class);
            beanDef.setTargetType(type);
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.topic());
            beanDef.getConstructorArgumentValues().addGenericArgumentValue(consumer);

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