package com.bullit.application.streaming;

import com.bullit.domain.model.stream.InputStreamPort;
import com.bullit.domain.model.stream.OutputStreamPort;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
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

import java.util.List;
import java.util.stream.Collectors;

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

    private List<KafkaInputStream<?>> inputStreams;
    private List<KafkaOutputStream<?>> outputStreams;

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

        outputStreams = createOutputStreams();
        inputStreams = createInputStreams();
        createHandlers();
    }

    @PreDestroy
    public void shutdownStreams() {
        log.info("Shutting down input streams");
        inputStreams.forEach(KafkaInputStream::close);
        log.info("Shutting down output streams");
        outputStreams.forEach(KafkaOutputStream::close);
    }

    private List<KafkaOutputStream<?>> createOutputStreams() {
        var registry = (BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

        return config.outputs().stream()
                .map(cfg -> {
                    log.info("Bootstrapping output stream for topic: {}", cfg.topic());

                    var producer = new KafkaProducer<String, Object>(kafkaProps.buildProducerProperties());

                    var resolvableType = ResolvableType
                            .forClassWithGenerics(
                                    OutputStreamPort.class,
                                    cfg.payloadType()
                            );

                    var beanDef = new RootBeanDefinition(KafkaOutputStream.class);
                    beanDef.setTargetType(resolvableType);
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.topic());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(producer);

                    var beanName = "outputStream:" + cfg.payloadType().getName();
                    registry.registerBeanDefinition(beanName, beanDef);

                    return (KafkaOutputStream<?>) context.getBean(beanName);
                })
                .collect(Collectors.toUnmodifiableList());
    }

    private List<KafkaInputStream<?>> createInputStreams() {
        var registry = (BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

        return config.inputs().stream()
                .map(cfg -> {
                    log.info("Bootstrapping input stream for topic: {}", cfg.topic());

                    var consumer = new KafkaConsumer<String, Object>(kafkaProps.buildConsumerProperties(cfg.groupId()));

                    var resolvableType = ResolvableType
                            .forClassWithGenerics(
                                    InputStreamPort.class,
                                    cfg.payloadType()
                            );

                    var beanDef = new RootBeanDefinition(KafkaInputStream.class);
                    beanDef.setTargetType(resolvableType);
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.topic());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(consumer);

                    var beanName = "inputStream:" + cfg.payloadType().getName();
                    registry.registerBeanDefinition(beanName, beanDef);

                    return (KafkaInputStream<?>) context.getBean(beanName);
                })
                .collect(Collectors.toUnmodifiableList());
    }

    private void createHandlers() {
        config.handlers().forEach(cfg -> {
            log.info("Bootstrapping handler for class: {}", cfg.handlerClass());
            factory.createBean(cfg.handlerClass());
        });
    }
}