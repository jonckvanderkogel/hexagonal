package com.bullit.application.streaming;

import com.bullit.domain.port.driven.stream.BatchInputStreamPort;
import com.bullit.domain.port.driven.stream.InputStreamPort;
import com.bullit.domain.port.driven.stream.OutputStreamPort;
import com.bullit.domain.port.driven.stream.StreamKey;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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

    private List<KafkaInputStream<?>> inputStreams;
    private List<KafkaOutputStream<?>> outputStreams;

    public StreamBootstrap(StreamConfigProperties config,
                           KafkaClientProperties kafkaProps,
                           ApplicationContext context) {
        this.config = config;
        this.kafkaProps = kafkaProps;
        this.context = context;
    }

    @PostConstruct
    public void bootstrapStreams() {
        log.info("Bootstrapping streams");

        outputStreams = createOutputStreams();
        inputStreams = createInputStreams();
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

        return config.outputsOrEmpty().stream()
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
                    var keyFun = resolveKeyFunction(cfg.payloadType(), cfg.key());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(keyFun);

                    var beanName = "outputStream:" + cfg.payloadType().getName();
                    registry.registerBeanDefinition(beanName, beanDef);

                    return (KafkaOutputStream<?>) context.getBean(beanName);
                })
                .collect(Collectors.toUnmodifiableList());
    }

    private List<KafkaInputStream<?>> createInputStreams() {
        var registry = (BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

        return config.inputsOrEmpty().stream()
                .map(cfg -> {
                    log.info("Bootstrapping input stream for topic: {}", cfg.topic());

                    var consumerProps = kafkaProps.buildConsumerProperties(
                            cfg.groupId(),
                            cfg.partitionQueueCapacity()
                    );

                    var consumer = new KafkaConsumer<String, Object>(consumerProps);

                    var resolvableType = ResolvableType
                            .forClassWithGenerics(
                                    KafkaInputStream.class,
                                    cfg.payloadType()
                            );

                    var beanDef = new RootBeanDefinition(KafkaInputStream.class);
                    beanDef.setTargetType(resolvableType);
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.topic());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(consumer);
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.partitionQueueCapacity());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.maxBatchSize());

                    var beanName = "inputStream:" + cfg.payloadType().getName();
                    registry.registerBeanDefinition(beanName, beanDef);

                    return (KafkaInputStream<?>) context.getBean(beanName);
                })
                .collect(Collectors.toUnmodifiableList());
    }

    private static <T> Function<T, String> resolveKeyFunction(
            Class<?> payloadType,
            Class<? extends StreamKey<?>> keyClass
    ) {
        return Optional.ofNullable(keyClass)
                .map(StreamBootstrap::instantiateKey)
                .map(key -> verifyKeyMatchesPayload(key, payloadType))
                .map(StreamBootstrap::<T>unsafeCastToPayloadFunction)
                .orElseGet(StreamBootstrap::nullKeyFunction);
    }

    private static StreamKey<?> instantiateKey(Class<? extends StreamKey<?>> keyClass) {
        try {
            return keyClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to instantiate StreamKey '%s'. Ensure it has a public no-args constructor."
                            .formatted(keyClass.getName()),
                    e
            );
        }
    }

    private static StreamKey<?> verifyKeyMatchesPayload(StreamKey<?> key, Class<?> payloadType) {
        var keyPayloadType = key.payloadType();
        if (!keyPayloadType.equals(payloadType)) {
            throw new IllegalStateException(
                    "StreamKey payload type mismatch. Output payloadType=%s but key=%s payloadType=%s"
                            .formatted(payloadType.getName(), key.getClass().getName(), keyPayloadType.getName())
            );
        }
        return key;
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<T, String> unsafeCastToPayloadFunction(StreamKey<?> key) {
        return (Function<T, String>) key;
    }

    private static <T> Function<T, String> nullKeyFunction() {
        return _ -> null;
    }
}