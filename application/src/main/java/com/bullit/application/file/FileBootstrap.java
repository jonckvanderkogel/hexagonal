package com.bullit.application.file;

import com.bullit.domain.port.driven.file.FileInputPort;
import com.bullit.domain.port.driven.file.FileOutputPort;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.MinioClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@EnableConfigurationProperties({
        FileConfigProperties.class,
        S3ClientProperties.class,
        GarageAdminProperties.class
})
public class FileBootstrap {

    private static final Logger log = LoggerFactory.getLogger(FileBootstrap.class);

    private final ApplicationContext context;
    private final FileConfigProperties config;
    private final S3ClientProperties s3Props;
    private final ObjectMapper objectMapper;
    private final S3CredentialBootstrap credentialBootstrap;
    private final GarageAdminClient garageAdmin;

    private List<S3FileInput<?>> inputs;
    private List<S3FileOutput<?>> outputs;

    public FileBootstrap(
            ApplicationContext context,
            FileConfigProperties config,
            S3ClientProperties s3Props,
            ObjectMapper objectMapper,
            S3CredentialBootstrap credentialBootstrap,
            GarageAdminClient garageAdmin
    ) {
        this.context = context;
        this.config = config;
        this.s3Props = s3Props;
        this.objectMapper = objectMapper;
        this.credentialBootstrap = credentialBootstrap;
        this.garageAdmin = garageAdmin;
    }

    @PostConstruct
    public void bootstrapFiles() {
        log.info("Bootstrapping file ports");

        var credentials = credentialBootstrap.ensureS3Credentials();

        ensureBucketsForConfiguredPorts(credentials.getAccessKey());

        var effectiveProps = s3Props.withCredentials(
                credentials.getAccessKey(),
                credentials.getSecretKey()
        );

        var client = S3ClientFactory.create(effectiveProps);

        outputs = createOutputPorts(client);
        inputs = createInputPorts(client);
    }

    private void ensureBucketsForConfiguredPorts(String accessKeyId) {
        Stream.concat(config.inputsOrEmpty().stream().map(FileConfigProperties.InputConfig::bucket),
                        config.outputsOrEmpty().stream().map(FileConfigProperties.OutputConfig::bucket))
                .distinct()
                .forEach(bucket -> garageAdmin.ensureBucketAndPermissions(bucket, accessKeyId));
    }

    @PreDestroy
    public void shutdownFiles() {
        log.info("Shutting down file inputs");
        inputs.forEach(S3FileInput::close);
        log.info("File inputs shut down");

        log.info("Shutting down file outputs");
        outputs.forEach(S3FileOutput::close);
        log.info("File outputs shut down");
    }

    private List<S3FileInput<?>> createInputPorts(MinioClient client) {
        var registry = (BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

        return config.inputsOrEmpty().stream()
                .map(cfg -> {
                    log.info("Bootstrapping file input for bucket: {}", cfg.bucket());

                    var resolvableType = ResolvableType
                            .forClassWithGenerics(FileInputPort.class, cfg.payloadType());

                    var beanDef = new RootBeanDefinition(S3FileInput.class);
                    beanDef.setTargetType(resolvableType);
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(client);
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.bucket());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.incomingPrefix());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.handledPrefix());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.errorPrefix());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.pollInterval());

                    var beanName = "fileInput:" + cfg.payloadType().getName();
                    registry.registerBeanDefinition(beanName, beanDef);

                    return (S3FileInput<?>) context.getBean(beanName);
                })
                .collect(Collectors.toUnmodifiableList());
    }

    private List<S3FileOutput<?>> createOutputPorts(MinioClient client) {
        var registry = (BeanDefinitionRegistry) context.getAutowireCapableBeanFactory();

        return config.outputsOrEmpty().stream()
                .map(cfg -> {
                    log.info("Bootstrapping file output for payloadType: {}", cfg.payloadType().getName());

                    var resolvableType = ResolvableType
                            .forClassWithGenerics(FileOutputPort.class, cfg.payloadType());

                    var beanDef = new RootBeanDefinition(S3FileOutput.class);
                    beanDef.setTargetType(resolvableType);
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(cfg.bucket());
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(client);
                    beanDef.getConstructorArgumentValues().addGenericArgumentValue(objectMapper);

                    var beanName = "fileOutput:" + cfg.payloadType().getName();
                    registry.registerBeanDefinition(beanName, beanDef);

                    return (S3FileOutput<?>) context.getBean(beanName);
                })
                .collect(Collectors.toUnmodifiableList());
    }
}