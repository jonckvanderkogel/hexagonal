package com.bullit.application.streaming;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.UUID;

import static com.bullit.application.TestUtils.anyMessageContains;
import static com.bullit.application.TestUtils.assertNoViolations;
import static com.bullit.application.TestUtils.validate;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaClientPropertiesTest {

    @Test
    void validProperties_haveNoViolations() {
        var props = new KafkaClientProperties("localhost:9092", "http://localhost:8081");

        assertNoViolations(validate(props));
    }

    @Test
    void bootstrapServers_isRequired() {
        var props = new KafkaClientProperties("", "http://localhost:8081");

        var violations = validate(props);

        anyMessageContains(violations, "kafka.bootstrap-servers is required");
    }

    @Test
    void schemaRegistryUrl_isRequired() {
        var props = new KafkaClientProperties("localhost:9092", "");

        var violations = validate(props);

        anyMessageContains(violations, "kafka.schema-registry-url is required");
    }

    @Test
    void buildConsumerProperties_setsExpectedDefaults() {
        var kafka = new KafkaClientProperties("localhost:9092", "http://localhost:8081");
        var groupId = "test-" + UUID.randomUUID();

        Properties props = kafka.buildConsumerProperties(groupId, 1000);

        assertThat(props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
                .isEqualTo("localhost:9092");
        assertThat(props.getProperty(ConsumerConfig.GROUP_ID_CONFIG))
                .isEqualTo(groupId);

        assertThat(props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringDeserializer.class.getName());
        assertThat(props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG))
                .isEqualTo(KafkaAvroDeserializer.class.getName());

        assertThat(props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
                .isEqualTo("earliest");
        assertThat(props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                .isEqualTo("false");
        assertThat(props.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))
                .isEqualTo("500");
        assertThat(props.getProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG))
                .isEqualTo("300000");

        assertThat(props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))
                .isEqualTo("http://localhost:8081");

        assertThat(props.get(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG))
                .isEqualTo(true);
    }

    @Test
    void buildProducerProperties_setsExpectedDefaults() {
        var kafka = new KafkaClientProperties("localhost:9092", "http://localhost:8081");

        Properties props = kafka.buildProducerProperties();

        assertThat(props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                .isEqualTo("localhost:9092");

        assertThat(props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringSerializer.class.getName());
        assertThat(props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(KafkaAvroSerializer.class.getName());

        assertThat(props.getProperty(ProducerConfig.ACKS_CONFIG))
                .isEqualTo("all");
        assertThat(props.getProperty(ProducerConfig.RETRIES_CONFIG))
                .isEqualTo("10");
        assertThat(props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))
                .isEqualTo("true");
        assertThat(props.getProperty(ProducerConfig.LINGER_MS_CONFIG))
                .isEqualTo("5");
        assertThat(props.getProperty(ProducerConfig.BATCH_SIZE_CONFIG))
                .isEqualTo("32768");
        assertThat(props.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION))
                .isEqualTo("5");

        assertThat(props.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))
                .isEqualTo("http://localhost:8081");
    }
}