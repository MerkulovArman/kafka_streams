package org.example.kafka_streams;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.kafka_streams.model.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@KafkaStreamsIntegrationTest
class UserBalanceProcessorIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaTemplate<String, User> kafkaTemplate;
    private org.apache.kafka.clients.consumer.Consumer<String, User> outputConsumer;
    private org.apache.kafka.clients.consumer.Consumer<String, User> errorConsumer;
    private SchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    void setUp() {
        assertThat(embeddedKafka).isNotNull();

        schemaRegistryClient = new MockSchemaRegistryClient();

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", "mock://test-url");
        producerProps.put("auto.register.schemas", "true");
        producerProps.put("specific.avro.reader", "true");
        producerProps.put("schema.registry.client", schemaRegistryClient);

        DefaultKafkaProducerFactory<String, User> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put("schema.registry.url", "mock://test-url");
        consumerProps.put("specific.avro.reader", "true");
        consumerProps.put("schema.registry.client", schemaRegistryClient);

        DefaultKafkaConsumerFactory<String, User> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        outputConsumer = consumerFactory.createConsumer();
        errorConsumer = consumerFactory.createConsumer();

        outputConsumer.subscribe(Collections.singletonList("users-output"));
        errorConsumer.subscribe(Collections.singletonList("users-error"));
    }

    @Test
    void whenPositiveBalanceUserSent_thenGoesToOutputTopic() throws ExecutionException, InterruptedException {
        // Given
        User user = new User("Alice", "+79161234567", 1500.75);

        // When
        kafkaTemplate.send("users-input", user.getName(), user).get();

        // Then
        await().atMost(java.time.Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    ConsumerRecord<String, User> record = KafkaTestUtils.getSingleRecord(outputConsumer, "users-output");
                    assertThat(record).isNotNull();
                    assertThat(record.value().getName()).isEqualTo("Alice");
                    assertThat(record.value().getBalance()).isEqualTo(1500.75);
                });
    }

    @Test
    void whenNegativeBalanceUserSent_thenGoesToErrorTopic() throws ExecutionException, InterruptedException {
        // Given
        User user = new User("Bob", "+79039876543", -500.0);

        // When
        kafkaTemplate.send("users-input", user.getName(), user).get();

        // Then
        await().atMost(java.time.Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    ConsumerRecord<String, User> record = KafkaTestUtils.getSingleRecord(errorConsumer, "users-error");
                    assertThat(record).isNotNull();
                    assertThat(record.value().getName()).isEqualTo("Bob");
                    assertThat(record.value().getBalance()).isEqualTo(-500.0);
                });
    }
}