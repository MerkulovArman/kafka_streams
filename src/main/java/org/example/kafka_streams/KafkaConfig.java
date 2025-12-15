package org.example.kafka_streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.example.kafka_streams.model.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-balance-processor-avro");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put("schema.registry.url", schemaRegistryUrl);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public NewTopic usersInputTopic() {
        return TopicBuilder.name("users-input")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic usersOutputTopic() {
        return TopicBuilder.name("users-output")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic usersErrorTopic() {
        return TopicBuilder.name("users-error")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public SpecificAvroSerde<User> userAvroSerde() {
        SpecificAvroSerde<User> serde = new SpecificAvroSerde<>();
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        serde.configure(config, false);
        return serde;
    }
}