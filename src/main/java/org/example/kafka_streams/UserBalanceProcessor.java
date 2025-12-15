package org.example.kafka_streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafka_streams.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserBalanceProcessor {

    private final SpecificAvroSerde<User> userAvroSerde;

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        KStream<String, User> inputStream = streamsBuilder
                .stream("users-input",
                        Consumed.with(Serdes.String(), userAvroSerde));

        log.info("Configured Kafka Streams pipeline for user processing");

        inputStream.peek((key, user) ->
                log.info("Received user: {}, balance: {}", user.getName(), user.getBalance()));

        BranchedKStream<String, User> branched = inputStream.split(Named.as("branch-"));

        branched.branch(
                (key, user) -> user.getBalance() > 0.0,
                Branched.withConsumer(this::processPositiveBalance)
        );

        branched.branch(
                (key, user) -> user.getBalance() <= 0.0,
                Branched.withConsumer(this::processNonPositiveBalance)
        );
    }

    private void processPositiveBalance(KStream<String, User> positiveStream) {
        positiveStream
                .mapValues(user -> {
                    log.info("Positive balance user processed: {} (balance: {})",
                            user.getName(), user.getBalance());
                    return user;
                })
                .to("users-output", Produced.with(Serdes.String(), userAvroSerde));
    }

    private void processNonPositiveBalance(KStream<String, User> nonPositiveStream) {
        nonPositiveStream
                .mapValues(user -> {
                    String message = String.format(
                            "User %s has non-positive balance: %s (phone: %s)",
                            user.getName(),
                            user.getBalance(),
                            user.getPhone()
                    );
                    log.warn(message);
                    return user;
                })
                .to("users-error", Produced.with(Serdes.String(), userAvroSerde));
    }
}