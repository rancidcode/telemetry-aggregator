package org.rancidcode.telemetryaggregator.infra;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.rancidcode.telemetryaggregator.domain.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.application.name}")
    private String applicationId;

    @Value(value = "${kafka.topic.raw}")
    private String rawTopic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KafkaStreams.StateListener streamsStateLogger(StreamsBuilderFactoryBean fb) {
        fb.setKafkaStreamsCustomizer(kafkaStreams ->
                kafkaStreams.setStateListener((newState, oldState) ->
                        log.info("KafkaStreams state: {} -> {}", oldState, newState)
                )
        );
        return null;
    }

    @Bean
    public Topology topology() {
        return new Topology();
    }

    @Bean
    public KStream<String, String> stream(StreamsBuilder builder, Topology topology) {

        KStream<String, String> input = builder.stream(rawTopic, Consumed.with(Serdes.String(), Serdes.String()));
        topology.build(input);

        return input;
    }
}