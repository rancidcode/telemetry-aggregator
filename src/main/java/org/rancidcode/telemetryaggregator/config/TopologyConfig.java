package org.rancidcode.telemetryaggregator.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TopologyConfig {

    @Bean
    public KGroupedStream<String, String> stream(StreamsBuilder builder) {

        return builder.stream("telemetry.raw", Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> key != null ? key : "deviceId")
                .groupByKey();
    }
}