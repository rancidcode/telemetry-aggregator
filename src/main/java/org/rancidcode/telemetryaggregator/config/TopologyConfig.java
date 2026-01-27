package org.rancidcode.telemetryaggregator.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class TopologyConfig {

    @Bean
    public KStream<String, String> stream(StreamsBuilder builder) {

        KStream<String, String> input = builder.stream("telemetry.raw", Consumed.with(Serdes.String(), Serdes.String()));

        KGroupedStream<String, String> grouped = input.selectKey((key, value) -> key != null ? key : "default-key")
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        aggregate(grouped, 1);
        aggregate(grouped, 5);

        return input;
    }

    private void aggregate(KGroupedStream<String, String> grouped, int minutes) {
        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(minutes)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("device-counts-" + minutes + "m").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .toStream()
                .peek((key, value) -> System.out.println("Processing -> Key: " + key.key() + " Count: " + value + " Duration: " + minutes))
                .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), String.valueOf(count)))
                .to("telemetry.counts." + minutes + "m", Produced.with(Serdes.String(), Serdes.String()));
    }
}