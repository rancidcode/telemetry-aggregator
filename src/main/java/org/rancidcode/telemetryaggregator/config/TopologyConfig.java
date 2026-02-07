package org.rancidcode.telemetryaggregator.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

@Slf4j
@Configuration
public class TopologyConfig {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Value(value = "${kafka.topic.raw}")
    private String rawTopic;

    @Bean
    public KStream<String, String> stream(StreamsBuilder builder) {

        KStream<String, String> input = builder.stream(rawTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KGroupedStream<String, String> grouped = input.selectKey((key, value) -> key != null ? key : "default-key")
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        aggregateAvg(grouped, 1);
        aggregateAvg(grouped, 2);

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

    private void aggregateAvg(KGroupedStream<String, String> grouped, int minutes) {

        JacksonJsonSerde<AvgStats> avgStatsSerde = new JacksonJsonSerde<>(AvgStats.class);

        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(minutes)))
                .aggregate(
                        AvgStats::new,
                        (key, v, stats) -> {
                            stats.parseJSON(v);
                            return stats;
                        },
                        Materialized.<String, AvgStats, WindowStore<Bytes, byte[]>>as("device-avg-" + minutes + "m")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(avgStatsSerde)
                )
                .toStream()
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), MAPPER.writeValueAsString(value)))
                .peek((key, value) -> System.out.println("Processing -> " + "value: " + value + " Duration: " + minutes))
                .to("telemetry.avg." + minutes + "m", Produced.with(Serdes.String(), Serdes.String()));
    }

}
