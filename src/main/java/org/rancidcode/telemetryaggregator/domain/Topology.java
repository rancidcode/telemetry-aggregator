package org.rancidcode.telemetryaggregator.domain;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;

@Slf4j
public class Topology {

    private final ObjectMapper MAPPER = new ObjectMapper();

    public void build(KStream<String, String> input) {
        KGroupedStream<String, String> grouped = input
                .selectKey((key, value) -> key != null ? key : "default-key")
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        aggregateAvg(grouped, 1);
        aggregateAvg(grouped, 2);
    }

    private void aggregate(KGroupedStream<String, String> grouped, int minutes) {
        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(minutes)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("device-counts-" + minutes + "m").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .toStream()
                .peek((key, value) -> log.info("Processing -> key={}, value={}, window={}m", key, value, minutes))
                .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), String.valueOf(count)))
                .to("telemetry.counts." + minutes + "m", Produced.with(Serdes.String(), Serdes.String()));
    }

    private void aggregateAvg(KGroupedStream<String, String> grouped, int minutes) {

        JacksonJsonSerde<AvgStats> avgStatsSerde = new JacksonJsonSerde<>(AvgStats.class);

        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(minutes)))
                .aggregate(
                        AvgStats::new,
                        (key, v, stats) -> {
                            try {
                                stats.parseJSON(v);
                            } catch (Exception e) {
                                log.warn("Failed to parse telemetry payload: {}", v, e);
                            }
                            return stats;
                        },
                        Materialized.<String, AvgStats, WindowStore<Bytes, byte[]>>as("device-avg-" + minutes + "m")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(avgStatsSerde)
                )
                .toStream()
                .map((windowedKey, value) ->
                {
                    try {
                        return KeyValue.pair(windowedKey.key(), MAPPER.writeValueAsString(value));
                    } catch (Exception e) {
                        log.error("Failed to serialize AvgStats", e);
                        return KeyValue.pair(windowedKey.key(), "{}");
                    }
                })
                .peek((key, value) -> log.info("Processing -> key={}, value={}, window={}m", key, value, minutes))
                .to("telemetry.avg." + minutes + "m", Produced.with(Serdes.String(), Serdes.String()));
    }
}
