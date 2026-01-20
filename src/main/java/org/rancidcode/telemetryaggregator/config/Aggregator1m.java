package org.rancidcode.telemetryaggregator.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class Aggregator1m {

    @Autowired
    public Aggregator1m(KGroupedStream<String, String> gstream) {
        gstream.windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.as("device-counts-1m"))
                .toStream()
                .peek((key, value) -> System.out.println("Processing -> Key: " + key.key() + " Count: " + value))
                .map((windowedKey, count) -> org.apache.kafka.streams.KeyValue.pair(windowedKey.key(), String.valueOf(count)))
                .to("telemetry.counts.1m", Produced.with(Serdes.String(), Serdes.String()));
    }
}
