package org.rancidcode.telemetryaggregator.adaptor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RawTelemetryLogger {

    @KafkaListener(topics = "${kafka.topic.raw}", groupId = "${kafka.group.raw-logger}")
    public void rawProcess(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);
    }
}