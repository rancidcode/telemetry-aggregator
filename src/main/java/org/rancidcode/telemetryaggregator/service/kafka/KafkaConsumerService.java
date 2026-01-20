package org.rancidcode.telemetryaggregator.service.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(topics = "${kafka.topic.raw}", groupId = "${kafka.group.raw}")
    public void rawProcess(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);
    }

    @KafkaListener(topics = "${kafka.topic.dlq}", groupId = "${kafka.group.dlq}")
    public void dlqProcess(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Topic : {}, message : {}", topic, message);
    }

}