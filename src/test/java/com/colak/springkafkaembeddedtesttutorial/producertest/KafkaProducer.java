package com.colak.springkafkaembeddedtesttutorial.producertest;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component

@Getter
@Slf4j
@RequiredArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public boolean send(String topic, String payload) {
        log.info("sending payload='{}' to topic='{}'", payload, topic);

        boolean result = true;
        try {
            kafkaTemplate.send(topic, payload).get();
        } catch (Exception exception) {
            result = false;
        }
        return result;
    }
}
