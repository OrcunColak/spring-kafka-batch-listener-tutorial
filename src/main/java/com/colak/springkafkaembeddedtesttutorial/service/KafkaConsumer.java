package com.colak.springkafkaembeddedtesttutorial.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component

@Getter
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "topic2" , groupId = "group2", batch = "true")
    public void receive(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        try {
            log.error("List size is : {}", records.size());
            for (ConsumerRecord<String, String> consumerRecord : records) {
                log.error("Value is : {}", consumerRecord.value());
            }
        } finally {
            acknowledgment.acknowledge();

        }

    }
}
