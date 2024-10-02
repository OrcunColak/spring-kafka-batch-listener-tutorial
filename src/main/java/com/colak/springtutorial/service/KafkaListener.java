package com.colak.springtutorial.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component

@Getter
@Slf4j
public class KafkaListener {

    @org.springframework.kafka.annotation.KafkaListener(topics = "topic1", groupId = "group1", batch = "true")
    public void receive(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment acknowledgment) {
        log.info("List size : {}", consumerRecords.size());

        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            log.info("Values : {}", consumerRecord.value());
        }

        acknowledgment.acknowledge();
    }
}
