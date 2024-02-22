package com.colak.springkafkaembeddedtesttutorial.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

@Slf4j
public class KafkaBatchMessageListener implements BatchAcknowledgingMessageListener<String, String> {


    @Override
    public void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        try {
            log.error("List size is : {}", records.size());
            for (ConsumerRecord<String, String> consumerRecord : records) {
                log.error("Value is : {}", consumerRecord.value());
            }
        } catch (Exception exception) {
            log.error("Execute condition process error", exception);
        } finally {
            acknowledgment.acknowledge();
        }
    }
}
