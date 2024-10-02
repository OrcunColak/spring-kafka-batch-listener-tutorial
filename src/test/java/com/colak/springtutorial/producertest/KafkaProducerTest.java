package com.colak.springtutorial.producertest;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class KafkaProducerTest {

    @Autowired
    private KafkaProducer producer;

    @Test
    void sendMessage() {
        for (int index = 0; index < 100; index++) {
            boolean result = producer.send("topic1", "alo " + index);
            assertTrue(result);
        }
    }
}
