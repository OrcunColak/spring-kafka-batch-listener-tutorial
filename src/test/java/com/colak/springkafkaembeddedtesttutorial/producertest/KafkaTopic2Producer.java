package com.colak.springkafkaembeddedtesttutorial.producertest;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaTopic2Producer {

    @Autowired
    private KafkaProducer producer;

    @Test
    void sendMessage() {
        for (int index = 0; index < 100; index++) {
            producer.send("topic2", "alo " + index);
        }
    }
}
