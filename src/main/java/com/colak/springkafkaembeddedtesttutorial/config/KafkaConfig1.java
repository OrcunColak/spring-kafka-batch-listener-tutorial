package com.colak.springkafkaembeddedtesttutorial.config;

import com.colak.springkafkaembeddedtesttutorial.service.KafkaBatchMessageListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig1 {

    ContainerProperties getContainerProps(String topics) {
        ContainerProperties containerProps = new ContainerProperties(topics.split(","));
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        KafkaBatchMessageListener kafkaBatchMessageListener = new KafkaBatchMessageListener();
        containerProps.setMessageListener(kafkaBatchMessageListener);
        containerProps.setPollTimeout(5_000L);
        return containerProps;
    }

    protected Map<String, Object> getConsumerConfigs() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return propsMap;
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, Object> kafkaAckMessageListenerContainer1() {
        ConsumerFactory<String, Object> factory = new DefaultKafkaConsumerFactory<>(getConsumerConfigs());
        // String topics = "topic1,topic2,topic3";
        String topics = "topic1";
        ConcurrentMessageListenerContainer<String, Object> container = new ConcurrentMessageListenerContainer<>(
                factory,
                getContainerProps(topics)
        );
        container.setBeanName("kafkaAckMessageListenerContainer1");
        container.setConcurrency(2); // Kafka listener container will be configured to use 2 threads for message processing
        container.setAutoStartup(true);
        return container;
    }
}
