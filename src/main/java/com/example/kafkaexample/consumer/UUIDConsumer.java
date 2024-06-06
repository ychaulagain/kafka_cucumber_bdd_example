package com.example.kafkaexample.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UUIDConsumer {

    private static final Logger logger = LoggerFactory.getLogger(UUIDConsumer.class);

    @KafkaListener(topics = "uuid_topic", groupId = "group_id")
    public void consume(String message) {
        logger.info("Consumed UUID: {}", message);
    }
}
