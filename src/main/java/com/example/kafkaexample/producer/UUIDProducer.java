package com.example.kafkaexample.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class UUIDProducer {

    private static final Logger logger = LoggerFactory.getLogger(UUIDProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    public UUIDProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String sendMessage() {
        String uuid = UUID.randomUUID().toString();
        logger.info("Producing UUID: {}", uuid);
        kafkaTemplate.send("uuid_topic", uuid);
        return uuid;
    }
}
