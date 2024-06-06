package com.example.kafkaexample.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class UUIDProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public UUIDProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() {
        String uuid = UUID.randomUUID().toString();
        kafkaTemplate.send("uuid_topic", uuid);
    }
}
