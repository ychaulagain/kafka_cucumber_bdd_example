package com.example.kafkaexample.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class UUIDProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "uuid_topic";

    public String sendMessage() {
        String uuid = UUID.randomUUID().toString();
        kafkaTemplate.send(TOPIC, uuid);
        System.out.println("Message sent: " + uuid);
        return uuid;
    }

    public String sendMessage(String uuid) {
        kafkaTemplate.send(TOPIC, uuid);
        System.out.println("Message sent: " + uuid);
        return uuid;
    }

    public void sendInvalidMessage() {
        String invalidUUID = "invalid-uuid";
        kafkaTemplate.send(TOPIC, invalidUUID);
        System.out.println("Invalid message sent: " + invalidUUID);
    }
}
