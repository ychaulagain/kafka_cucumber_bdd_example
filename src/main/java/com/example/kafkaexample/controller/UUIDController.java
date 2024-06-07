package com.example.kafkaexample.controller;

import com.example.kafkaexample.producer.UUIDProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UUIDController {

    private final UUIDProducer uuidProducer;

    public UUIDController(UUIDProducer uuidProducer) {
        this.uuidProducer = uuidProducer;
    }

    @GetMapping("/produce")
    public String produce() {
        return uuidProducer.sendMessage();
    }
}
