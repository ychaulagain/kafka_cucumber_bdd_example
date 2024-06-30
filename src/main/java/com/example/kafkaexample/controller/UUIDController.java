package com.example.kafkaexample.controller;

import com.example.kafkaexample.producer.UUIDProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/produce")
public class UUIDController {

    @Autowired
    private UUIDProducer uuidProducer;

    @GetMapping
    public String produceRandomUUID() {
        return uuidProducer.sendMessage();
    }

    @PostMapping
    public String produceUUID(@RequestBody String uuid) {
        System.out.println("Received UUID via POST: " + uuid);
        return uuidProducer.sendMessage(uuid);
    }
}
