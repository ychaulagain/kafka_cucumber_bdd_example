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

        String uuid = uuidProducer.sendMessage();
        System.out.println("Produced UUID via GET: " + uuid);
        return uuid;
    }

    @PostMapping
    public String produceUUID(@RequestBody String uuid) {
        String sentUuid = uuidProducer.sendMessage(uuid);
        System.out.println("Produced UUID via POST: " + sentUuid);
        return sentUuid;
    }
}
