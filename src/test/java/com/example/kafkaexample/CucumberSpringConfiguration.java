package com.example.kafkaexample;

import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@CucumberContextConfiguration
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"uuid_topic"})
public class CucumberSpringConfiguration {
}
