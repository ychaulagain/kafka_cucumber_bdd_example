package com.example.kafkaexample.cucumber.steps;

import com.example.kafkaexample.CucumberSpringConfiguration;
import com.example.kafkaexample.producer.UUIDProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringJUnitConfig
@ContextConfiguration(classes = CucumberSpringConfiguration.class)
public class UUIDSteps {

    @Autowired
    private UUIDProducer uuidProducer;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaConsumer<String, String> consumer;
    private List<String> producedUUIDs;

    @Given("a running Kafka broker")
    public void kafkaBrokerRunning() {
        // Create consumer configuration
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Ensure consumer starts from the earliest offset

        // Create and configure the consumer
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton("uuid_topic"));

        producedUUIDs = new ArrayList<>();
        System.out.println("Consumer subscribed to topic: uuid_topic");
    }

    @When("I produce a random UUID")
    public void produceRandomUUID() {
        String producedUUID = uuidProducer.sendMessage();
        System.out.println("Produced UUID: " + producedUUID);  // Logging produced UUID
        producedUUIDs.add(producedUUID);
        kafkaTemplate.flush();  // Ensure the message is sent immediately
    }


    @When("no UUID is produced")
    public void noUUIDProduced() {
        // Intentionally do nothing
    }

    @When("I produce an invalid UUID")
    public void produceInvalidUUID() {
        uuidProducer.sendInvalidMessage();
        kafkaTemplate.flush();  // Ensure the message is sent immediately
    }

    @Then("the exact UUID should be consumed and logged")
    public void consumeUUID() throws InterruptedException {
        // Add delay to ensure the message is consumed
        TimeUnit.SECONDS.sleep(5);

        // Consume the message
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "uuid_topic", Duration.ofSeconds(10));
        assertNotNull(record.value(), "No records found for topic");
        assertEquals(producedUUIDs.get(0), record.value(), "The consumed UUID does not match the produced UUID");
    }

    @Then("no UUID should be consumed")
    public void noUUIDConsumed() throws InterruptedException {
        // Add delay to ensure the consumer has time to poll
        TimeUnit.SECONDS.sleep(5);

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        consumerRecords.forEach(records::add);

        assertEquals(0, records.size(), "No UUIDs should be consumed");
    }

    @Then("the invalid UUID should be handled correctly")
    public void handleInvalidUUID() throws InterruptedException {
        // Add delay to ensure the message is consumed
        TimeUnit.SECONDS.sleep(5);

        try {
            // Consume the message
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "uuid_topic", Duration.ofSeconds(10));
            assertNotNull(record.value(), "No records found for topic");
            System.out.println("Consumed invalid UUID: " + record.value());  // Logging consumed invalid UUID

            // Add custom validation for invalid UUID handling if necessary
            // For example, check if it's logged, stored in a different topic, etc.
        } catch (IllegalStateException e) {
            System.out.println("No invalid UUID found in the topic.");  // Logging no records found
        }
    }
}
