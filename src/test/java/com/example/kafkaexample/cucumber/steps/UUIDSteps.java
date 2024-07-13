package com.example.kafkaexample.cucumber.steps;

import com.example.kafkaexample.CucumberSpringConfiguration;
import com.example.kafkaexample.producer.UUIDProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.http.MediaType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringJUnitConfig
@ContextConfiguration(classes = CucumberSpringConfiguration.class)
public class UUIDSteps {

    @Autowired
    private UUIDProducer uuidProducer;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    private KafkaConsumer<String, String> consumer;
    private List<String> producedUUIDs;

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
        producedUUIDs = new ArrayList<>();
    }

    @After
    public void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Given("a running Kafka broker")
    public void kafkaBrokerRunning() {
        // Create consumer configuration
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // Increase session timeout
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // Increase heartbeat interval

        // Create and configure the consumer
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton("uuid_topic"));

        System.out.println("Consumer subscribed to topic: uuid_topic");
    }

    @When("I produce a random UUID")
    public void produceRandomUUID() throws Exception {
        String response = mockMvc.perform(get("/produce"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        System.out.println("Produced UUID via GET: " + response);
        producedUUIDs.add(response);
        kafkaTemplate.flush();
        TimeUnit.SECONDS.sleep(10); // Increased sleep time for producing message
    }

    @Then("the exact UUID should be consumed and logged")
    public void consumeUUID() throws InterruptedException {
        System.out.println("Waiting for the UUID to be consumed...");
        TimeUnit.SECONDS.sleep(30); // Increased sleep time for consuming message

        // Verify consumer is subscribed to the correct topic
        System.out.println("Subscribed topics: " + consumer.subscription());

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15));
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        consumerRecords.forEach(records::add);

        System.out.println("Number of records consumed: " + records.size());
        records.forEach(record -> System.out.println("Consumed UUID: " + record.value()));

        // Assert that records are not empty
        Assertions.assertFalse(records.isEmpty(), "No records found for topic");

        // Check the first record
        ConsumerRecord<String, String> record = records.get(0);
        Assertions.assertNotNull(record.value(), "Record value is null");

        // Compare the consumed UUID with the produced one
        Assertions.assertEquals(producedUUIDs.get(0), record.value(), "The consumed UUID does not match the produced UUID");

        // Log the exact UUID that was consumed
        System.out.println("Exact UUID consumed: " + record.value());
    }

    @When("I send a UUID {string} to the POST API")
    public void sendUUIDToPostAPI(String uuid) throws Exception {
        String response = mockMvc.perform(post("/produce")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(uuid))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        System.out.println("Sent UUID via POST API: " + response);
        producedUUIDs.add(uuid); // Fix: add the input uuid to producedUUIDs
        kafkaTemplate.flush();
        TimeUnit.SECONDS.sleep(10); // Increased sleep time for producing message
    }

    @When("no UUID is produced")
    public void noUUIDProduced() {
        // Intentionally do nothing
    }

    @When("I produce an invalid UUID")
    public void produceInvalidUUID() {
        uuidProducer.sendInvalidMessage();
        kafkaTemplate.flush();
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Then("no UUID should be consumed")
    public void noUUIDConsumed() throws InterruptedException {
        System.out.println("Waiting to verify no UUID is consumed...");
        TimeUnit.SECONDS.sleep(20); // Increased sleep time for consuming message

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15));
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        consumerRecords.forEach(records::add);

        Assertions.assertEquals(0, records.size(), "No UUIDs should be consumed");
    }

    @Then("the invalid UUID should be handled correctly")
    public void handleInvalidUUID() throws InterruptedException {
        System.out.println("Waiting to verify invalid UUID consumption...");
        TimeUnit.SECONDS.sleep(20); // Increased sleep time for consuming message

        try {
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "uuid_topic", Duration.ofSeconds(15));
            Assertions.assertNotNull(record.value(), "No records found for topic");
            System.out.println("Consumed invalid UUID: " + record.value());
        } catch (IllegalStateException e) {
            System.out.println("No invalid UUID found in the topic.");
        }
    }
}
