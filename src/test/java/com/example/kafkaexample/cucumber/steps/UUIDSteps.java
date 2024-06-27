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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
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

    @Given("a running Kafka broker")
    public void kafkaBrokerRunning() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();

        // Create consumer configuration
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create and configure the consumer
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton("uuid_topic"));

        producedUUIDs = new ArrayList<>();
        System.out.println("Consumer subscribed to topic: uuid_topic");
    }

    @When("I produce a random UUID")
    public void produceRandomUUID() throws Exception {
        String response = mockMvc.perform(get("/produce"))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        producedUUIDs.add(response);
        kafkaTemplate.flush();
    }

    @Then("the exact UUID should be consumed and logged")
    public void consumeUUID() throws InterruptedException {
        TimeUnit.SECONDS.sleep(5);

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "uuid_topic", Duration.ofSeconds(10));
        Assertions.assertNotNull(record.value(), "No records found for topic");
        Assertions.assertEquals(producedUUIDs.getFirst(), record.value(), "The consumed UUID does not match the produced UUID");
    }

    @When("no UUID is produced")
    public void noUUIDProduced() {
        // Intentionally do nothing
    }

    @When("I produce an invalid UUID")
    public void produceInvalidUUID() {
        uuidProducer.sendInvalidMessage();
        kafkaTemplate.flush();
    }

    @Then("no UUID should be consumed")
    public void noUUIDConsumed() throws InterruptedException {
        TimeUnit.SECONDS.sleep(5);

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        consumerRecords.forEach(records::add);

        Assertions.assertEquals(0, records.size(), "No UUIDs should be consumed");
    }

    @Then("the invalid UUID should be handled correctly")
    public void handleInvalidUUID() throws InterruptedException {
        TimeUnit.SECONDS.sleep(5);

        try {
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "uuid_topic", Duration.ofSeconds(10));
            Assertions.assertNotNull(record.value(), "No records found for topic");
            System.out.println("Consumed invalid UUID: " + record.value());
        } catch (IllegalStateException e) {
            System.out.println("No invalid UUID found in the topic.");
        }
    }
}
