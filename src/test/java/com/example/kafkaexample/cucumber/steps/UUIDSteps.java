package com.example.kafkaexample.cucumber.steps;

import com.example.kafkaexample.CucumberSpringConfiguration;
import com.example.kafkaexample.producer.UUIDProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringJUnitConfig
@ContextConfiguration(classes = CucumberSpringConfiguration.class)
public class UUIDSteps {

    @Autowired
    private UUIDProducer uuidProducer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaConsumer<String, String> consumer;

    @Given("a running Kafka broker")
    public void kafkaBrokerRunning() {
        // Create consumer configuration
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create and configure the consumer
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton("uuid_topic"));
    }

    @When("I produce a random UUID")
    public void produceRandomUUID() {
        uuidProducer.sendMessage();
    }

    @Then("the UUID should be consumed and logged")
    public void consumeUUID() throws InterruptedException {
        // Add delay to ensure the message is consumed
        TimeUnit.SECONDS.sleep(2);

        // Consume the message
        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "uuid_topic");
        assertNotNull(record.value(), "No records found for topic");
    }
}
