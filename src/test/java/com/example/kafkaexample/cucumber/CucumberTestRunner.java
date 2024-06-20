package com.example.kafkaexample.cucumber;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        features = "src/test/resources/features",
        glue = "com.example.kafkaexample.cucumber.steps",
        plugin = {"pretty", "html:target/cucumber-html-report.html"}
)
public class CucumberTestRunner {
}
