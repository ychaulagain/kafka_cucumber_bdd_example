Feature: UUID Kafka Integration

  Scenario: Produce and consume exact UUID
    Given a running Kafka broker
    When I produce a random UUID
    Then the exact UUID should be consumed and logged

  Scenario: Send UUID via POST API and consume
    Given a running Kafka broker
    When I send a UUID "123e4567-e89b-12d3-a456-426614174000" to the POST API
    Then the exact UUID should be consumed and logged

  Scenario: Handle no UUID production
    Given a running Kafka broker
    When no UUID is produced
    Then no UUID should be consumed

  Scenario: Handle invalid UUID
    Given a running Kafka broker
    When I produce an invalid UUID
    Then the invalid UUID should be handled correctly

