Feature: UUID Kafka Integration

  Scenario: Produce and consume UUID
    Given a running Kafka broker
    When I produce a random UUID
    Then the UUID should be consumed and logged
