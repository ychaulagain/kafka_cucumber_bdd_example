Feature: UUID Kafka Integration

  Scenario: Produce and consume exact UUID
    Given a running Kafka broker
    When I produce a random UUID
    Then the exact UUID should be consumed and logged
