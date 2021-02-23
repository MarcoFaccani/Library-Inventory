# Library-Inventory

This is a Spring Boot project focused on Apache Kafka.
It includes a microservice that produces messages on a topic and a second application that consumes those messages and persists them in a H2 in-memory database using Hibernate and JPA.

They key points in this project are:
- Kafka unit tests
- Kafka integration tests
- Kafka error handling (retries for specific exceptions or for any exception, custom recovery logic)
- Kafka configuration
- Manual acknowledgment
- Producing and consuming messages


