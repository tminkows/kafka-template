# Kafka Template

A Spring Boot Kafka consumer template that demonstrates how to deserialize XML payloads into strongly typed objects generated from an XSD schema.

## Features

* Spring Boot 3.x application with a ready-to-use Kafka listener.
* Consumer group `my-cons-group` listening to the `my-topic` topic by default (configurable via `application.yml`).
* JAXB/XSD based schema-first model generation configured with the `maven-jaxb2-plugin`.
* XML message conversion using a custom `RecordMessageConverter` so that listeners receive typed objects.
* Centralized Kafka consumer configuration with sensible defaults, retry/backoff error handling and structured logging.
* Embedded Kafka based integration test that verifies message consumption end-to-end.

## Running locally

```bash
mvn spring-boot:run
```

The default Kafka bootstrap server is `localhost:19092`. Adjust `spring.kafka.bootstrap-servers` and `app.kafka.topic` in `src/main/resources/application.yml` to match your environment.

## Testing

```bash
mvn test
```

The integration test uses the embedded Kafka broker from `spring-kafka-test`, so no external broker is required.

## Schema

The XML schema that drives the JAXB model lives under `src/main/resources/schema/message.xsd`. The corresponding Java classes are generated into `target/generated-sources/jaxb` during the Maven build.
