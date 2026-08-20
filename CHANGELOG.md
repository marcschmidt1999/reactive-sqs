# Changelog

This project will use Semantic Versioning after its first public release.

## Unreleased

### Added

- Java 21 Gradle build with four modules.
- Spring Boot 3 / Jackson 2 and Spring Boot 4 / Jackson 3 starters.
- `@ReactiveSqsListener` with typed payloads and `SqsMessage<T>`.
- Bounded receive capacity and automatic visibility renewal.
- Delete batching, processing deadlines, retries, and Spring shutdown support.
- Standard queue support. FIFO queues are rejected.
- Optional Micrometer metrics with bounded tags.
- A Spring Boot demo with a real SQS queue helper, Prometheus endpoint, and performance report.
- Formatting, tests, SpotBugs, JaCoCo, dependency locking, and reproducible build checks.
