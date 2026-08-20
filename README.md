# Reactive SQS

Reactive SQS is a Spring library for consuming standard Amazon SQS queues with Reactor.

It supports Java 21, Spring Boot 3, Spring Boot 4, typed JSON payloads, visibility renewal,
bounded concurrency, batched deletes, and Micrometer metrics.

Status: `0.1.0-SNAPSHOT`. It is not released for production use yet.

## Add a starter

Use the starter for your Spring Boot version:

```kotlin
dependencies {
    implementation("io.github.marcschmidt1999:reactive-sqs-spring-boot-3-starter:0.1.0-SNAPSHOT")

    // For Spring Boot 4:
    // implementation("io.github.marcschmidt1999:reactive-sqs-spring-boot-4-starter:0.1.0-SNAPSHOT")
}
```

The starter uses the Reactor and Spring versions managed by your Boot BOM. If you use
`reactive-sqs-core` or `reactive-sqs-spring` directly, add compatible Reactor and Spring
dependencies to your application.

Create the `SqsAsyncClient` in your application. The library uses it but does not close it.

```java
@Bean
SqsAsyncClient sqsAsyncClient() {
    return SqsAsyncClient.builder()
            .region(Region.EU_CENTRAL_1)
            .overrideConfiguration(configuration -> configuration
                    .apiCallAttemptTimeout(Duration.ofSeconds(25))
                    .apiCallTimeout(Duration.ofSeconds(30))
                    .retryStrategy(StandardRetryStrategy.builder().maxAttempts(3).build()))
            .build();
}
```

The attempt timeout must be longer than the 20-second SQS long poll.

## Add a listener

Use the payload type directly:

```java
@Component
final class OrderListener {

    @ReactiveSqsListener(queue = "${app.queues.orders}", maxInFlight = 8)
    Mono<Void> onOrderCreated(OrderCreated event) {
        return orderProcessor.process(event);
    }
}
```

Use `SqsMessage<T>` when you also need SQS metadata:

```java
@ReactiveSqsListener(queue = "${app.queues.orders}")
Mono<Void> onOrderCreated(SqsMessage<OrderCreated> message) {
    return orderProcessor.process(message.payload());
}
```

The handler must return `Mono<Void>` and should not block.

## What happens to a message

| Result | Library action |
| --- | --- |
| Handler completes | Add the receipt handle to a small delete batch |
| Handler fails | Do not delete the message |
| JSON cannot be mapped | Do not call the handler or delete the message |
| Processing or visibility time expires | Cancel the work and do not delete the message |
| Delete fails | Do not treat the message as acknowledged |
| Shutdown grace expires | Cancel unfinished work and do not delete it |

SQS is at least once. Handlers must be idempotent.

## Listener options

| Option | Default | Description |
| --- | ---: | --- |
| `queue` | required | Queue URL or Spring placeholder |
| `maxInFlight` | 1 | Maximum received but not settled messages |
| `visibilityTimeoutSeconds` | 60 | SQS visibility timeout |
| `maxProcessingDurationSeconds` | 3600 | Maximum handler time |
| `shutdownGraceSeconds` | 30 | Time allowed for work to finish during shutdown |

The listener keeps visibility alive while it processes a message or waits for its delete request.
It runs one long poll at a time and requests no more messages than free capacity allows.

Disable all listeners:

```yaml
reactive-sqs:
  enabled: false
```

## Metrics

Metrics are on by default when the application provides a Micrometer `MeterRegistry`. Add your
normal Actuator and registry dependency; the starters do not choose one for you.

Disable only the library metrics:

```yaml
reactive-sqs:
  metrics:
    enabled: false
```

Main meters:

- `reactive.sqs.messages.received`
- `reactive.sqs.delivery`
- `reactive.sqs.receive`
- `reactive.sqs.processing`
- `reactive.sqs.delete`
- `reactive.sqs.delete.batch.requests`
- `reactive.sqs.delete.batch.entries`
- `reactive.sqs.listener.active`
- `reactive.sqs.listener.inflight`
- `reactive.sqs.listener.running`
- `reactive.sqs.listener.failed`

Metrics use bounded listener and outcome tags. They never include message IDs, receipt handles,
payloads, queue URLs, account IDs, or exception text.

## Scope

Supported:

- Java 21
- AWS SDK for Java 2.x
- Spring Boot 3.5 / Jackson 2
- Spring Boot 4 / Jackson 3
- Standard SQS queues

Not supported yet:

- FIFO queues
- SNS envelope handling
- Manual acknowledgements
- Queue or DLQ management
- Exactly-once processing

## Modules

- `reactive-sqs-core`: SQS polling, visibility, retries, deletes, and shutdown.
- `reactive-sqs-spring`: annotations and Spring lifecycle support.
- `reactive-sqs-spring-boot-3-starter`: Boot 3 and Jackson 2 setup.
- `reactive-sqs-spring-boot-4-starter`: Boot 4 and Jackson 3 setup.

## Build

```shell
./run.sh format
./run.sh check
./run.sh build
./run.sh versions
./run.sh publish-local
```

Each module must keep at least 75% line coverage. Reports are written to
`build/reports/jacoco/test/html/index.html`.

Create a local release tag with `./run.sh tag 0.1.0`. The command requires a clean Git working
tree and does not push the tag.

See [CONTRIBUTING.md](CONTRIBUTING.md) and [the demo](samples/reactive-sqs-demo/README.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
