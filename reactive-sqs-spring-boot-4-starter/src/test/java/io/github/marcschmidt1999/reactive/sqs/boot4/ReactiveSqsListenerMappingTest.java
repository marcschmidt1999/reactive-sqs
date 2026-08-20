package io.github.marcschmidt1999.reactive.sqs.boot4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.marcschmidt1999.reactive.sqs.SqsMessage;
import io.github.marcschmidt1999.reactive.sqs.annotation.ReactiveSqsListener;
import io.github.marcschmidt1999.reactive.sqs.boot4.autoconfigure.ReactiveSqsAutoConfiguration;
import io.github.marcschmidt1999.reactive.sqs.boot4.autoconfigure.ReactiveSqsMetricsAutoConfiguration;
import io.github.marcschmidt1999.reactive.sqs.spring.ReactiveSqsListenerRegistrar;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import tools.jackson.databind.json.JsonMapper;

class ReactiveSqsListenerMappingTest {

    private static final String QUEUE_URL =
            "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

    @Test
    void annotatedListenerReceivesBodyMappedToItsParameterType() {
        var sqsClient = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ReactiveSqsAutoConfiguration.class))
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, () -> sqsClient)
                .withBean(JsonMapper.class, () -> JsonMapper.builder().build())
                .withBean(OrderListener.class, () -> listener)
                .run(
                        context ->
                                StepVerifier.create(listener.received.asMono())
                                        .expectNext(new OrderCreated("order-42"))
                                        .verifyComplete());
    }

    @Test
    void annotatedListenerMayReceiveMappedPayloadWithSqsMetadata() {
        var sqsClient = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new EnvelopeOrderListener();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ReactiveSqsAutoConfiguration.class))
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, () -> sqsClient)
                .withBean(JsonMapper.class, () -> JsonMapper.builder().build())
                .withBean(EnvelopeOrderListener.class, () -> listener)
                .run(
                        context ->
                                StepVerifier.create(listener.received.asMono())
                                        .assertNext(
                                                message -> {
                                                    assertThat(message.payload())
                                                            .isEqualTo(
                                                                    new OrderCreated("order-42"));
                                                    assertThat(message.messageId())
                                                            .isEqualTo("message-42");
                                                    assertThat(message.queueUrl())
                                                            .isEqualTo(QUEUE_URL);
                                                })
                                        .verifyComplete());
    }

    @Test
    void starterIsDiscoveredBySpringBootAutoConfiguration() {
        var sqsClient = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        new ApplicationContextRunner()
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, () -> sqsClient)
                .withBean(JsonMapper.class, () -> JsonMapper.builder().build())
                .withBean(OrderListener.class, () -> listener)
                .run(
                        context ->
                                StepVerifier.create(listener.received.asMono())
                                        .expectNext(new OrderCreated("order-42"))
                                        .expectComplete()
                                        .verify(Duration.ofSeconds(2)));
    }

    @Test
    void listenerRegistrationCanBeDisabledOperationally() {
        var sqsClient = sqsClientReturning("{\"orderId\":\"order-42\"}");

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ReactiveSqsAutoConfiguration.class))
                .withPropertyValues("test.queue-url=" + QUEUE_URL, "reactive-sqs.enabled=false")
                .withBean(SqsAsyncClient.class, () -> sqsClient)
                .withBean(JsonMapper.class, () -> JsonMapper.builder().build())
                .withBean(OrderListener.class, OrderListener::new)
                .run(
                        context -> {
                            assertThat(context).doesNotHaveBean(ReactiveSqsListenerRegistrar.class);
                            verify(sqsClient, never())
                                    .receiveMessage(any(ReceiveMessageRequest.class));
                        });
    }

    @Test
    void enabledMetricsRecordSuccessfulMessageProcessing() {
        var sqsClient = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        new ApplicationContextRunner()
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, () -> sqsClient)
                .withBean(JsonMapper.class, () -> JsonMapper.builder().build())
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean("ordersListener", OrderListener.class, () -> listener)
                .run(
                        context -> {
                            StepVerifier.create(listener.received.asMono())
                                    .expectNext(new OrderCreated("order-42"))
                                    .verifyComplete();
                            Mono.delay(Duration.ofMillis(50)).block();

                            var registry = context.getBean(SimpleMeterRegistry.class);
                            assertThat(
                                            registry.get("reactive.sqs.messages.received")
                                                    .tag("listener", "ordersListener.handle")
                                                    .counter()
                                                    .count())
                                    .isEqualTo(1.0);
                            assertThat(
                                            registry.get("reactive.sqs.delivery")
                                                    .tags(
                                                            "listener",
                                                            "ordersListener.handle",
                                                            "outcome",
                                                            "acknowledged")
                                                    .counter()
                                                    .count())
                                    .isEqualTo(1.0);
                            assertThat(
                                            registry.get("reactive.sqs.processing")
                                                    .tags(
                                                            "listener",
                                                            "ordersListener.handle",
                                                            "outcome",
                                                            "success")
                                                    .timer()
                                                    .count())
                                    .isEqualTo(1L);
                            assertThat(
                                            registry.get("reactive.sqs.delete.batch.requests")
                                                    .tags(
                                                            "listener",
                                                            "ordersListener.handle",
                                                            "outcome",
                                                            "success")
                                                    .counter()
                                                    .count())
                                    .isEqualTo(1.0);
                            assertThat(
                                            registry.get("reactive.sqs.delete.batch.entries")
                                                    .tag("listener", "ordersListener.handle")
                                                    .counter()
                                                    .count())
                                    .isEqualTo(1.0);
                        });
    }

    @Test
    void disabledMetricsRegisterNoReactiveSqsMeters() {
        var sqsClient = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        new ApplicationContextRunner()
                .withConfiguration(
                        AutoConfigurations.of(
                                ReactiveSqsMetricsAutoConfiguration.class,
                                ReactiveSqsAutoConfiguration.class))
                .withPropertyValues(
                        "test.queue-url=" + QUEUE_URL, "reactive-sqs.metrics.enabled=false")
                .withBean(SqsAsyncClient.class, () -> sqsClient)
                .withBean(JsonMapper.class, () -> JsonMapper.builder().build())
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean("ordersListener", OrderListener.class, () -> listener)
                .run(
                        context -> {
                            StepVerifier.create(listener.received.asMono())
                                    .expectNext(new OrderCreated("order-42"))
                                    .verifyComplete();

                            assertThat(context.getBean(SimpleMeterRegistry.class).getMeters())
                                    .noneMatch(
                                            meter ->
                                                    meter.getId()
                                                            .getName()
                                                            .startsWith("reactive.sqs."));
                        });
    }

    @Test
    void listenerDoesNotRequireMicrometerOnTheApplicationClasspath() {
        var sqsClient = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        new ApplicationContextRunner()
                .withClassLoader(new FilteredClassLoader("io.micrometer.core.instrument"))
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, () -> sqsClient)
                .withBean(JsonMapper.class, () -> JsonMapper.builder().build())
                .withBean(OrderListener.class, () -> listener)
                .run(
                        context ->
                                StepVerifier.create(listener.received.asMono())
                                        .expectNext(new OrderCreated("order-42"))
                                        .verifyComplete());
    }

    @Test
    void metricsClassifyPayloadMappingFailuresSeparately() {
        var sqsClient = sqsClientReturning("not-json");
        var listener = new InvocationCountingOrderListener();

        new ApplicationContextRunner()
                .withConfiguration(
                        AutoConfigurations.of(
                                ReactiveSqsMetricsAutoConfiguration.class,
                                ReactiveSqsAutoConfiguration.class))
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, () -> sqsClient)
                .withBean(JsonMapper.class, () -> JsonMapper.builder().build())
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean("ordersListener", InvocationCountingOrderListener.class, () -> listener)
                .run(
                        context -> {
                            assertThat(listener.invocations).hasValue(0);
                            verify(sqsClient, never())
                                    .deleteMessageBatch(any(DeleteMessageBatchRequest.class));
                            assertThat(
                                            context.getBean(SimpleMeterRegistry.class)
                                                    .get("reactive.sqs.processing")
                                                    .tags(
                                                            "listener",
                                                            "ordersListener.handle",
                                                            "outcome",
                                                            "mapping_error")
                                                    .timer()
                                                    .count())
                                    .isEqualTo(1L);
                            assertThat(
                                            context.getBean(SimpleMeterRegistry.class)
                                                    .get("reactive.sqs.delivery")
                                                    .tags(
                                                            "listener",
                                                            "ordersListener.handle",
                                                            "outcome",
                                                            "mapping_error")
                                                    .counter()
                                                    .count())
                                    .isEqualTo(1.0);
                        });
    }

    private static SqsAsyncClient sqsClientReturning(String body) {
        var client = mock(SqsAsyncClient.class);
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body(body)
                        .build();
        var firstReceive =
                CompletableFuture.completedFuture(
                        ReceiveMessageResponse.builder().messages(message).build());
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();
        var receiveCalls = new AtomicInteger();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? firstReceive
                                        : pendingReceive);
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation ->
                                CompletableFuture.completedFuture(
                                        successfulDeleteResponse(
                                                invocation.getArgument(
                                                        0, DeleteMessageBatchRequest.class))));
        return client;
    }

    private static DeleteMessageBatchResponse successfulDeleteResponse(
            DeleteMessageBatchRequest request) {
        return DeleteMessageBatchResponse.builder()
                .successful(
                        request.entries().stream()
                                .map(
                                        entry ->
                                                DeleteMessageBatchResultEntry.builder()
                                                        .id(entry.id())
                                                        .build())
                                .toList())
                .build();
    }

    record OrderCreated(String orderId) {}

    static final class OrderListener {
        private final Sinks.One<OrderCreated> received = Sinks.one();

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.fromRunnable(() -> received.tryEmitValue(event));
        }
    }

    static final class EnvelopeOrderListener {
        private final Sinks.One<SqsMessage<OrderCreated>> received = Sinks.one();

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(SqsMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> received.tryEmitValue(message));
        }
    }

    static final class InvocationCountingOrderListener {
        private final AtomicInteger invocations = new AtomicInteger();

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.fromRunnable(invocations::incrementAndGet);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {}
}
