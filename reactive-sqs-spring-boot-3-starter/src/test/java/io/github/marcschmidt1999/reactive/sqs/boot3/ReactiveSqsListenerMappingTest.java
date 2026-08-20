package io.github.marcschmidt1999.reactive.sqs.boot3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.marcschmidt1999.reactive.sqs.SqsMessage;
import io.github.marcschmidt1999.reactive.sqs.annotation.ReactiveSqsListener;
import io.github.marcschmidt1999.reactive.sqs.boot3.autoconfigure.ReactiveSqsAutoConfiguration;
import io.github.marcschmidt1999.reactive.sqs.boot3.autoconfigure.ReactiveSqsMetricsAutoConfiguration;
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

class ReactiveSqsListenerMappingTest {

    private static final String QUEUE_URL =
            "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(
                                    ReactiveSqsMetricsAutoConfiguration.class,
                                    ReactiveSqsAutoConfiguration.class));

    @Test
    void annotatedListenerReceivesBodyMappedToItsParameterType() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(OrderListener.class, () -> listener)
                .run(
                        context ->
                                StepVerifier.create(listener.received.asMono())
                                        .expectNext(new OrderCreated("order-42"))
                                        .verifyComplete());
    }

    @Test
    void annotatedListenerMayReceiveMappedPayloadWithSqsMetadata() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new EnvelopeOrderListener();

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
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
    void messageIsDeletedOnlyAfterReactiveHandlerCompletes() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new ControllableOrderListener();

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(ControllableOrderListener.class, () -> listener)
                .run(
                        context -> {
                            StepVerifier.create(listener.received.asMono())
                                    .expectNext(new OrderCreated("order-42"))
                                    .verifyComplete();
                            assertThat(sqs.deleteRequest()).isNotDone();

                            listener.processing.tryEmitEmpty();

                            StepVerifier.create(Mono.fromFuture(sqs.deleteRequest()))
                                    .assertNext(
                                            request -> {
                                                assertThat(request.queueUrl()).isEqualTo(QUEUE_URL);
                                                assertThat(request.entries())
                                                        .singleElement()
                                                        .extracting(entry -> entry.receiptHandle())
                                                        .isEqualTo("receipt-42");
                                            })
                                    .expectComplete()
                                    .verify(Duration.ofSeconds(2));
                        });
    }

    @Test
    void handlerFailureLeavesMessageUndeleted() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new FailingOrderListener();

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(FailingOrderListener.class, () -> listener)
                .run(
                        context -> {
                            StepVerifier.create(listener.received.asMono())
                                    .expectNext(new OrderCreated("order-42"))
                                    .verifyComplete();

                            assertThat(sqs.receiveCalls()).hasValue(2);
                            assertThat(sqs.deleteRequest()).isNotDone();
                        });
    }

    @Test
    void payloadMappingFailureLeavesMessageUndeletedAndDoesNotInvokeHandler() {
        var sqs = sqsClientReturning("not-json");
        var listener = new InvocationCountingOrderListener();

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(InvocationCountingOrderListener.class, () -> listener)
                .run(
                        context -> {
                            assertThat(listener.invocations).hasValue(0);
                            assertThat(sqs.receiveCalls()).hasValue(2);
                            assertThat(sqs.deleteRequest()).isNotDone();
                        });
    }

    @Test
    void doesNotReceiveAnotherMessageWhilePreviousHandlerIsRunning() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new ControllableOrderListener();

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(ControllableOrderListener.class, () -> listener)
                .run(
                        context -> {
                            StepVerifier.create(listener.received.asMono())
                                    .expectNext(new OrderCreated("order-42"))
                                    .verifyComplete();

                            assertThat(sqs.receiveCalls()).hasValue(1);
                            listener.processing.tryEmitEmpty();
                        });
    }

    @Test
    void starterIsDiscoveredBySpringBootAutoConfiguration() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        new ApplicationContextRunner()
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(OrderListener.class, () -> listener)
                .run(
                        context ->
                                StepVerifier.create(listener.received.asMono())
                                        .expectNext(new OrderCreated("order-42"))
                                        .expectComplete()
                                        .verify(Duration.ofSeconds(2)));
    }

    @Test
    void rejectsAnnotatedHandlerThatDoesNotReturnMonoVoid() throws NoSuchMethodException {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var invalidMethod =
                InvalidReturnTypeListener.class.getDeclaredMethod("handle", OrderCreated.class);

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(InvalidReturnTypeListener.class, InvalidReturnTypeListener::new)
                .run(
                        context ->
                                assertThat(context.getStartupFailure())
                                        .isNotNull()
                                        .hasMessage(
                                                "@ReactiveSqsListener method must have one parameter and return Mono<Void>: "
                                                        + invalidMethod));
    }

    @Test
    void listenerRegistrationCanBeDisabledOperationally() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL, "reactive-sqs.enabled=false")
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(OrderListener.class, OrderListener::new)
                .run(
                        context -> {
                            assertThat(context).doesNotHaveBean(ReactiveSqsListenerRegistrar.class);
                            assertThat(sqs.receiveCalls()).hasValue(0);
                        });
    }

    @Test
    void enabledMetricsRecordSuccessfulMessageProcessing() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        new ApplicationContextRunner()
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean("ordersListener", OrderListener.class, () -> listener)
                .run(
                        context -> {
                            StepVerifier.create(Mono.fromFuture(sqs.deleteRequest()))
                                    .expectNextCount(1)
                                    .verifyComplete();

                            var registry = context.getBean(SimpleMeterRegistry.class);
                            assertThat(
                                            registry.get("reactive.sqs.messages.received")
                                                    .tag("listener", "ordersListener.handle")
                                                    .counter()
                                                    .count())
                                    .isEqualTo(1.0);
                            assertThat(
                                            registry.get("reactive.sqs.receive")
                                                    .tags(
                                                            "listener",
                                                            "ordersListener.handle",
                                                            "outcome",
                                                            "messages")
                                                    .timer()
                                                    .count())
                                    .isEqualTo(1L);
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
                                            registry.get("reactive.sqs.delete")
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
                            assertThat(
                                            registry.get("reactive.sqs.listener.active")
                                                    .tag("listener", "ordersListener.handle")
                                                    .gauge()
                                                    .value())
                                    .isZero();
                            assertThat(
                                            registry.get("reactive.sqs.listener.inflight")
                                                    .tag("listener", "ordersListener.handle")
                                                    .gauge()
                                                    .value())
                                    .isEqualTo(1.0);
                            assertThat(
                                            registry.get("reactive.sqs.listener.running")
                                                    .tag("listener", "ordersListener.handle")
                                                    .gauge()
                                                    .value())
                                    .isEqualTo(1.0);
                        });
    }

    @Test
    void disabledMetricsRegisterNoReactiveSqsMeters() {
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        contextRunner
                .withPropertyValues(
                        "test.queue-url=" + QUEUE_URL, "reactive-sqs.metrics.enabled=false")
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean("ordersListener", OrderListener.class, () -> listener)
                .run(
                        context -> {
                            StepVerifier.create(Mono.fromFuture(sqs.deleteRequest()))
                                    .expectNextCount(1)
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
        var sqs = sqsClientReturning("{\"orderId\":\"order-42\"}");
        var listener = new OrderListener();

        new ApplicationContextRunner()
                .withClassLoader(new FilteredClassLoader("io.micrometer.core.instrument"))
                .withUserConfiguration(TestApplication.class)
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(OrderListener.class, () -> listener)
                .run(
                        context ->
                                StepVerifier.create(listener.received.asMono())
                                        .expectNext(new OrderCreated("order-42"))
                                        .verifyComplete());
    }

    @Test
    void metricsClassifyPayloadMappingFailuresSeparately() {
        var sqs = sqsClientReturning("not-json");
        var listener = new InvocationCountingOrderListener();

        contextRunner
                .withPropertyValues("test.queue-url=" + QUEUE_URL)
                .withBean(SqsAsyncClient.class, sqs::client)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean("ordersListener", InvocationCountingOrderListener.class, () -> listener)
                .run(
                        context -> {
                            assertThat(listener.invocations).hasValue(0);
                            assertThat(sqs.receiveCalls()).hasValue(2);
                            assertThat(sqs.deleteRequest()).isNotDone();

                            var registry = context.getBean(SimpleMeterRegistry.class);
                            assertThat(
                                            registry.get("reactive.sqs.processing")
                                                    .tags(
                                                            "listener",
                                                            "ordersListener.handle",
                                                            "outcome",
                                                            "mapping_error")
                                                    .timer()
                                                    .count())
                                    .isEqualTo(1L);
                            assertThat(
                                            registry.get("reactive.sqs.delivery")
                                                    .tags(
                                                            "listener",
                                                            "ordersListener.handle",
                                                            "outcome",
                                                            "mapping_error")
                                                    .counter()
                                                    .count())
                                    .isEqualTo(1.0);
                            assertThat(
                                            registry.get("reactive.sqs.delete")
                                                    .tag("listener", "ordersListener.handle")
                                                    .timers())
                                    .allMatch(timer -> timer.count() == 0L);
                        });
    }

    private static SqsClientProbe sqsClientReturning(String body) {
        var client = mock(SqsAsyncClient.class);
        var deleteRequest = new CompletableFuture<DeleteMessageBatchRequest>();
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
                        invocation -> {
                            var request =
                                    invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            deleteRequest.complete(request);
                            return CompletableFuture.completedFuture(
                                    successfulDeleteResponse(request));
                        });
        return new SqsClientProbe(client, deleteRequest, receiveCalls);
    }

    record OrderCreated(String orderId) {}

    static final class OrderListener {
        private final Sinks.One<OrderCreated> received = Sinks.one();

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.fromRunnable(() -> received.tryEmitValue(event));
        }
    }

    static final class ControllableOrderListener {
        private final Sinks.One<OrderCreated> received = Sinks.one();
        private final Sinks.Empty<Void> processing = Sinks.empty();

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.defer(
                    () -> {
                        received.tryEmitValue(event);
                        return processing.asMono();
                    });
        }
    }

    static final class FailingOrderListener {
        private final Sinks.One<OrderCreated> received = Sinks.one();

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.defer(
                    () -> {
                        received.tryEmitValue(event);
                        return Mono.error(new IllegalStateException("handler failed"));
                    });
        }
    }

    static final class InvocationCountingOrderListener {
        private final AtomicInteger invocations = new AtomicInteger();

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.fromRunnable(invocations::incrementAndGet);
        }
    }

    static final class EnvelopeOrderListener {
        private final Sinks.One<SqsMessage<OrderCreated>> received = Sinks.one();

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(SqsMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> received.tryEmitValue(message));
        }
    }

    static final class InvalidReturnTypeListener {

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<String> handle(OrderCreated event) {
            return Mono.just(event.orderId());
        }
    }

    private record SqsClientProbe(
            SqsAsyncClient client,
            CompletableFuture<DeleteMessageBatchRequest> deleteRequest,
            AtomicInteger receiveCalls) {}

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

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class TestApplication {}
}
