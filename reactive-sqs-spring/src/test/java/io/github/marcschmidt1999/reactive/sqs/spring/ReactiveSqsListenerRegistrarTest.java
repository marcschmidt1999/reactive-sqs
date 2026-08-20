package io.github.marcschmidt1999.reactive.sqs.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.marcschmidt1999.reactive.sqs.annotation.ReactiveSqsListener;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.mock.env.MockEnvironment;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.scheduler.VirtualTimeScheduler;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

class ReactiveSqsListenerRegistrarTest {

    private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
    private ReactiveSqsListenerRegistrar registrar;

    @AfterEach
    void stopRegistrar() {
        if (registrar != null) {
            registrar.destroy();
        }
        scheduler.dispose();
    }

    @Test
    void retriesReceiveAfterBackoffAndContinuesHandlingMessages() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var handled = new CompletableFuture<OrderCreated>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        var response = ReceiveMessageResponse.builder().messages(message).build();
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                switch (receiveCalls.getAndIncrement()) {
                                    case 0 ->
                                            CompletableFuture.failedFuture(
                                                    new IllegalStateException(
                                                            "temporarily unavailable"));
                                    case 1 -> CompletableFuture.completedFuture(response);
                                    default -> pendingReceive;
                                });
        successfulDeletes(client);

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(OrderListener.class, () -> new OrderListener(handled)));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);

        registrar.afterSingletonsInstantiated();
        registrar.start();

        assertThat(receiveCalls).hasValue(1);
        scheduler.advanceTimeBy(Duration.ofMillis(499));
        assertThat(receiveCalls).hasValue(1);

        scheduler.advanceTimeBy(Duration.ofMillis(501));

        assertThat(receiveCalls).hasValue(3);
        assertThat(handled).isCompletedWithValue(new OrderCreated("order-42"));
    }

    @Test
    void doesNotRetryPermanentSqsClientErrors() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var failure = SqsException.builder().statusCode(403).message("access denied").build();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored -> {
                            receiveCalls.incrementAndGet();
                            return CompletableFuture.failedFuture(failure);
                        });

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        OrderListener.class, () -> new OrderListener(new CompletableFuture<>())));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);

        registrar.afterSingletonsInstantiated();
        registrar.start();
        scheduler.advanceTimeBy(Duration.ofMinutes(5));

        assertThat(receiveCalls).hasValue(1);
    }

    @Test
    void rejectsFifoQueueUntilMessageGroupOrderingIsSupported() {
        var client = mock(SqsAsyncClient.class);
        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        OrderListener.class, () -> new OrderListener(new CompletableFuture<>())));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment()
                                .withProperty(
                                        "test.queue-url",
                                        "https://sqs.eu-central-1.amazonaws.com/123456789012/orders.fifo"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);

        assertThatThrownBy(registrar::afterSingletonsInstantiated)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIFO queues are not supported yet: orders.fifo");
    }

    @Test
    void rejectsDuplicateListenerIdsBeforeStartingAnyEngine() {
        var client = mock(SqsAsyncClient.class);
        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "overloadedListener", new RootBeanDefinition(OverloadedListener.class));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> body,
                        scheduler);

        assertThatThrownBy(registrar::afterSingletonsInstantiated)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Duplicate reactive SQS listener id: overloadedListener.handle. "
                                + "Listener bean names and method names must form a unique id.");
        assertThat(Mockito.mockingDetails(client).getInvocations()).isEmpty();
    }

    @Test
    void springStopWaitsForHandlerAndDeleteToComplete() {
        var client = mock(SqsAsyncClient.class);
        var processing = Sinks.<Void>empty();
        var handlerStarted = new CompletableFuture<Void>();
        var deleteStarted = new CompletableFuture<DeleteMessageBatchRequest>();
        var deleteCompleted = new CompletableFuture<DeleteMessageBatchResponse>();
        var stopped = new CompletableFuture<Void>();
        var receiveCalls = new AtomicInteger();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        var response = ReceiveMessageResponse.builder().messages(message).build();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored -> {
                            receiveCalls.incrementAndGet();
                            return CompletableFuture.completedFuture(response);
                        });
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            deleteStarted.complete(invocation.getArgument(0));
                            return deleteCompleted;
                        });

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        ControllableOrderListener.class,
                        () -> new ControllableOrderListener(processing, handlerStarted)));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);
        registrar.afterSingletonsInstantiated();
        registrar.start();
        assertThat(handlerStarted).isCompleted();

        registrar.stop(() -> stopped.complete(null));

        assertThat(stopped).isNotCompleted();
        assertThat(deleteStarted).isNotCompleted();
        processing.tryEmitEmpty();
        scheduler.advanceTimeBy(java.time.Duration.ofMillis(5));
        assertThat(deleteStarted).isCompleted();
        assertThat(stopped).isNotCompleted();

        deleteCompleted.complete(successfulDeleteResponse(deleteStarted.join()));

        assertThat(stopped).isCompleted();
        assertThat(receiveCalls).hasValue(1);
    }

    @Test
    void springStopCancelsUnfinishedHandlerAfterGraceWithoutDeleting() {
        var client = mock(SqsAsyncClient.class);
        var handlerStarted = new CompletableFuture<Void>();
        var handlerCancelled = new CompletableFuture<Void>();
        var stopped = new CompletableFuture<Void>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        var response = ReceiveMessageResponse.builder().messages(message).build();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(response));

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        StuckOrderListener.class,
                        () -> new StuckOrderListener(handlerStarted, handlerCancelled)));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);
        registrar.afterSingletonsInstantiated();
        registrar.start();
        assertThat(handlerStarted).isCompleted();

        registrar.stop(() -> stopped.complete(null));
        scheduler.advanceTimeBy(Duration.ofMillis(4_999));
        assertThat(stopped).isNotCompleted();
        assertThat(handlerCancelled).isNotCompleted();

        scheduler.advanceTimeBy(Duration.ofMillis(1));

        assertThat(handlerCancelled).isCompleted();
        assertThat(stopped).isCompleted();
        assertThat(Mockito.mockingDetails(client).getInvocations())
                .noneMatch(invocation -> invocation.getMethod().getName().equals("deleteMessage"));
    }

    @Test
    void processingTimeoutLeavesMessageUndeletedAndReleasesCapacity() {
        var client = mock(SqsAsyncClient.class);
        var handlerStarted = new CompletableFuture<Void>();
        var handlerCancelled = new CompletableFuture<Void>();
        var receiveCalls = new AtomicInteger();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        var response = ReceiveMessageResponse.builder().messages(message).build();
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : pendingReceive);
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                ChangeMessageVisibilityResponse.builder().build()));

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        TimedOrderListener.class,
                        () -> new TimedOrderListener(handlerStarted, handlerCancelled)));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);
        registrar.afterSingletonsInstantiated();
        registrar.start();
        assertThat(handlerStarted).isCompleted();

        scheduler.advanceTimeBy(Duration.ofMillis(9_999));
        assertThat(handlerCancelled).isNotCompleted();
        assertThat(receiveCalls).hasValue(1);

        scheduler.advanceTimeBy(Duration.ofMillis(1));

        assertThat(handlerCancelled).isCompleted();
        assertThat(receiveCalls).hasValue(2);
        assertThat(Mockito.mockingDetails(client).getInvocations())
                .noneMatch(invocation -> invocation.getMethod().getName().equals("deleteMessage"));
    }

    @Test
    void renewsVisibilityWhileProcessingAndStopsAfterSettlement() {
        var client = mock(SqsAsyncClient.class);
        var processing = Sinks.<Void>empty();
        var handlerStarted = new CompletableFuture<Void>();
        var receiveRequests = new CopyOnWriteArrayList<ReceiveMessageRequest>();
        var visibilityChanges = new CopyOnWriteArrayList<ChangeMessageVisibilityRequest>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        var response = ReceiveMessageResponse.builder().messages(message).build();
        var receiveCalls = new AtomicInteger();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        invocation -> {
                            receiveRequests.add(invocation.getArgument(0));
                            return receiveCalls.getAndIncrement() == 0
                                    ? CompletableFuture.completedFuture(response)
                                    : new CompletableFuture<ReceiveMessageResponse>();
                        });
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenAnswer(
                        invocation -> {
                            visibilityChanges.add(invocation.getArgument(0));
                            return CompletableFuture.completedFuture(
                                    ChangeMessageVisibilityResponse.builder().build());
                        });
        successfulDeletes(client);

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        VisibilityOrderListener.class,
                        () -> new VisibilityOrderListener(processing, handlerStarted)));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);
        registrar.afterSingletonsInstantiated();
        registrar.start();

        assertThat(handlerStarted).isCompleted();
        assertThat(receiveRequests.get(0).visibilityTimeout()).isEqualTo(10);
        scheduler.advanceTimeBy(Duration.ofMillis(4_499));
        assertThat(visibilityChanges).isEmpty();

        scheduler.advanceTimeBy(Duration.ofMillis(1));

        assertThat(visibilityChanges)
                .singleElement()
                .satisfies(
                        request -> {
                            assertThat(request.queueUrl()).isEqualTo("queue-url");
                            assertThat(request.receiptHandle()).isEqualTo("receipt-42");
                            assertThat(request.visibilityTimeout()).isEqualTo(10);
                        });

        processing.tryEmitEmpty();
        scheduler.advanceTimeBy(Duration.ofSeconds(10));
        assertThat(visibilityChanges).hasSize(1);
    }

    @Test
    void maxInFlightBoundsReceiveReservationsAndUnsettledMessages() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var started = new CopyOnWriteArrayList<String>();
        var processing = new ConcurrentHashMap<String, Sinks.Empty<Void>>();
        processing.put("order-1", Sinks.empty());
        processing.put("order-2", Sinks.empty());
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                message("message-1", "receipt-1", "order-1"),
                                message("message-2", "receipt-2", "order-2"))
                        .build();
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : pendingReceive);
        successfulDeletes(client);

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        ConcurrentOrderListener.class,
                        () -> new ConcurrentOrderListener(started, processing)));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);
        registrar.afterSingletonsInstantiated();
        registrar.start();

        assertThat(started).containsExactlyInAnyOrder("order-1", "order-2");
        assertThat(receiveCalls).hasValue(1);

        processing.get("order-1").tryEmitEmpty();
        scheduler.advanceTimeBy(java.time.Duration.ofMillis(5));

        assertThat(receiveCalls).hasValue(2);
        processing.get("order-2").tryEmitEmpty();
    }

    @Test
    void standardQueueMessagesSharingMessageGroupIdMayRunConcurrently() {
        var client = mock(SqsAsyncClient.class);
        var started = new CopyOnWriteArrayList<String>();
        var processing = new ConcurrentHashMap<String, Sinks.Empty<Void>>();
        processing.put("order-1", Sinks.empty());
        processing.put("order-2", Sinks.empty());
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                messageWithGroupId("message-1", "receipt-1", "order-1", "tenant-1"),
                                messageWithGroupId("message-2", "receipt-2", "order-2", "tenant-1"))
                        .build();
        var receiveCalls = new AtomicInteger();
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : pendingReceive);
        successfulDeletes(client);

        var beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition(
                "orderListener",
                new RootBeanDefinition(
                        ConcurrentOrderListener.class,
                        () -> new ConcurrentOrderListener(started, processing)));
        registrar =
                new ReactiveSqsListenerRegistrar(
                        beanFactory,
                        new MockEnvironment().withProperty("test.queue-url", "queue-url"),
                        client,
                        (body, type) -> new OrderCreated(body),
                        scheduler);

        registrar.afterSingletonsInstantiated();
        registrar.start();

        assertThat(started).containsExactlyInAnyOrder("order-1", "order-2");
        processing.values().forEach(sink -> sink.tryEmitEmpty());
    }

    private static Message message(String messageId, String receiptHandle, String body) {
        return Message.builder()
                .messageId(messageId)
                .receiptHandle(receiptHandle)
                .body(body)
                .build();
    }

    private static Message messageWithGroupId(
            String messageId, String receiptHandle, String body, String messageGroupId) {
        return Message.builder()
                .messageId(messageId)
                .receiptHandle(receiptHandle)
                .body(body)
                .attributes(Map.of(MessageSystemAttributeName.MESSAGE_GROUP_ID, messageGroupId))
                .build();
    }

    record OrderCreated(String orderId) {}

    static final class OrderListener {
        private final CompletableFuture<OrderCreated> handled;

        private OrderListener(CompletableFuture<OrderCreated> handled) {
            this.handled = handled;
        }

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.fromRunnable(() -> handled.complete(event));
        }
    }

    static final class ControllableOrderListener {
        private final Sinks.Empty<Void> processing;
        private final CompletableFuture<Void> handlerStarted;

        private ControllableOrderListener(
                Sinks.Empty<Void> processing, CompletableFuture<Void> handlerStarted) {
            this.processing = processing;
            this.handlerStarted = handlerStarted;
        }

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.defer(
                    () -> {
                        handlerStarted.complete(null);
                        return processing.asMono();
                    });
        }
    }

    static final class OverloadedListener {

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(OrderCreated event) {
            return Mono.empty();
        }

        @ReactiveSqsListener(queue = "${test.queue-url}")
        Mono<Void> handle(String event) {
            return Mono.empty();
        }
    }

    private static void successfulDeletes(SqsAsyncClient client) {
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation ->
                                CompletableFuture.completedFuture(
                                        successfulDeleteResponse(
                                                invocation.getArgument(
                                                        0, DeleteMessageBatchRequest.class))));
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

    static final class StuckOrderListener {
        private final CompletableFuture<Void> handlerStarted;
        private final CompletableFuture<Void> handlerCancelled;

        private StuckOrderListener(
                CompletableFuture<Void> handlerStarted, CompletableFuture<Void> handlerCancelled) {
            this.handlerStarted = handlerStarted;
            this.handlerCancelled = handlerCancelled;
        }

        @ReactiveSqsListener(queue = "${test.queue-url}", shutdownGraceSeconds = 5)
        Mono<Void> handle(OrderCreated event) {
            return Mono.<Void>never()
                    .doOnSubscribe(ignored -> handlerStarted.complete(null))
                    .doOnCancel(() -> handlerCancelled.complete(null));
        }
    }

    static final class TimedOrderListener {
        private final CompletableFuture<Void> handlerStarted;
        private final CompletableFuture<Void> handlerCancelled;

        private TimedOrderListener(
                CompletableFuture<Void> handlerStarted, CompletableFuture<Void> handlerCancelled) {
            this.handlerStarted = handlerStarted;
            this.handlerCancelled = handlerCancelled;
        }

        @ReactiveSqsListener(
                queue = "${test.queue-url}",
                visibilityTimeoutSeconds = 4,
                maxProcessingDurationSeconds = 10)
        Mono<Void> handle(OrderCreated event) {
            return Mono.<Void>never()
                    .doOnSubscribe(ignored -> handlerStarted.complete(null))
                    .doOnCancel(() -> handlerCancelled.complete(null));
        }
    }

    static final class VisibilityOrderListener {
        private final Sinks.Empty<Void> processing;
        private final CompletableFuture<Void> handlerStarted;

        private VisibilityOrderListener(
                Sinks.Empty<Void> processing, CompletableFuture<Void> handlerStarted) {
            this.processing = processing;
            this.handlerStarted = handlerStarted;
        }

        @ReactiveSqsListener(queue = "${test.queue-url}", visibilityTimeoutSeconds = 10)
        Mono<Void> handle(OrderCreated event) {
            return Mono.defer(
                    () -> {
                        handlerStarted.complete(null);
                        return processing.asMono();
                    });
        }
    }

    static final class ConcurrentOrderListener {
        private final CopyOnWriteArrayList<String> started;
        private final ConcurrentHashMap<String, Sinks.Empty<Void>> processing;

        private ConcurrentOrderListener(
                CopyOnWriteArrayList<String> started,
                ConcurrentHashMap<String, Sinks.Empty<Void>> processing) {
            this.started = started;
            this.processing = processing;
        }

        @ReactiveSqsListener(queue = "${test.queue-url}", maxInFlight = 2)
        Mono<Void> handle(OrderCreated event) {
            return Mono.defer(
                    () -> {
                        started.add(event.orderId());
                        return processing.get(event.orderId()).asMono();
                    });
        }
    }
}
