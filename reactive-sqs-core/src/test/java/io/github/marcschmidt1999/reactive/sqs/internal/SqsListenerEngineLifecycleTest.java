package io.github.marcschmidt1999.reactive.sqs.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.test.scheduler.VirtualTimeScheduler;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.RequestThrottledException;
import software.amazon.awssdk.services.sqs.model.SqsException;

class SqsListenerEngineLifecycleTest {

    private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
    private SqsListenerEngine engine;

    @AfterEach
    void stopEngine() {
        if (engine != null) {
            engine.stop().subscribe();
        }
        scheduler.dispose();
    }

    @Test
    void stopCancelsReceiveThatReturnsFromClientAfterShutdownStarted() throws Exception {
        var client = mock(SqsAsyncClient.class);
        var receiveEntered = new CountDownLatch(1);
        var releaseReceiveCall = new CountDownLatch(1);
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored -> {
                            receiveEntered.countDown();
                            if (!releaseReceiveCall.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException(
                                        "test did not release receive call");
                            }
                            return pendingReceive;
                        });
        engine = engine(client, 1, 30, 100);

        var start = CompletableFuture.runAsync(() -> engine.start(ignored -> Mono.empty()));
        assertThat(receiveEntered.await(5, TimeUnit.SECONDS)).isTrue();

        var stopped = engine.stop().toFuture();
        releaseReceiveCall.countDown();
        start.join();

        try {
            assertThat(pendingReceive).isCancelled();
            assertThat(stopped).isCompleted();
        } finally {
            pendingReceive.complete(ReceiveMessageResponse.builder().build());
        }
    }

    @Test
    void retriesSqsThrottlingResponseThatUsesHttp400() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var throttled =
                RequestThrottledException.builder()
                        .statusCode(400)
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .serviceName("SQS")
                                        .errorCode("RequestThrottled")
                                        .errorMessage("rate exceeded")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.failedFuture(throttled)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        engine = engine(client, 1, 30, 100);

        engine.start(ignored -> Mono.empty());
        scheduler.advanceTimeBy(Duration.ofSeconds(1));

        assertThat(receiveCalls).hasValue(2);
        assertThat(engine.isRunning()).isTrue();
    }

    @Test
    void permanentReceiveFailureDoesNotCancelActiveHandler() {
        var client = mock(SqsAsyncClient.class);
        var processing = new CompletableFuture<Void>();
        var handlerCancelled = new CompletableFuture<Void>();
        var deleteRequest = new CompletableFuture<DeleteMessageBatchRequest>();
        var receiveCalls = new AtomicInteger();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        var forbidden = SqsException.builder().statusCode(403).message("forbidden").build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                switch (receiveCalls.getAndIncrement()) {
                                    case 0 ->
                                            CompletableFuture.completedFuture(
                                                    ReceiveMessageResponse.builder()
                                                            .messages(message)
                                                            .build());
                                    default -> CompletableFuture.failedFuture(forbidden);
                                });
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var request =
                                    invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            deleteRequest.complete(request);
                            return CompletableFuture.completedFuture(
                                    successfulDeleteResponse(request));
                        });
        engine = engine(client, 2, 1, 100);

        engine.start(
                ignored ->
                        Mono.fromFuture(processing)
                                .doOnCancel(() -> handlerCancelled.complete(null)));
        assertThat(receiveCalls).hasValue(2);

        scheduler.advanceTimeBy(Duration.ofSeconds(2));

        assertThat(handlerCancelled).isNotCompleted();
        assertThat(deleteRequest).isNotCompleted();

        processing.complete(null);
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);
        assertThat(deleteRequest).isCompleted();
    }

    @Test
    void activeDeliveryCompletionCannotBypassReceiveFailureBackoff() throws Exception {
        var client = mock(SqsAsyncClient.class);
        var processing = new CompletableFuture<Void>();
        var failedReceive = new CompletableFuture<ReceiveMessageResponse>();
        var jitterEntered = new CountDownLatch(1);
        var releaseJitter = new CountDownLatch(1);
        var receiveCalls = new AtomicInteger();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                switch (receiveCalls.getAndIncrement()) {
                                    case 0 ->
                                            CompletableFuture.completedFuture(
                                                    ReceiveMessageResponse.builder()
                                                            .messages(message)
                                                            .build());
                                    case 1 -> failedReceive;
                                    default -> new CompletableFuture<ReceiveMessageResponse>();
                                });
        successfulDeletes(client);
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 2, 60, 20, 30, 100),
                        scheduler,
                        () -> {
                            jitterEntered.countDown();
                            try {
                                if (!releaseJitter.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("test did not release jitter");
                                }
                            } catch (InterruptedException error) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("test interrupted", error);
                            }
                            return 1.0;
                        });

        engine.start(ignored -> Mono.fromFuture(processing));
        var failureCallback =
                CompletableFuture.runAsync(
                        () ->
                                failedReceive.completeExceptionally(
                                        new IllegalStateException("temporary receive failure")));
        assertThat(jitterEntered.await(5, TimeUnit.SECONDS)).isTrue();

        try {
            processing.complete(null);
            assertThat(receiveCalls).hasValue(2);
        } finally {
            releaseJitter.countDown();
            failureCallback.join();
        }
    }

    @Test
    void stopPreventsReceiveRetryArmingDuringShutdown() throws Exception {
        var client = mock(SqsAsyncClient.class);
        var jitterEntered = new CountDownLatch(1);
        var releaseJitter = new CountDownLatch(1);
        var trackingScheduler = new TrackingScheduler(scheduler);
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(
                        CompletableFuture.failedFuture(
                                new IllegalStateException("temporary receive failure")));
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 30, 100),
                        trackingScheduler,
                        () -> {
                            jitterEntered.countDown();
                            try {
                                if (!releaseJitter.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("test did not release jitter");
                                }
                            } catch (InterruptedException error) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("test interrupted", error);
                            }
                            return 1.0;
                        });

        var start = CompletableFuture.runAsync(() -> engine.start(ignored -> Mono.empty()));
        assertThat(jitterEntered.await(5, TimeUnit.SECONDS)).isTrue();
        var stopped = engine.stop().toFuture();

        try {
            releaseJitter.countDown();
            start.join();

            assertThat(stopped).isCompleted();
            assertThat(trackingScheduler.lastScheduled.get()).isNull();
        } finally {
            releaseJitter.countDown();
        }
    }

    @Test
    void receiveRetryCannotFireBeforeFailedReservationIsReleased() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.failedFuture(
                                                new IllegalStateException(
                                                        "temporary receive failure"))
                                        : new CompletableFuture<ReceiveMessageResponse>());
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 30, 100),
                        new ImmediateDelayedScheduler(scheduler),
                        () -> 1.0);

        engine.start(ignored -> Mono.empty());

        assertThat(receiveCalls).hasValue(2);
    }

    @Test
    void visibilityRenewalDoesNotExceedOriginalReceiveBudget() {
        var client = mock(SqsAsyncClient.class);
        var visibilityRequest = new AtomicReference<ChangeMessageVisibilityRequest>();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenAnswer(
                        invocation -> {
                            visibilityRequest.set(invocation.getArgument(0));
                            return CompletableFuture.completedFuture(
                                    ChangeMessageVisibilityResponse.builder().build());
                        });
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 30_000, 20, 0, 43_200),
                        scheduler,
                        () -> 1.0);

        engine.start(ignored -> Mono.never());
        scheduler.advanceTimeBy(Duration.ofSeconds(15_000));

        assertThat(visibilityRequest.get()).isNotNull();
        assertThat(visibilityRequest.get().visibilityTimeout()).isBetween(1, 28_200);
    }

    @Test
    void absoluteVisibilityBudgetCancelsProcessingBeforeTwelveHours() {
        var client = mock(SqsAsyncClient.class);
        var handlerCancelled = new CompletableFuture<Void>();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                ChangeMessageVisibilityResponse.builder().build()));
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 43_200, 20, 0, 43_200),
                        scheduler,
                        () -> 1.0);

        engine.start(
                ignored -> Mono.<Void>never().doOnCancel(() -> handlerCancelled.complete(null)));
        scheduler.advanceTimeBy(Duration.ofSeconds(43_194));
        assertThat(handlerCancelled).isNotCompleted();

        scheduler.advanceTimeBy(Duration.ofSeconds(1));
        assertThat(handlerCancelled).isCompleted();
        verify(client, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void initialHeartbeatDoesNotConsumeLongPollTime() {
        var client = mock(SqsAsyncClient.class);
        var firstReceive = new CompletableFuture<ReceiveMessageResponse>();
        var visibilityCalls = new AtomicInteger();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? firstReceive
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenAnswer(
                        ignored -> {
                            visibilityCalls.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    ChangeMessageVisibilityResponse.builder().build());
                        });
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 10, 20, 0, 100),
                        scheduler,
                        () -> 1.0);

        engine.start(ignored -> Mono.never());
        scheduler.advanceTimeBy(Duration.ofSeconds(8));
        firstReceive.complete(response);
        scheduler.advanceTimeBy(Duration.ofSeconds(1));
        assertThat(visibilityCalls).hasValue(0);

        scheduler.advanceTimeBy(Duration.ofMillis(3_500));

        assertThat(visibilityCalls).hasValue(1);
    }

    @Test
    void heartbeatCannotOverwriteSuccessorWhenScheduledTaskFiresImmediately() {
        var client = mock(SqsAsyncClient.class);
        var visibilityCalls = new AtomicInteger();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenAnswer(
                        ignored -> {
                            visibilityCalls.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    ChangeMessageVisibilityResponse.builder().build());
                        });
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 10, 20, 0, 100),
                        new FirstDelayedTaskImmediateScheduler(scheduler),
                        () -> 1.0);

        engine.start(ignored -> Mono.never());
        assertThat(visibilityCalls).hasValue(1);

        scheduler.advanceTimeBy(Duration.ofSeconds(5));
        assertThat(visibilityCalls).hasValue(2);
    }

    @Test
    void heartbeatFailureIsRetriedBeforeCurrentVisibilityExpires() {
        var client = mock(SqsAsyncClient.class);
        var visibilityCalls = new AtomicInteger();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenAnswer(
                        ignored -> {
                            visibilityCalls.incrementAndGet();
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("temporary visibility failure"));
                        });
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 10, 20, 0, 100),
                        scheduler,
                        () -> 1.0);

        engine.start(ignored -> Mono.never());
        scheduler.advanceTimeBy(Duration.ofSeconds(5));
        assertThat(visibilityCalls).hasValue(1);

        scheduler.advanceTimeBy(Duration.ofSeconds(1));
        assertThat(visibilityCalls).hasValue(2);
    }

    @Test
    void lateHeartbeatFailureClampsRetryToCurrentLeaseDeadline() {
        var client = mock(SqsAsyncClient.class);
        var visibilityCalls = new AtomicInteger();
        var firstVisibility = new CompletableFuture<ChangeMessageVisibilityResponse>();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenAnswer(
                        ignored -> {
                            var call = visibilityCalls.getAndIncrement();
                            return call == 0
                                    ? firstVisibility
                                    : CompletableFuture.completedFuture(
                                            ChangeMessageVisibilityResponse.builder().build());
                        });
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 10, 20, 0, 100),
                        scheduler,
                        () -> 1.0);

        engine.start(ignored -> Mono.never());
        scheduler.advanceTimeBy(Duration.ofMillis(8_500));
        firstVisibility.completeExceptionally(new IllegalStateException("late visibility failure"));

        scheduler.advanceTimeBy(Duration.ofMillis(399));
        assertThat(visibilityCalls).hasValue(2);
    }

    @Test
    void permanentHeartbeatFailureIsNotRetried() {
        var client = mock(SqsAsyncClient.class);
        var visibilityCalls = new AtomicInteger();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenAnswer(
                        ignored -> {
                            visibilityCalls.incrementAndGet();
                            return CompletableFuture.failedFuture(
                                    SqsException.builder()
                                            .statusCode(403)
                                            .message("forbidden")
                                            .build());
                        });
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 10, 20, 0, 100),
                        scheduler,
                        () -> 1.0);

        engine.start(ignored -> Mono.never());
        scheduler.advanceTimeBy(Duration.ofSeconds(30));

        assertThat(visibilityCalls).hasValue(1);
    }

    @Test
    void nonRetryableSdkClientHeartbeatFailureIsNotRetried() {
        var client = mock(SqsAsyncClient.class);
        var visibilityCalls = new AtomicInteger();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenAnswer(
                        ignored -> {
                            visibilityCalls.incrementAndGet();
                            return CompletableFuture.failedFuture(
                                    SdkClientException.create("invalid client configuration"));
                        });
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 10, 20, 0, 100),
                        scheduler,
                        () -> 1.0);

        engine.start(ignored -> Mono.never());
        scheduler.advanceTimeBy(Duration.ofSeconds(30));

        assertThat(visibilityCalls).hasValue(1);
    }

    @Test
    void synchronousReceiveConfigurationFailureIsNotRetried() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored -> {
                            receiveCalls.incrementAndGet();
                            throw new IllegalArgumentException("invalid endpoint");
                        });
        engine = engine(client, 1, 30, 100);

        engine.start(ignored -> Mono.empty());
        scheduler.advanceTimeBy(Duration.ofMinutes(5));

        assertThat(receiveCalls).hasValue(1);
        assertThat(engine.isRunning()).isFalse();
    }

    @Test
    void nonRetryableSdkClientReceiveFailureIsNotRetried() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored -> {
                            receiveCalls.incrementAndGet();
                            return CompletableFuture.failedFuture(
                                    SdkClientException.create("invalid client configuration"));
                        });
        engine = engine(client, 1, 30, 100);

        engine.start(ignored -> Mono.empty());
        scheduler.advanceTimeBy(Duration.ofMinutes(5));

        assertThat(receiveCalls).hasValue(1);
        assertThat(engine.isRunning()).isFalse();
    }

    @Test
    void forcedStopDoesNotStartDeliveryThatHasNotBegunProcessing() {
        var client = mock(SqsAsyncClient.class);
        var deliveryStart = new AtomicReference<Runnable>();
        var handlerInvocations = new AtomicInteger();
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 0, 100),
                        scheduler,
                        () -> 1.0,
                        deliveryStart::set);

        engine.start(
                ignored -> {
                    handlerInvocations.incrementAndGet();
                    return Mono.empty();
                });
        assertThat(deliveryStart.get()).isNotNull();

        var stopped = engine.stop().toFuture();
        scheduler.advanceTimeBy(Duration.ZERO);

        assertThat(stopped).isCompleted();
        deliveryStart.get().run();
        assertThat(handlerInvocations).hasValue(0);
        verify(client, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void forcedStopDuringSynchronousHandlerInvocationPreventsDelete() throws Exception {
        var client = mock(SqsAsyncClient.class);
        var deliveryStart = new AtomicReference<Runnable>();
        var handlerEntered = new CountDownLatch(1);
        var releaseHandler = new CountDownLatch(1);
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        successfulDeletes(client);
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 0, 100),
                        scheduler,
                        () -> 1.0,
                        deliveryStart::set);
        engine.start(
                ignored -> {
                    handlerEntered.countDown();
                    try {
                        if (!releaseHandler.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("test did not release handler");
                        }
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("test interrupted", error);
                    }
                    return Mono.empty();
                });

        var startingDelivery = CompletableFuture.runAsync(deliveryStart.get());
        assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).isTrue();
        var stopped = engine.stop().toFuture();
        scheduler.advanceTimeBy(Duration.ZERO);

        try {
            assertThat(stopped).isCompleted();
        } finally {
            releaseHandler.countDown();
            startingDelivery.join();
        }
        verify(client, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void forcedStopBeforeDeleteStartsPreventsDelete() throws Exception {
        var client = mock(SqsAsyncClient.class);
        var processing = new CompletableFuture<Void>();
        var releaseDelete = new CompletableFuture<Void>();
        var deleteBoundaryEntered = new CountDownLatch(1);
        var receiveCalls = new AtomicInteger();
        var response =
                ReceiveMessageResponse.builder()
                        .messages(
                                Message.builder()
                                        .messageId("message-42")
                                        .receiptHandle("receipt-42")
                                        .body("order-42")
                                        .build())
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(response)
                                        : new CompletableFuture<ReceiveMessageResponse>());
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 0, 100),
                        scheduler,
                        () -> 1.0,
                        Runnable::run,
                        deletion ->
                                Mono.defer(
                                        () -> {
                                            deleteBoundaryEntered.countDown();
                                            return Mono.fromFuture(releaseDelete).then(deletion);
                                        }));
        engine.start(ignored -> Mono.fromFuture(processing));

        processing.complete(null);
        assertThat(deleteBoundaryEntered.await(5, TimeUnit.SECONDS)).isTrue();
        var stopped = engine.stop().toFuture();
        scheduler.advanceTimeBy(Duration.ZERO);

        assertThat(stopped).isCompleted();
        releaseDelete.complete(null);
        verify(client, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    private SqsListenerEngine engine(
            SqsAsyncClient client,
            int maxInFlight,
            int shutdownGraceSeconds,
            int maxProcessingDurationSeconds) {
        return new SqsListenerEngine(
                client,
                new SqsListenerEngine.Configuration(
                        "orders",
                        "queue-url",
                        maxInFlight,
                        60,
                        20,
                        shutdownGraceSeconds,
                        maxProcessingDurationSeconds),
                scheduler,
                () -> 1.0);
    }

    private static void successfulDeletes(SqsAsyncClient client) {
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var request =
                                    invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            return CompletableFuture.completedFuture(
                                    successfulDeleteResponse(request));
                        });
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

    private static final class TrackingScheduler implements Scheduler {
        private final Scheduler delegate;
        private final AtomicReference<Disposable> lastScheduled = new AtomicReference<>();

        private TrackingScheduler(Scheduler delegate) {
            this.delegate = delegate;
        }

        @Override
        public Disposable schedule(Runnable task) {
            var scheduled = delegate.schedule(task);
            lastScheduled.set(scheduled);
            return scheduled;
        }

        @Override
        public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            var scheduled = delegate.schedule(task, delay, unit);
            lastScheduled.set(scheduled);
            return scheduled;
        }

        @Override
        public Worker createWorker() {
            return delegate.createWorker();
        }

        @Override
        public long now(TimeUnit unit) {
            return delegate.now(unit);
        }

        @Override
        public void dispose() {
            delegate.dispose();
        }

        @Override
        public boolean isDisposed() {
            return delegate.isDisposed();
        }
    }

    private static final class ImmediateDelayedScheduler implements Scheduler {
        private final Scheduler delegate;

        private ImmediateDelayedScheduler(Scheduler delegate) {
            this.delegate = delegate;
        }

        @Override
        public Disposable schedule(Runnable task) {
            return delegate.schedule(task);
        }

        @Override
        public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            task.run();
            return Disposables.disposed();
        }

        @Override
        public Worker createWorker() {
            return delegate.createWorker();
        }

        @Override
        public long now(TimeUnit unit) {
            return delegate.now(unit);
        }

        @Override
        public void dispose() {
            delegate.dispose();
        }

        @Override
        public boolean isDisposed() {
            return delegate.isDisposed();
        }
    }

    private static final class FirstDelayedTaskImmediateScheduler implements Scheduler {
        private final Scheduler delegate;
        private final AtomicInteger delayedTasks = new AtomicInteger();

        private FirstDelayedTaskImmediateScheduler(Scheduler delegate) {
            this.delegate = delegate;
        }

        @Override
        public Disposable schedule(Runnable task) {
            return delegate.schedule(task);
        }

        @Override
        public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            if (delayedTasks.getAndIncrement() == 0) {
                task.run();
                return Disposables.disposed();
            }
            return delegate.schedule(task, delay, unit);
        }

        @Override
        public Worker createWorker() {
            return delegate.createWorker();
        }

        @Override
        public long now(TimeUnit unit) {
            return delegate.now(unit);
        }

        @Override
        public void dispose() {
            delegate.dispose();
        }

        @Override
        public boolean isDisposed() {
            return delegate.isDisposed();
        }
    }
}
