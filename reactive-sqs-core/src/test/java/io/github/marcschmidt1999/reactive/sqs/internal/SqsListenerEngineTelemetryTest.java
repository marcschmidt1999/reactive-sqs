package io.github.marcschmidt1999.reactive.sqs.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.scheduler.VirtualTimeScheduler;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

class SqsListenerEngineTelemetryTest {

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
    void completedVisibilityRenewalIsObserved() {
        var client = mock(SqsAsyncClient.class);
        var processing = new CompletableFuture<Void>();
        var receiveCalls = new AtomicInteger();
        var renewalEvents =
                new CopyOnWriteArrayList<SqsListenerTelemetry.VisibilityRenewalCompleted>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                ChangeMessageVisibilityResponse.builder().build()));
        successfulDeletes(client);
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void visibilityRenewalCompleted(VisibilityRenewalCompleted event) {
                        renewalEvents.add(event);
                    }
                };
        engine = new SqsListenerEngine(client, configuration(), scheduler, telemetry);

        engine.start(ignored -> Mono.fromFuture(processing));
        scheduler.advanceTimeBy(Duration.ofMillis(500));

        assertThat(renewalEvents)
                .extracting(SqsListenerTelemetry.VisibilityRenewalCompleted::outcome)
                .containsExactly(SqsListenerTelemetry.VisibilityOutcome.SUCCESS);
        processing.complete(null);
    }

    @Test
    void telemetryFailureCannotBreakSettlementOrCapacityRelease() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var deleteCalls = new AtomicInteger();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            deleteCalls.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    successfulDeleteResponse(
                                            invocation.getArgument(
                                                    0, DeleteMessageBatchRequest.class)));
                        });
        var throwingTelemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void receiveCompleted(ReceiveCompleted event) {
                        throw new IllegalStateException("receive telemetry failed");
                    }

                    @Override
                    public void deliveryCompleted(DeliveryCompleted event) {
                        throw new IllegalStateException("delivery telemetry failed");
                    }

                    @Override
                    public void stateChanged(ListenerState state) {
                        throw new IllegalStateException("state telemetry failed");
                    }
                };
        engine = new SqsListenerEngine(client, configuration(), scheduler, throwingTelemetry);

        engine.start(ignored -> Mono.empty());
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(deleteCalls).hasValue(1);
        assertThat(receiveCalls).hasValue(2);
        assertThat(engine.isRunning()).isTrue();
    }

    @Test
    void deliveryObservationRunsAfterCapacityAndShutdownOwnershipAreReleased() throws Exception {
        var client = mock(SqsAsyncClient.class);
        var realScheduler = Schedulers.newSingle("telemetry-observation-test");
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();
        var receiveCalls = new AtomicInteger();
        var deleteGateEntered = new CountDownLatch(1);
        var observationEntered = new CountDownLatch(1);
        var releaseObservation = new CountDownLatch(1);
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : pendingReceive);
        successfulDeletes(client);
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void deliveryCompleted(DeliveryCompleted event) {
                        observationEntered.countDown();
                        try {
                            if (!releaseObservation.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("test did not release telemetry");
                            }
                        } catch (InterruptedException error) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("test interrupted", error);
                        }
                    }
                };
        engine =
                new SqsListenerEngine(
                        client,
                        configuration(),
                        realScheduler,
                        telemetry,
                        () -> 1.0,
                        Runnable::run,
                        deletion ->
                                Mono.defer(
                                        () -> {
                                            deleteGateEntered.countDown();
                                            return deletion;
                                        }));

        var starting =
                CompletableFuture.runAsync(
                        () ->
                                engine.start(
                                        ignored -> {
                                            return Mono.empty();
                                        }));
        try {
            assertThat(deleteGateEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(observationEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(engine.stop().toFuture()).succeedsWithin(Duration.ofSeconds(2));
        } finally {
            releaseObservation.countDown();
            starting.join();
            realScheduler.dispose();
        }
    }

    @Test
    void retryableReceiveFailureRecordsOutcomeAndScheduledRetry() {
        var client = mock(SqsAsyncClient.class);
        var receives = new CopyOnWriteArrayList<SqsListenerTelemetry.ReceiveCompleted>();
        var retries = new CopyOnWriteArrayList<SqsListenerTelemetry.RetryOperation>();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(
                        CompletableFuture.failedFuture(
                                new IllegalStateException("temporarily unavailable")));
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void receiveCompleted(ReceiveCompleted event) {
                        receives.add(event);
                    }

                    @Override
                    public void retryScheduled(RetryOperation operation) {
                        retries.add(operation);
                    }
                };
        engine = new SqsListenerEngine(client, configuration(), scheduler, telemetry);

        engine.start(ignored -> Mono.empty());

        assertThat(receives)
                .extracting(SqsListenerTelemetry.ReceiveCompleted::outcome)
                .containsExactly(SqsListenerTelemetry.ReceiveOutcome.RETRYABLE_ERROR);
        assertThat(retries).containsExactly(SqsListenerTelemetry.RetryOperation.RECEIVE);
    }

    @Test
    void shutdownDuringSynchronousReceiveFailureRecordsCancellationWithoutRetry() throws Exception {
        var client = mock(SqsAsyncClient.class);
        var receiveEntered = new CountDownLatch(1);
        var releaseReceive = new CountDownLatch(1);
        var receives = new CopyOnWriteArrayList<SqsListenerTelemetry.ReceiveCompleted>();
        var retries = new CopyOnWriteArrayList<SqsListenerTelemetry.RetryOperation>();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored -> {
                            receiveEntered.countDown();
                            if (!releaseReceive.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("test did not release receive");
                            }
                            throw new IllegalStateException("receive failed during shutdown");
                        });
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void receiveCompleted(ReceiveCompleted event) {
                        receives.add(event);
                    }

                    @Override
                    public void retryScheduled(RetryOperation operation) {
                        retries.add(operation);
                    }
                };
        engine = new SqsListenerEngine(client, configuration(), scheduler, telemetry);

        var starting = CompletableFuture.runAsync(() -> engine.start(ignored -> Mono.empty()));
        assertThat(receiveEntered.await(5, TimeUnit.SECONDS)).isTrue();
        var stopped = engine.stop().toFuture();
        releaseReceive.countDown();
        starting.join();

        assertThat(receives)
                .extracting(SqsListenerTelemetry.ReceiveCompleted::outcome)
                .containsExactly(SqsListenerTelemetry.ReceiveOutcome.CANCELLED);
        assertThat(retries).isEmpty();
        assertThat(stopped).isCompleted();
    }

    @Test
    void terminalReceiveFailureIsReflectedInTelemetryState() {
        var client = mock(SqsAsyncClient.class);
        var receives = new CopyOnWriteArrayList<SqsListenerTelemetry.ReceiveCompleted>();
        var states = new CopyOnWriteArrayList<SqsListenerTelemetry.ListenerState>();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(
                        CompletableFuture.failedFuture(
                                SqsException.builder()
                                        .statusCode(403)
                                        .message("forbidden")
                                        .build()));
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void receiveCompleted(ReceiveCompleted event) {
                        receives.add(event);
                    }

                    @Override
                    public void stateChanged(ListenerState state) {
                        states.add(state);
                    }
                };
        engine = new SqsListenerEngine(client, configuration(), scheduler, telemetry);

        engine.start(ignored -> Mono.empty());

        assertThat(receives)
                .extracting(SqsListenerTelemetry.ReceiveCompleted::outcome)
                .containsExactly(SqsListenerTelemetry.ReceiveOutcome.TERMINAL_ERROR);
        assertThat(states.getLast().running()).isFalse();
        assertThat(states.getLast().failed()).isTrue();
        assertThat(states.getLast().inFlight()).isZero();
    }

    @Test
    void nullReceiveResponseIsATerminalClientContractFailure() {
        var client = mock(SqsAsyncClient.class);
        var receives = new CopyOnWriteArrayList<SqsListenerTelemetry.ReceiveCompleted>();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void receiveCompleted(ReceiveCompleted event) {
                        receives.add(event);
                    }
                };
        engine = new SqsListenerEngine(client, configuration(), scheduler, telemetry);

        engine.start(ignored -> Mono.empty());

        assertThat(receives)
                .extracting(SqsListenerTelemetry.ReceiveCompleted::outcome)
                .containsExactly(SqsListenerTelemetry.ReceiveOutcome.TERMINAL_ERROR);
        assertThat(engine.isRunning()).isFalse();
    }

    @Test
    void processingTimeoutAndDeleteFailureHaveDistinctDeliveryOutcomes() {
        var timedOut =
                deliveryFor(
                        Mono.never(),
                        CompletableFuture.completedFuture(
                                DeleteMessageBatchResponse.builder().build()),
                        1);
        assertThat(timedOut.processingOutcome())
                .isEqualTo(SqsListenerTelemetry.ProcessingOutcome.PROCESSING_TIMEOUT);
        assertThat(timedOut.outcome())
                .isEqualTo(SqsListenerTelemetry.DeliveryOutcome.PROCESSING_TIMEOUT);
        assertThat(timedOut.delete().outcome())
                .isEqualTo(SqsListenerTelemetry.DeleteOutcome.NOT_ATTEMPTED);

        var deleteFailed =
                deliveryFor(
                        Mono.empty(),
                        CompletableFuture.failedFuture(
                                new IllegalStateException("delete unavailable")),
                        100);
        assertThat(deleteFailed.processingOutcome())
                .isEqualTo(SqsListenerTelemetry.ProcessingOutcome.SUCCESS);
        assertThat(deleteFailed.outcome())
                .isEqualTo(SqsListenerTelemetry.DeliveryOutcome.DELETE_ERROR);
        assertThat(deleteFailed.delete().outcome())
                .isEqualTo(SqsListenerTelemetry.DeleteOutcome.ERROR);
    }

    @Test
    void applicationTimeoutExceptionIsAHandlerError() {
        var delivery =
                deliveryFor(
                        Mono.error(new TimeoutException("downstream timed out")),
                        CompletableFuture.completedFuture(
                                DeleteMessageBatchResponse.builder().build()),
                        100);

        assertThat(delivery.processingOutcome())
                .isEqualTo(SqsListenerTelemetry.ProcessingOutcome.HANDLER_ERROR);
        assertThat(delivery.outcome())
                .isEqualTo(SqsListenerTelemetry.DeliveryOutcome.HANDLER_ERROR);
        assertThat(delivery.delete().outcome())
                .isEqualTo(SqsListenerTelemetry.DeleteOutcome.NOT_ATTEMPTED);
    }

    @Test
    void forcedShutdownDuringSettlementRetainsItsCausalDeliveryOutcome() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var events = new CopyOnWriteArrayList<SqsListenerTelemetry.DeliveryCompleted>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : new CompletableFuture<ReceiveMessageResponse>());
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void deliveryCompleted(DeliveryCompleted event) {
                        events.add(event);
                    }
                };
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 0, 100),
                        scheduler,
                        telemetry,
                        () -> 1.0,
                        Runnable::run,
                        ignored -> Mono.never());

        engine.start(ignored -> Mono.empty());
        var stopped = engine.stop().toFuture();
        scheduler.advanceTimeBy(Duration.ZERO);

        assertThat(stopped).isCompleted();
        assertThat(events)
                .singleElement()
                .extracting(SqsListenerTelemetry.DeliveryCompleted::outcome)
                .isEqualTo(SqsListenerTelemetry.DeliveryOutcome.SHUTDOWN_CANCELLED);
    }

    @Test
    void shutdownCannotRelabelAConfirmedSuccessfulDelete() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var events = new CopyOnWriteArrayList<SqsListenerTelemetry.DeliveryCompleted>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : new CompletableFuture<ReceiveMessageResponse>());
        successfulDeletes(client);
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void deliveryCompleted(DeliveryCompleted event) {
                        events.add(event);
                    }
                };
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 0, 100),
                        scheduler,
                        telemetry,
                        () -> 1.0,
                        Runnable::run,
                        deletion -> deletion.then(Mono.never()));

        engine.start(ignored -> Mono.empty());
        var stopped = engine.stop().toFuture();
        scheduler.advanceTimeBy(Duration.ZERO);

        assertThat(stopped).isCompleted();
        assertThat(events)
                .singleElement()
                .extracting(SqsListenerTelemetry.DeliveryCompleted::outcome)
                .isEqualTo(SqsListenerTelemetry.DeliveryOutcome.ACKNOWLEDGED);
    }

    @Test
    void shutdownCannotRelabelAConfirmedDeleteFailure() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var events = new CopyOnWriteArrayList<SqsListenerTelemetry.DeliveryCompleted>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(
                        CompletableFuture.failedFuture(
                                new IllegalStateException("delete unavailable")));
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void deliveryCompleted(DeliveryCompleted event) {
                        events.add(event);
                    }
                };
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 0, 100),
                        scheduler,
                        telemetry,
                        () -> 1.0,
                        Runnable::run,
                        deletion -> deletion.onErrorResume(ignored -> Mono.never()));

        engine.start(ignored -> Mono.empty());
        var stopped = engine.stop().toFuture();
        scheduler.advanceTimeBy(Duration.ZERO);

        assertThat(stopped).isCompleted();
        assertThat(events)
                .singleElement()
                .extracting(SqsListenerTelemetry.DeliveryCompleted::outcome)
                .isEqualTo(SqsListenerTelemetry.DeliveryOutcome.DELETE_ERROR);
    }

    @Test
    void visibilityExpiryDuringSettlementRetainsItsCausalDeliveryOutcome() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var events = new CopyOnWriteArrayList<SqsListenerTelemetry.DeliveryCompleted>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                ChangeMessageVisibilityResponse.builder().build()));
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void deliveryCompleted(DeliveryCompleted event) {
                        events.add(event);
                    }
                };
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 30, 100),
                        scheduler,
                        telemetry,
                        () -> 1.0,
                        Runnable::run,
                        ignored -> Mono.never());

        engine.start(ignored -> Mono.empty());
        scheduler.advanceTimeBy(Duration.ofSeconds(43_195));

        assertThat(events)
                .singleElement()
                .extracting(SqsListenerTelemetry.DeliveryCompleted::outcome)
                .isEqualTo(SqsListenerTelemetry.DeliveryOutcome.VISIBILITY_TIMEOUT);
    }

    @Test
    void receiveDurationStopsBeforeSynchronousHandlerWork() {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var receives = new CopyOnWriteArrayList<SqsListenerTelemetry.ReceiveCompleted>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : new CompletableFuture<ReceiveMessageResponse>());
        successfulDeletes(client);
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void receiveCompleted(ReceiveCompleted event) {
                        receives.add(event);
                    }
                };
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 0, 100),
                        scheduler,
                        telemetry);

        engine.start(
                ignored -> Mono.fromRunnable(() -> scheduler.advanceTimeBy(Duration.ofSeconds(5))));

        assertThat(receives)
                .singleElement()
                .extracting(SqsListenerTelemetry.ReceiveCompleted::duration)
                .isEqualTo(Duration.ZERO);
    }

    private SqsListenerTelemetry.DeliveryCompleted deliveryFor(
            Mono<Void> handler,
            CompletableFuture<DeleteMessageBatchResponse> delete,
            int maxProcessingDurationSeconds) {
        var client = mock(SqsAsyncClient.class);
        var receiveCalls = new AtomicInteger();
        var events = new CopyOnWriteArrayList<SqsListenerTelemetry.DeliveryCompleted>();
        var message =
                Message.builder()
                        .messageId("message-42")
                        .receiptHandle("receipt-42")
                        .body("order-42")
                        .build();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(message)
                                                        .build())
                                        : new CompletableFuture<ReceiveMessageResponse>());
        when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                ChangeMessageVisibilityResponse.builder().build()));
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(delete);
        var telemetry =
                new SqsListenerTelemetry() {
                    @Override
                    public void deliveryCompleted(DeliveryCompleted event) {
                        events.add(event);
                    }
                };
        engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 1, 60, 20, 0, maxProcessingDurationSeconds),
                        scheduler,
                        telemetry);

        engine.start(ignored -> handler);
        if (maxProcessingDurationSeconds == 1) {
            scheduler.advanceTimeBy(Duration.ofSeconds(1));
        }
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(events).hasSize(1);
        var event = events.getFirst();
        engine.stop().block(Duration.ofSeconds(2));
        return event;
    }

    private static SqsListenerEngine.Configuration configuration() {
        return new SqsListenerEngine.Configuration("orders", "queue-url", 1, 2, 20, 0, 100);
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
}
