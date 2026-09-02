package io.github.marcschmidt1999.reactive.sqs.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
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

class SqsListenerEngineAcknowledgementSafetyTest {

    private static final Set<String> EXPECTED_SUCCESSFUL_RECEIPTS =
            Set.of("receipt-0", "receipt-2", "receipt-4", "receipt-6", "receipt-8");

    private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
    private SqsListenerEngine engine;

    @AfterEach
    void stopEngine() {
        if (engine != null) {
            engine.stop().subscribe();
        }
        scheduler.dispose();
    }

    @RepeatedTest(25)
    void deleteBatchContainsOnlyConcurrentlySuccessfulDeliveries() {
        var completions = new ConcurrentHashMap<String, CompletableFuture<Void>>();
        var messages = new ArrayList<Message>();
        for (var index = 0; index < SqsDeleteBatcher.MAX_BATCH_SIZE; index++) {
            var receiptHandle = "receipt-" + index;
            completions.put(receiptHandle, new CompletableFuture<>());
            messages.add(message("duplicate-message", receiptHandle));
        }
        var harness =
                new SafetyHarness(
                        messages,
                        message -> Mono.fromFuture(completions.get(message.receiptHandle())));
        engine = harness.engine(10, 30, 100);
        engine.start(harness.handler());

        var releaseCompletions = new CountDownLatch(1);
        var completionTasks =
                completions.entrySet().stream()
                        .map(
                                entry ->
                                        CompletableFuture.runAsync(
                                                () -> {
                                                    await(releaseCompletions);
                                                    if (EXPECTED_SUCCESSFUL_RECEIPTS.contains(
                                                            entry.getKey())) {
                                                        entry.getValue().complete(null);
                                                    } else {
                                                        entry.getValue()
                                                                .completeExceptionally(
                                                                        new IllegalStateException(
                                                                                "handler failed"));
                                                    }
                                                }))
                        .toArray(CompletableFuture[]::new);

        releaseCompletions.countDown();
        CompletableFuture.allOf(completionTasks).join();
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(harness.unsafeDeletes()).isEmpty();
        assertThat(harness.deletedReceiptHandles())
                .containsExactlyInAnyOrderElementsOf(EXPECTED_SUCCESSFUL_RECEIPTS);
    }

    @Test
    void lateSuccessAfterProcessingTimeoutCannotDeleteMessage() {
        var handlerSink = new AtomicReference<MonoSink<Void>>();
        var harness =
                new SafetyHarness(List.of(message(42)), ignored -> manuallyControlled(handlerSink));
        engine = harness.engine(1, 30, 1);
        engine.start(harness.handler());
        assertThat(handlerSink.get()).isNotNull();

        scheduler.advanceTimeBy(Duration.ofSeconds(1));
        handlerSink.get().success();
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(harness.unsafeDeletes()).isEmpty();
        assertThat(harness.deletedReceiptHandles()).isEmpty();
    }

    @Test
    void lateSuccessAfterForcedShutdownCannotDeleteMessage() {
        var handlerSink = new AtomicReference<MonoSink<Void>>();
        var harness =
                new SafetyHarness(List.of(message(42)), ignored -> manuallyControlled(handlerSink));
        engine = harness.engine(1, 0, 100);
        engine.start(harness.handler());
        assertThat(handlerSink.get()).isNotNull();

        var stopped = engine.stop().toFuture();
        scheduler.advanceTimeBy(Duration.ZERO);
        assertThat(stopped).isCompleted();

        handlerSink.get().success();
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(harness.unsafeDeletes()).isEmpty();
        assertThat(harness.deletedReceiptHandles()).isEmpty();
    }

    @Test
    void synchronousHandlerFailureCannotDeleteMessage() {
        var harness =
                new SafetyHarness(
                        List.of(message(42)),
                        ignored -> {
                            throw new IllegalStateException("handler invocation failed");
                        });
        engine = harness.engine(1, 30, 100);

        engine.start(harness.handler());
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(harness.unsafeDeletes()).isEmpty();
        assertThat(harness.deletedReceiptHandles()).isEmpty();
    }

    @RepeatedTest(25)
    void handlerSuccessRacingForcedShutdownNeverCausesUnsafeDelete() {
        var handlerCompletion = new CompletableFuture<Void>();
        var harness =
                new SafetyHarness(
                        List.of(message(42)), ignored -> Mono.fromFuture(handlerCompletion));
        engine = harness.engine(1, 1, 100);
        engine.start(harness.handler());
        engine.stop().subscribe();

        var startRace = new CountDownLatch(1);
        var forceStop =
                CompletableFuture.runAsync(
                        () -> {
                            await(startRace);
                            scheduler.advanceTimeBy(Duration.ofSeconds(1));
                        });
        var completeHandler =
                CompletableFuture.runAsync(
                        () -> {
                            await(startRace);
                            handlerCompletion.complete(null);
                        });

        startRace.countDown();
        CompletableFuture.allOf(forceStop, completeHandler).join();
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(harness.unsafeDeletes()).isEmpty();
        assertThat(harness.deletedReceiptHandles())
                .allMatch("receipt-42"::equals)
                .hasSizeLessThanOrEqualTo(1);
    }

    @Test
    void deferredDeleteSubscribedAfterVisibilityBudgetExpiryCannotDeleteMessage() {
        var deferredDelete = new AtomicReference<Mono<Void>>();
        var harness = new SafetyHarness(List.of(message(42)), ignored -> Mono.empty());
        engine =
                harness.engine(
                        1,
                        43_200,
                        30,
                        43_200,
                        deletion -> {
                            deferredDelete.set(deletion);
                            return Mono.never();
                        });
        engine.start(harness.handler());
        assertThat(deferredDelete.get()).isNotNull();

        scheduler.advanceTimeBy(Duration.ofSeconds(43_194));
        assertThat(harness.deletedReceiptHandles()).isEmpty();
        scheduler.advanceTimeBy(Duration.ofSeconds(1));

        deferredDelete.get().subscribe();
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(harness.unsafeDeletes()).isEmpty();
        assertThat(harness.deletedReceiptHandles()).isEmpty();
    }

    private static Mono<Void> manuallyControlled(AtomicReference<MonoSink<Void>> sinkReference) {
        return Mono.create(sinkReference::set);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("test interrupted", error);
        }
    }

    private static Message message(int index) {
        return message("message-" + index, "receipt-" + index);
    }

    private static Message message(String messageId, String receiptHandle) {
        return Message.builder()
                .messageId(messageId)
                .receiptHandle(receiptHandle)
                .body("order")
                .build();
    }

    private final class SafetyHarness {
        private final SqsAsyncClient client = mock(SqsAsyncClient.class);
        private final Function<Message, Mono<Void>> delegate;
        private final Set<String> successfulHandlers = ConcurrentHashMap.newKeySet();
        private final List<String> unsafeDeletes = new CopyOnWriteArrayList<>();
        private final List<DeleteMessageBatchRequest> deleteRequests = new CopyOnWriteArrayList<>();

        private SafetyHarness(List<Message> messages, Function<Message, Mono<Void>> delegate) {
            this.delegate = delegate;
            var receiveCalls = new AtomicInteger();
            when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                    .thenAnswer(
                            ignored ->
                                    receiveCalls.getAndIncrement() == 0
                                            ? CompletableFuture.completedFuture(
                                                    ReceiveMessageResponse.builder()
                                                            .messages(messages)
                                                            .build())
                                            : new CompletableFuture<ReceiveMessageResponse>());
            when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                    .thenAnswer(
                            invocation -> {
                                var request =
                                        invocation.getArgument(0, DeleteMessageBatchRequest.class);
                                deleteRequests.add(request);
                                request.entries().stream()
                                        .map(entry -> entry.receiptHandle())
                                        .filter(receipt -> !successfulHandlers.contains(receipt))
                                        .forEach(unsafeDeletes::add);
                                return CompletableFuture.completedFuture(
                                        successfulDeleteResponse(request));
                            });
            when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                    .thenReturn(
                            CompletableFuture.completedFuture(
                                    ChangeMessageVisibilityResponse.builder().build()));
        }

        private SqsListenerEngine engine(
                int maxInFlight, int shutdownGraceSeconds, int maxProcessingDurationSeconds) {
            return engine(
                    maxInFlight,
                    60,
                    shutdownGraceSeconds,
                    maxProcessingDurationSeconds,
                    UnaryOperator.identity());
        }

        private SqsListenerEngine engine(
                int maxInFlight,
                int visibilityTimeoutSeconds,
                int shutdownGraceSeconds,
                int maxProcessingDurationSeconds,
                UnaryOperator<Mono<Void>> deleteGate) {
            return new SqsListenerEngine(
                    client,
                    new SqsListenerEngine.Configuration(
                            "orders",
                            "queue-url",
                            maxInFlight,
                            visibilityTimeoutSeconds,
                            20,
                            shutdownGraceSeconds,
                            maxProcessingDurationSeconds),
                    scheduler,
                    () -> 1.0,
                    Runnable::run,
                    deleteGate);
        }

        private Function<Message, Mono<Void>> handler() {
            return message ->
                    delegate.apply(message)
                            .doOnSuccess(
                                    ignored -> successfulHandlers.add(message.receiptHandle()));
        }

        private List<String> unsafeDeletes() {
            return List.copyOf(unsafeDeletes);
        }

        private List<String> deletedReceiptHandles() {
            return deleteRequests.stream()
                    .flatMap(request -> request.entries().stream())
                    .map(entry -> entry.receiptHandle())
                    .toList();
        }
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
