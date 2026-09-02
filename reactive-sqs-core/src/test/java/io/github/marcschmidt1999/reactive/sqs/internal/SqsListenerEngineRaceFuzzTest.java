package io.github.marcschmidt1999.reactive.sqs.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
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

class SqsListenerEngineRaceFuzzTest {

    private static final String SEED_PROPERTY = "reactiveSqs.raceSeed";
    private static final String SEED_ENVIRONMENT_VARIABLE = "REACTIVE_SQS_RACE_SEED";
    private static final String CASES_ENVIRONMENT_VARIABLE = "REACTIVE_SQS_RACE_CASES";
    private static final long DEFAULT_SEED = 0x5A17_E5EEDL;
    private static final int DEFAULT_CASES = 64;
    private static final int DELIVERY_COUNT = 10;
    private static final long PROCESSING_TIMEOUT_MILLIS = 2_000;
    private static final long SHUTDOWN_GRACE_MILLIS = 1_000;
    private static final long[] INTERESTING_DELAYS_MILLIS = {
        0, 1, 499, 500, 999, 1_000, 1_499, 1_500, 1_999, 2_000, 2_001, 2_499, 2_500
    };

    @Test
    void seededInterleavingsNeverDeleteAnUnsafeDelivery() {
        var requestedSeed = requestedSeed();
        if (requestedSeed != null) {
            runSeed(requestedSeed);
            return;
        }

        var seeds = new SplittableRandom(DEFAULT_SEED);
        for (var caseIndex = 0; caseIndex < requestedCases(); caseIndex++) {
            runSeed(seeds.nextLong());
        }
    }

    private static Long requestedSeed() {
        var configured = System.getProperty(SEED_PROPERTY);
        if (configured == null || configured.isBlank()) {
            configured = System.getenv(SEED_ENVIRONMENT_VARIABLE);
        }
        return configured == null || configured.isBlank() ? null : Long.parseLong(configured);
    }

    private static int requestedCases() {
        var configured = System.getenv(CASES_ENVIRONMENT_VARIABLE);
        var cases =
                configured == null || configured.isBlank()
                        ? DEFAULT_CASES
                        : Integer.parseInt(configured);
        if (cases < 1) {
            throw new IllegalArgumentException(CASES_ENVIRONMENT_VARIABLE + " must be at least 1");
        }
        return cases;
    }

    private static void runSeed(long seed) {
        var scenario = new RaceScenario(seed);
        try {
            scenario.run();
        } finally {
            scenario.close();
        }
    }

    private static final class RaceScenario {
        private final long seed;
        private final SplittableRandom random;
        private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        private final SqsAsyncClient client = mock(SqsAsyncClient.class);
        private final Map<String, CompletableFuture<Void>> handlers = new HashMap<>();
        private final Map<String, Long> handlerSuccesses = new ConcurrentHashMap<>();
        private final Map<String, Long> handlerErrors = new ConcurrentHashMap<>();
        private final Map<String, Long> handlerCancellations = new ConcurrentHashMap<>();
        private final Set<String> terminalWithoutDelete = ConcurrentHashMap.newKeySet();
        private final List<DeleteSent> deletes = new CopyOnWriteArrayList<>();
        private final List<ObservedEvent> events = new CopyOnWriteArrayList<>();
        private final AtomicLong eventSequence = new AtomicLong();
        private final AtomicLong stopCompletedSequence = new AtomicLong(-1L);
        private final AtomicBoolean stopRequested = new AtomicBoolean();
        private final AtomicInteger deleteCalls = new AtomicInteger();
        private final List<Message> messages;
        private final List<PlannedAction> actions;
        private final List<DeleteResponsePlan> deleteResponsePlans;
        private final boolean shutdownPlanned;
        private final long shutdownAtMillis;
        private SqsListenerEngine engine;

        private RaceScenario(long seed) {
            this.seed = seed;
            this.random = new SplittableRandom(seed);
            this.messages = createMessages();
            this.shutdownPlanned = random.nextBoolean();
            this.shutdownAtMillis = shutdownPlanned ? 250L + random.nextLong(501) : -1L;
            this.actions = createActionScript();
            this.deleteResponsePlans = createDeleteResponsePlans();
        }

        private void run() {
            configureClient();
            engine =
                    new SqsListenerEngine(
                            client,
                            new SqsListenerEngine.Configuration(
                                    "race-fuzz", "queue-url", DELIVERY_COUNT, 60, 20, 1, 2),
                            scheduler,
                            () -> 1.0);
            engine.start(this::handle);

            scheduler.schedule(
                    this::markPendingAsTerminal,
                    PROCESSING_TIMEOUT_MILLIS + 1,
                    TimeUnit.MILLISECONDS);
            if (shutdownPlanned) {
                scheduler.schedule(
                        this::markPendingAsTerminal,
                        shutdownAtMillis + SHUTDOWN_GRACE_MILLIS + 1,
                        TimeUnit.MILLISECONDS);
            }
            actions.forEach(
                    action ->
                            scheduler.schedule(
                                    action.task(), action.delayMillis(), TimeUnit.MILLISECONDS));

            scheduler.advanceTimeBy(Duration.ofSeconds(5));
            requestStop();
            scheduler.advanceTimeBy(Duration.ofSeconds(2));
            verifyInvariants();
        }

        private Mono<Void> handle(Message message) {
            var receiptHandle = message.receiptHandle();
            return Mono.fromFuture(handlers.get(receiptHandle))
                    .doOnSuccess(
                            ignored ->
                                    handlerSuccesses.putIfAbsent(
                                            receiptHandle,
                                            observe(EventType.HANDLER_SUCCESS, receiptHandle)))
                    .doOnError(
                            ignored ->
                                    handlerErrors.putIfAbsent(
                                            receiptHandle,
                                            observe(EventType.HANDLER_ERROR, receiptHandle)))
                    .doOnCancel(
                            () ->
                                    handlerCancellations.putIfAbsent(
                                            receiptHandle,
                                            observe(EventType.HANDLER_CANCELLED, receiptHandle)));
        }

        private List<Message> createMessages() {
            var result = new ArrayList<Message>(DELIVERY_COUNT);
            for (var index = 0; index < DELIVERY_COUNT; index++) {
                var receiptHandle = "receipt-" + index;
                handlers.put(receiptHandle, new CompletableFuture<>());
                result.add(
                        Message.builder()
                                .messageId("duplicate-message")
                                .receiptHandle(receiptHandle)
                                .body("order-" + index)
                                .build());
            }
            return List.copyOf(result);
        }

        private List<PlannedAction> createActionScript() {
            var result = new ArrayList<PlannedAction>();
            var earlyBoundary = shutdownPlanned ? shutdownAtMillis : 750L;
            addCompletion(result, 0, true, random.nextLong(Math.max(1L, earlyBoundary)));
            addCompletion(result, 1, false, random.nextLong(Math.max(1L, earlyBoundary)));
            addCompletion(
                    result,
                    2,
                    true,
                    shutdownPlanned
                            ? shutdownAtMillis + SHUTDOWN_GRACE_MILLIS + 2
                            : PROCESSING_TIMEOUT_MILLIS + 2);

            var firstRandomDelivery = 3;
            if (shutdownPlanned) {
                addCompletion(
                        result,
                        3,
                        true,
                        shutdownAtMillis + SHUTDOWN_GRACE_MILLIS + 2 + random.nextLong(49));
                result.add(
                        new PlannedAction(
                                shutdownAtMillis,
                                "shutdown@" + shutdownAtMillis,
                                this::requestStop));
                firstRandomDelivery = 4;
            }
            for (var index = firstRandomDelivery; index < DELIVERY_COUNT; index++) {
                addCompletion(result, index, random.nextBoolean(), randomizedBoundaryDelay());
            }
            shuffle(result);
            return List.copyOf(result);
        }

        private void addCompletion(
                List<PlannedAction> result, int index, boolean succeeds, long delayMillis) {
            var receiptHandle = "receipt-" + index;
            var outcome = succeeds ? "success" : "error";
            result.add(
                    new PlannedAction(
                            delayMillis,
                            outcome + "(" + receiptHandle + ")@" + delayMillis,
                            () -> completeHandler(receiptHandle, succeeds)));
        }

        private List<DeleteResponsePlan> createDeleteResponsePlans() {
            var result = new ArrayList<DeleteResponsePlan>(DELIVERY_COUNT);
            for (var index = 0; index < DELIVERY_COUNT; index++) {
                result.add(
                        new DeleteResponsePlan(
                                random.nextLong(51),
                                DeleteResponseMode.values()[random.nextInt(3)]));
            }
            return List.copyOf(result);
        }

        private void configureClient() {
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
                            invocation ->
                                    recordDelete(
                                            invocation.getArgument(
                                                    0, DeleteMessageBatchRequest.class)));
            when(client.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                    .thenReturn(
                            CompletableFuture.completedFuture(
                                    ChangeMessageVisibilityResponse.builder().build()));
        }

        private void completeHandler(String receiptHandle, boolean succeeds) {
            observe(succeeds ? EventType.SIGNAL_SUCCESS : EventType.SIGNAL_ERROR, receiptHandle);
            var handler = handlers.get(receiptHandle);
            if (succeeds) {
                handler.complete(null);
            } else {
                handler.completeExceptionally(new IllegalStateException("planned handler failure"));
            }
        }

        private void markPendingAsTerminal() {
            handlers.keySet().stream()
                    .filter(receipt -> !handlerSuccesses.containsKey(receipt))
                    .forEach(
                            receipt -> {
                                terminalWithoutDelete.add(receipt);
                                observe(EventType.MODEL_TERMINAL_WITHOUT_DELETE, receipt);
                            });
        }

        private CompletableFuture<DeleteMessageBatchResponse> recordDelete(
                DeleteMessageBatchRequest request) {
            request.entries()
                    .forEach(
                            entry -> {
                                var sequence =
                                        observe(EventType.DELETE_SENT, entry.receiptHandle());
                                deletes.add(new DeleteSent(entry.receiptHandle(), sequence));
                            });

            var call = deleteCalls.getAndIncrement();
            var plan = deleteResponsePlans.get(call);
            var result = new CompletableFuture<DeleteMessageBatchResponse>();
            scheduler.schedule(
                    () -> completeDeleteResult(result, request, plan),
                    plan.delayMillis(),
                    TimeUnit.MILLISECONDS);
            return result;
        }

        private void completeDeleteResult(
                CompletableFuture<DeleteMessageBatchResponse> result,
                DeleteMessageBatchRequest request,
                DeleteResponsePlan plan) {
            if (plan.mode() == DeleteResponseMode.ERROR) {
                observe(EventType.DELETE_RESPONSE_ERROR, null);
                result.completeExceptionally(new IllegalStateException("planned delete failure"));
                return;
            }
            var successfulEntries =
                    request.entries().stream()
                            .filter(
                                    entry ->
                                            plan.mode() == DeleteResponseMode.SUCCESS
                                                    || entry.id().hashCode() % 2 == 0)
                            .map(
                                    entry ->
                                            DeleteMessageBatchResultEntry.builder()
                                                    .id(entry.id())
                                                    .build())
                            .toList();
            observe(
                    plan.mode() == DeleteResponseMode.SUCCESS
                            ? EventType.DELETE_RESPONSE_SUCCESS
                            : EventType.DELETE_RESPONSE_PARTIAL,
                    null);
            result.complete(
                    DeleteMessageBatchResponse.builder().successful(successfulEntries).build());
        }

        private void requestStop() {
            if (!stopRequested.compareAndSet(false, true)) {
                return;
            }
            observe(EventType.STOP_REQUESTED, null);
            engine.stop()
                    .doOnSuccess(
                            ignored -> {
                                var sequence = observe(EventType.STOP_COMPLETED, null);
                                stopCompletedSequence.compareAndSet(-1L, sequence);
                            })
                    .subscribe();
        }

        private long observe(EventType type, String receiptHandle) {
            var sequence = eventSequence.incrementAndGet();
            events.add(
                    new ObservedEvent(
                            sequence,
                            scheduler.now(TimeUnit.NANOSECONDS),
                            Thread.currentThread().getName(),
                            type,
                            receiptHandle));
            return sequence;
        }

        private void verifyInvariants() {
            var violations = new ArrayList<String>();
            var knownReceipts = handlers.keySet();
            var deleteCounts = new HashMap<String, Integer>();
            for (var delete : deletes) {
                var receipt = delete.receiptHandle();
                deleteCounts.merge(receipt, 1, Integer::sum);
                if (!knownReceipts.contains(receipt)) {
                    violations.add("unknown receipt deleted: " + receipt);
                }
                var successSequence = handlerSuccesses.get(receipt);
                if (successSequence == null || successSequence >= delete.sequence()) {
                    violations.add("delete did not follow handler success: " + delete);
                }
                if (happenedBefore(handlerErrors.get(receipt), delete.sequence())) {
                    violations.add("handler error was deleted: " + delete);
                }
                if (happenedBefore(handlerCancellations.get(receipt), delete.sequence())) {
                    violations.add("cancelled handler was deleted: " + delete);
                }
                if (terminalWithoutDelete.contains(receipt)) {
                    violations.add("terminal no-delete delivery was deleted: " + delete);
                }
                if (happenedBefore(stopCompletedSequence.get(), delete.sequence())) {
                    violations.add("delete was sent after stop completed: " + delete);
                }
            }
            deleteCounts.forEach(
                    (receipt, count) -> {
                        if (count != 1) {
                            violations.add(receipt + " was deleted " + count + " times");
                        }
                    });
            if (!shutdownPlanned) {
                var deletedReceipts = deletes.stream().map(DeleteSent::receiptHandle).toList();
                if (!Set.copyOf(deletedReceipts).equals(handlerSuccesses.keySet())) {
                    violations.add(
                            "successful deliveries and deleted receipts differ: successful="
                                    + handlerSuccesses.keySet()
                                    + ", deleted="
                                    + deletedReceipts);
                }
            }
            if (stopCompletedSequence.get() < 1L) {
                violations.add("engine stop did not complete");
            }

            assertThat(violations).as(failureContext()).isEmpty();
        }

        private boolean happenedBefore(Long sequence, long boundary) {
            return sequence != null && sequence > 0L && sequence < boundary;
        }

        private boolean happenedBefore(long sequence, long boundary) {
            return sequence > 0L && sequence < boundary;
        }

        private String failureContext() {
            return "seed="
                    + seed
                    + "; replay: "
                    + SEED_ENVIRONMENT_VARIABLE
                    + "="
                    + seed
                    + " ./gradlew :reactive-sqs-core:test --tests '*SqsListenerEngineRaceFuzzTest' --rerun-tasks"
                    + "; actions="
                    + actions.stream().map(PlannedAction::description).toList()
                    + "; deleteResponses="
                    + deleteResponsePlans
                    + "; events="
                    + events;
        }

        private long randomizedBoundaryDelay() {
            if (random.nextInt(3) == 0) {
                return INTERESTING_DELAYS_MILLIS[random.nextInt(INTERESTING_DELAYS_MILLIS.length)];
            }
            return random.nextLong(2_501);
        }

        private void shuffle(List<PlannedAction> values) {
            for (var index = values.size() - 1; index > 0; index--) {
                Collections.swap(values, index, random.nextInt(index + 1));
            }
        }

        private void close() {
            if (engine != null) {
                requestStop();
                scheduler.advanceTimeBy(Duration.ofSeconds(2));
            }
            scheduler.dispose();
        }
    }

    private enum EventType {
        SIGNAL_SUCCESS,
        SIGNAL_ERROR,
        HANDLER_SUCCESS,
        HANDLER_ERROR,
        HANDLER_CANCELLED,
        MODEL_TERMINAL_WITHOUT_DELETE,
        DELETE_SENT,
        DELETE_RESPONSE_SUCCESS,
        DELETE_RESPONSE_PARTIAL,
        DELETE_RESPONSE_ERROR,
        STOP_REQUESTED,
        STOP_COMPLETED
    }

    private enum DeleteResponseMode {
        SUCCESS,
        PARTIAL,
        ERROR
    }

    private record PlannedAction(long delayMillis, String description, Runnable task) {}

    private record DeleteResponsePlan(long delayMillis, DeleteResponseMode mode) {}

    private record DeleteSent(String receiptHandle, long sequence) {}

    private record ObservedEvent(
            long sequence,
            long virtualNanos,
            String thread,
            EventType type,
            String receiptHandle) {}
}
