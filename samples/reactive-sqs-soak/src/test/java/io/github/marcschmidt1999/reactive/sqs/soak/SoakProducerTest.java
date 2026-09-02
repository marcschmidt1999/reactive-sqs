package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

class SoakProducerTest {

    @Test
    void startsEveryPrepareWriteBeforeWaitingForTheBatch() {
        var sqs = mock(SqsAsyncClient.class);
        var prepareCalls = new AtomicInteger();
        var firstPrepare =
                new JoinGuardFuture(
                        () -> prepareCalls.get() == 2,
                        "producer waited before starting every prepare write");
        var audit =
                new ControllableAuditStore(
                        ignored -> {
                            if (prepareCalls.incrementAndGet() == 1) {
                                return firstPrepare;
                            }
                            firstPrepare.complete(null);
                            return CompletableFuture.completedFuture(null);
                        },
                        (ignored, ignoredId) -> CompletableFuture.completedFuture(null));
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                SendMessageBatchResponse.builder()
                                        .successful(
                                                success("event-1", "sqs-1"),
                                                success("event-2", "sqs-2"))
                                        .build()));
        var producer = new SoakProducer(sqs, audit, new ObjectMapper(), "queue-url", ignored -> {});

        publish(producer, twoMessages());

        assertThat(prepareCalls).hasValue(2);
        verify(sqs).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void doesNotSendWhenAnyPrepareWriteFails() {
        var sqs = mock(SqsAsyncClient.class);
        var audit =
                new ControllableAuditStore(
                        message ->
                                message.eventId().equals("event-2")
                                        ? CompletableFuture.failedFuture(
                                                new IllegalStateException("Dynamo unavailable"))
                                        : CompletableFuture.completedFuture(null),
                        (ignored, ignoredId) -> CompletableFuture.completedFuture(null));

        assertThatThrownBy(
                        () ->
                                publish(
                                        new SoakProducer(
                                                sqs,
                                                audit,
                                                new ObjectMapper(),
                                                "queue-url",
                                                ignored -> {}),
                                        twoMessages()))
                .isInstanceOf(CompletionException.class);
        verifyNoInteractions(sqs);
    }

    @Test
    void startsEveryAcceptanceWriteBeforeWaitingForTheBatch() {
        var sqs = mock(SqsAsyncClient.class);
        var acceptanceCalls = new AtomicInteger();
        var firstAcceptance =
                new JoinGuardFuture(
                        () -> acceptanceCalls.get() == 2,
                        "producer waited before starting every acceptance write");
        var audit =
                new ControllableAuditStore(
                        ignored -> CompletableFuture.completedFuture(null),
                        (ignored, ignoredId) -> {
                            if (acceptanceCalls.incrementAndGet() == 1) {
                                return firstAcceptance;
                            }
                            firstAcceptance.complete(null);
                            return CompletableFuture.completedFuture(null);
                        });
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                SendMessageBatchResponse.builder()
                                        .successful(
                                                success("event-1", "sqs-1"),
                                                success("event-2", "sqs-2"))
                                        .build()));
        var producer = new SoakProducer(sqs, audit, new ObjectMapper(), "queue-url", ignored -> {});

        publish(producer, twoMessages());

        assertThat(acceptanceCalls).hasValue(2);
        verify(sqs).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void publishesTheTenMessageHighRateBatchAsOneAuditedSqsRequest() {
        var sqs = mock(SqsAsyncClient.class);
        var prepareCalls = new AtomicInteger();
        var acceptanceCalls = new AtomicInteger();
        var sentBatchSize = new AtomicInteger();
        var audit =
                new ControllableAuditStore(
                        ignored -> {
                            prepareCalls.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        },
                        (ignored, ignoredId) -> {
                            acceptanceCalls.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        });
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            SendMessageBatchRequest request = invocation.getArgument(0);
                            sentBatchSize.set(request.entries().size());
                            var successes =
                                    request.entries().stream()
                                            .map(entry -> success(entry.id(), "sqs-" + entry.id()))
                                            .toList();
                            return CompletableFuture.completedFuture(
                                    SendMessageBatchResponse.builder()
                                            .successful(successes)
                                            .build());
                        });
        var producer = new SoakProducer(sqs, audit, new ObjectMapper(), "queue-url", ignored -> {});
        var messages =
                java.util.stream.IntStream.rangeClosed(1, 10)
                        .mapToObj(
                                sequence ->
                                        new SoakMessage(
                                                "run-1",
                                                "event-" + sequence,
                                                sequence,
                                                SoakMode.NORMAL,
                                                10,
                                                42))
                        .toList();

        publish(producer, messages);

        assertThat(sentBatchSize).hasValue(10);
        assertThat(prepareCalls).hasValue(10);
        assertThat(acceptanceCalls).hasValue(10);
    }

    @ParameterizedTest
    @EnumSource(AcceptanceFailure.class)
    void retriesAcceptanceAuditWithoutResendingAnSqsConfirmedEntry(AcceptanceFailure failure) {
        var sqs = mock(SqsAsyncClient.class);
        var message = new SoakMessage("run-1", "event-1", 1, SoakMode.NORMAL, 10, 42);
        var pauses = new ArrayList<Duration>();
        var acceptanceAttempts = new AtomicInteger();
        var audit =
                new ControllableAuditStore(
                        ignored -> CompletableFuture.completedFuture(null),
                        (ignored, ignoredId) -> {
                            if (acceptanceAttempts.getAndIncrement() == 0) {
                                return failure.fail();
                            }
                            return CompletableFuture.completedFuture(null);
                        });
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                SendMessageBatchResponse.builder()
                                        .successful(success("event-1", "sqs-1"))
                                        .build()));
        var producer = new SoakProducer(sqs, audit, new ObjectMapper(), "queue-url", pauses::add);

        producer.publish(
                List.of(message),
                Instant.parse("2026-09-02T12:00:00Z"),
                Instant.parse("2026-10-02T12:00:00Z"));

        verify(sqs, times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
        assertThat(acceptanceAttempts).hasValue(2);
        assertThat(pauses).containsExactly(Duration.ofMillis(100));
    }

    @Test
    void retriesOnlyFailedAcceptanceWritesWithoutResendingTheBatch() {
        var sqs = mock(SqsAsyncClient.class);
        var pauses = new ArrayList<Duration>();
        var firstAttempts = new AtomicInteger();
        var secondAttempts = new AtomicInteger();
        var audit =
                new ControllableAuditStore(
                        ignored -> CompletableFuture.completedFuture(null),
                        (message, ignoredId) -> {
                            if (message.eventId().equals("event-1")) {
                                return firstAttempts.getAndIncrement() == 0
                                        ? CompletableFuture.failedFuture(
                                                new IllegalStateException("Dynamo unavailable"))
                                        : CompletableFuture.completedFuture(null);
                            }
                            secondAttempts.incrementAndGet();
                            return CompletableFuture.completedFuture(null);
                        });
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                SendMessageBatchResponse.builder()
                                        .successful(
                                                success("event-1", "sqs-1"),
                                                success("event-2", "sqs-2"))
                                        .build()));
        var producer = new SoakProducer(sqs, audit, new ObjectMapper(), "queue-url", pauses::add);

        publish(producer, twoMessages());

        assertThat(firstAttempts).hasValue(2);
        assertThat(secondAttempts).hasValue(1);
        assertThat(pauses).containsExactly(Duration.ofMillis(100));
        verify(sqs, times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void recordsKnownSqsAcceptancesBeforeRejectingAnUnknownResponseEntry() {
        var events = new ArrayList<String>();
        var audit = new ProducerAuditStore(events);
        var sqs = mock(SqsAsyncClient.class);
        var message = new SoakMessage("run-1", "event-1", 1, SoakMode.NORMAL, 10, 42);
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                SendMessageBatchResponse.builder()
                                        .successful(
                                                success("event-1", "sqs-1"),
                                                success("unknown", "sqs-unknown"))
                                        .build()));
        var producer = new SoakProducer(sqs, audit, new ObjectMapper(), "queue-url", ignored -> {});

        assertThatThrownBy(
                        () ->
                                producer.publish(
                                        List.of(message),
                                        Instant.parse("2026-09-02T12:00:00Z"),
                                        Instant.parse("2026-10-02T12:00:00Z")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SQS returned an unknown successful batch id: unknown");
        assertThat(events).contains("accepted:event-1:sqs-1");
    }

    @Test
    void recordsAcceptanceBeforeRejectingAResponseWithoutAnSqsMessageId() {
        var events = new ArrayList<String>();
        var audit = new ProducerAuditStore(events);
        var sqs = mock(SqsAsyncClient.class);
        var message = new SoakMessage("run-1", "event-1", 1, SoakMode.NORMAL, 10, 42);
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                SendMessageBatchResponse.builder()
                                        .successful(success("event-1", null))
                                        .build()));
        var producer = new SoakProducer(sqs, audit, new ObjectMapper(), "queue-url", ignored -> {});

        assertThatThrownBy(
                        () ->
                                producer.publish(
                                        List.of(message),
                                        Instant.parse("2026-09-02T12:00:00Z"),
                                        Instant.parse("2026-10-02T12:00:00Z")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SQS returned a blank message id for successful batch id: event-1");
        assertThat(events).contains("accepted:event-1:!missing-sqs-message-id");
    }

    @Test
    void preparesBeforeSendingAndRetriesOnlyFailedBatchEntries() {
        var events = new ArrayList<String>();
        var audit = new ProducerAuditStore(events);
        var sqs = mock(SqsAsyncClient.class);
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            SendMessageBatchRequest request = invocation.getArgument(0);
                            events.add(
                                    "send:"
                                            + request.entries().stream()
                                                    .map(entry -> entry.id())
                                                    .toList());
                            if (events.stream().filter(value -> value.startsWith("send:")).count()
                                    == 1) {
                                return CompletableFuture.completedFuture(
                                        SendMessageBatchResponse.builder()
                                                .successful(success("event-1", "sqs-1"))
                                                .failed(failure("event-2"))
                                                .build());
                            }
                            return CompletableFuture.completedFuture(
                                    SendMessageBatchResponse.builder()
                                            .successful(success("event-2", "sqs-2"))
                                            .build());
                        });
        var producer =
                new SoakProducer(
                        sqs,
                        audit,
                        new ObjectMapper(),
                        "queue-url",
                        duration -> events.add("backoff:" + duration.toMillis()));
        var first = new SoakMessage("run-1", "event-1", 1, SoakMode.NORMAL, 10, 42);
        var second = new SoakMessage("run-1", "event-2", 2, SoakMode.NORMAL, 20, 42);

        producer.publish(
                List.of(first, second),
                Instant.parse("2026-09-02T12:00:00Z"),
                Instant.parse("2026-10-02T12:00:00Z"));

        assertThat(events)
                .containsExactly(
                        "prepare:event-1",
                        "prepare:event-2",
                        "send:[event-1, event-2]",
                        "accepted:event-1:sqs-1",
                        "backoff:100",
                        "send:[event-2]",
                        "accepted:event-2:sqs-2");
    }

    private static void publish(SoakProducer producer, List<SoakMessage> messages) {
        producer.publish(
                messages,
                Instant.parse("2026-09-02T12:00:00Z"),
                Instant.parse("2026-10-02T12:00:00Z"));
    }

    private static List<SoakMessage> twoMessages() {
        return List.of(
                new SoakMessage("run-1", "event-1", 1, SoakMode.NORMAL, 10, 42),
                new SoakMessage("run-1", "event-2", 2, SoakMode.NORMAL, 20, 42));
    }

    private static SendMessageBatchResultEntry success(String id, String messageId) {
        return SendMessageBatchResultEntry.builder().id(id).messageId(messageId).build();
    }

    private static BatchResultErrorEntry failure(String id) {
        return BatchResultErrorEntry.builder()
                .id(id)
                .code("InternalError")
                .message("retry")
                .senderFault(false)
                .build();
    }

    private enum AcceptanceFailure {
        ASYNCHRONOUS,
        SYNCHRONOUS;

        private CompletableFuture<Void> fail() {
            var cause = new IllegalStateException("Dynamo unavailable");
            if (this == SYNCHRONOUS) {
                throw new CompletionException(cause);
            }
            return CompletableFuture.failedFuture(cause);
        }
    }

    private static final class JoinGuardFuture extends CompletableFuture<Void> {

        private final BooleanSupplier joinAllowed;
        private final String failureMessage;

        private JoinGuardFuture(BooleanSupplier joinAllowed, String failureMessage) {
            this.joinAllowed = joinAllowed;
            this.failureMessage = failureMessage;
        }

        @Override
        public Void join() {
            if (!joinAllowed.getAsBoolean()) {
                throw new AssertionError(failureMessage);
            }
            return super.join();
        }
    }

    private static final class ControllableAuditStore extends NoOpAuditStore {

        private final Function<SoakMessage, CompletableFuture<Void>> prepare;
        private final BiFunction<SoakMessage, String, CompletableFuture<Void>> markAccepted;

        private ControllableAuditStore(
                Function<SoakMessage, CompletableFuture<Void>> prepare,
                BiFunction<SoakMessage, String, CompletableFuture<Void>> markAccepted) {
            this.prepare = prepare;
            this.markAccepted = markAccepted;
        }

        @Override
        public CompletableFuture<Void> prepare(SoakMessage message, Instant at, Instant expiresAt) {
            return prepare.apply(message);
        }

        @Override
        public CompletableFuture<Void> markAccepted(
                SoakMessage message, String sqsMessageId, Instant at) {
            return markAccepted.apply(message, sqsMessageId);
        }
    }

    private static final class ProducerAuditStore extends NoOpAuditStore {

        private final List<String> events;

        private ProducerAuditStore(List<String> events) {
            this.events = events;
        }

        @Override
        public CompletableFuture<Void> prepare(SoakMessage message, Instant at, Instant expiresAt) {
            events.add("prepare:" + message.eventId());
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> markAccepted(
                SoakMessage message, String sqsMessageId, Instant at) {
            events.add("accepted:" + message.eventId() + ":" + sqsMessageId);
            return CompletableFuture.completedFuture(null);
        }
    }

    private abstract static class NoOpAuditStore implements AuditStore {

        @Override
        public CompletableFuture<Void> startRun(String runId, Instant at, Instant expiresAt) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> checkpointRun(
                String runId,
                long expectedAcceptedCount,
                long lastSequence,
                Instant at,
                boolean finished) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> markAttempt(
                SoakMessage message, String sqsMessageId, int receiveCount, Instant at) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> markProcessed(
                SoakMessage message, String checksum, Instant at) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> markDlq(
                SoakMessage message, String sqsMessageId, int receiveCount, Instant at) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<List<AuditRecord>> records(String runId) {
            return CompletableFuture.completedFuture(List.of());
        }
    }
}
