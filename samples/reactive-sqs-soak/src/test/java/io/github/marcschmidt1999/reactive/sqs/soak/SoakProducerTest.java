package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

class SoakProducerTest {

    @Test
    void retriesAcceptanceAuditWithoutResendingAnSqsConfirmedEntry() {
        var audit = mock(AuditStore.class);
        var sqs = mock(SqsAsyncClient.class);
        var message = new SoakMessage("run-1", "event-1", 1, SoakMode.NORMAL, 10, 42);
        var pauses = new ArrayList<Duration>();
        var acceptanceAttempts = new AtomicInteger();
        when(audit.prepare(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(audit.markAccepted(any(), any(), any()))
                .thenAnswer(
                        ignored ->
                                acceptanceAttempts.getAndIncrement() == 0
                                        ? CompletableFuture.failedFuture(
                                                new IllegalStateException("Dynamo down"))
                                        : CompletableFuture.completedFuture(null));
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
        verify(audit, times(2)).markAccepted(any(), any(), any());
        assertThat(pauses).containsExactly(Duration.ofMillis(100));
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

    private static final class ProducerAuditStore implements AuditStore {

        private final List<String> events;

        private ProducerAuditStore(List<String> events) {
            this.events = events;
        }

        @Override
        public CompletableFuture<Void> startRun(String runId, Instant at, Instant expiresAt) {
            events.add("start:" + runId);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> checkpointRun(
                String runId,
                long expectedAcceptedCount,
                long lastSequence,
                Instant at,
                boolean finished) {
            events.add("checkpoint:" + expectedAcceptedCount + ":" + finished);
            return CompletableFuture.completedFuture(null);
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

        @Override
        public CompletableFuture<Void> markAttempt(
                SoakMessage message, String sqsMessageId, int receiveCount, Instant at) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> markProcessed(
                SoakMessage message, String checksum, Instant at) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> markDlq(
                SoakMessage message, String sqsMessageId, int receiveCount, Instant at) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<List<AuditRecord>> records(String runId) {
            throw new UnsupportedOperationException();
        }
    }
}
