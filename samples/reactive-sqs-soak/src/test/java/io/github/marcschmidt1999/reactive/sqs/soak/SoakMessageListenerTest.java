package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.marcschmidt1999.reactive.sqs.SqsMessage;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

class SoakMessageListenerTest {

    private static final Instant NOW = Instant.parse("2026-09-02T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void successfulHandlerWaitsForDurableProcessedAudit() {
        var audit = new RecordingAuditStore();
        var processedWrite = new CompletableFuture<Void>();
        audit.processedResult = processedWrite;
        var listener = listener(audit);

        var completion = listener.consumeSource(message(SoakMode.NORMAL, 1)).toFuture();

        assertThat(completion).isNotDone();
        assertThat(audit.events).containsExactly("attempt:1", "processed:checksum");
        processedWrite.complete(null);
        completion.join();
    }

    @Test
    void failedProcessedAuditFailsTheHandler() {
        var audit = new RecordingAuditStore();
        audit.processedResult =
                CompletableFuture.failedFuture(new IllegalStateException("dynamo unavailable"));

        var completion = listener(audit).consumeSource(message(SoakMode.NORMAL, 1)).toFuture();

        assertThatThrownBy(completion::join)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("dynamo unavailable");
    }

    @Test
    void retryOnceOnlyCompletesAfterTheSecondDelivery() {
        var audit = new RecordingAuditStore();
        var listener = listener(audit);

        assertThatThrownBy(() -> listener.consumeSource(message(SoakMode.RETRY_ONCE, 1)).block())
                .isInstanceOf(ExpectedSoakFailure.class);
        listener.consumeSource(message(SoakMode.RETRY_ONCE, 2)).block();

        assertThat(audit.events).containsExactly("attempt:1", "attempt:2", "processed:checksum");
    }

    @Test
    void poisonDeliveryNeverRecordsSuccessfulProcessing() {
        var audit = new RecordingAuditStore();

        assertThatThrownBy(() -> listener(audit).consumeSource(message(SoakMode.POISON, 7)).block())
                .isInstanceOf(ExpectedSoakFailure.class);
        assertThat(audit.events).containsExactly("attempt:7");
    }

    @Test
    void dlqHandlerWaitsForDurableDlqAudit() {
        var audit = new RecordingAuditStore();
        var dlqWrite = new CompletableFuture<Void>();
        audit.dlqResult = dlqWrite;

        var completion = listener(audit).consumeDlq(message(SoakMode.POISON, 6)).toFuture();

        assertThat(completion).isNotDone();
        assertThat(audit.events).containsExactly("dlq:6");
        dlqWrite.complete(null);
        completion.join();
    }

    @Test
    void rejectsMessagesFromAnotherRunWithoutAcknowledgingThem() {
        var audit = new RecordingAuditStore();
        var payload = new SoakMessage("another-run", "event-1", 1, SoakMode.NORMAL, 10, 42);
        var raw = Message.builder().messageId("sqs-message-1").receiptHandle("handle").build();

        assertThatThrownBy(
                        () ->
                                listener(audit)
                                        .consumeSource(new SqsMessage<>(payload, "queue-url", raw))
                                        .block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("another-run");
        assertThat(audit.events).isEmpty();
    }

    private static SoakMessageListener listener(RecordingAuditStore audit) {
        return new SoakMessageListener(
                audit, (millis, seed) -> "checksum", Schedulers.immediate(), CLOCK, "run-1");
    }

    private static SqsMessage<SoakMessage> message(SoakMode mode, int receiveCount) {
        var payload = new SoakMessage("run-1", "event-1", 1, mode, 10, 42);
        var raw =
                Message.builder()
                        .messageId("sqs-message-1")
                        .receiptHandle("not-persisted")
                        .attributes(
                                java.util.Map.of(
                                        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT,
                                        Integer.toString(receiveCount)))
                        .build();
        return new SqsMessage<>(payload, "queue-url", raw);
    }

    private static final class RecordingAuditStore implements AuditStore {

        private final List<String> events = new ArrayList<>();
        private CompletableFuture<Void> processedResult = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> dlqResult = CompletableFuture.completedFuture(null);

        @Override
        public CompletableFuture<Void> startRun(String runId, Instant at, Instant expiresAt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> checkpointRun(
                String runId,
                long expectedAcceptedCount,
                long lastSequence,
                Instant at,
                boolean finished) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> prepare(SoakMessage message, Instant at, Instant expiresAt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> markAccepted(
                SoakMessage message, String sqsMessageId, Instant at) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> markAttempt(
                SoakMessage message, String sqsMessageId, int receiveCount, Instant at) {
            events.add("attempt:" + receiveCount);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> markProcessed(
                SoakMessage message, String checksum, Instant at) {
            events.add("processed:" + checksum);
            return processedResult;
        }

        @Override
        public CompletableFuture<Void> markDlq(
                SoakMessage message, String sqsMessageId, int receiveCount, Instant at) {
            events.add("dlq:" + receiveCount);
            return dlqResult;
        }

        @Override
        public CompletableFuture<List<AuditRecord>> records(String runId) {
            throw new UnsupportedOperationException();
        }
    }
}
