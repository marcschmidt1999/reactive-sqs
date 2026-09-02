package io.github.marcschmidt1999.reactive.sqs.soak;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

interface AuditStore {

    CompletableFuture<Void> startRun(String runId, Instant at, Instant expiresAt);

    CompletableFuture<Void> checkpointRun(
            String runId,
            long expectedAcceptedCount,
            long lastSequence,
            Instant at,
            boolean finished);

    CompletableFuture<Void> prepare(SoakMessage message, Instant at, Instant expiresAt);

    CompletableFuture<Void> markAccepted(SoakMessage message, String sqsMessageId, Instant at);

    CompletableFuture<Void> markAttempt(
            SoakMessage message, String sqsMessageId, int receiveCount, Instant at);

    CompletableFuture<Void> markProcessed(SoakMessage message, String checksum, Instant at);

    CompletableFuture<Void> markDlq(
            SoakMessage message, String sqsMessageId, int receiveCount, Instant at);

    CompletableFuture<List<AuditRecord>> records(String runId);
}
