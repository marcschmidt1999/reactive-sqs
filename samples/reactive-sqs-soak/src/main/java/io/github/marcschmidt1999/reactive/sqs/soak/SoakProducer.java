package io.github.marcschmidt1999.reactive.sqs.soak;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

final class SoakProducer {

    private static final int MAX_BATCH_SIZE = 10;
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofMillis(100);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(5);

    private final SqsAsyncClient sqs;
    private final AuditStore auditStore;
    private final ObjectMapper objectMapper;
    private final String queueUrl;
    private final RetryDelay retryDelay;
    private final Clock clock;

    SoakProducer(
            SqsAsyncClient sqs,
            AuditStore auditStore,
            ObjectMapper objectMapper,
            String queueUrl,
            RetryDelay retryDelay) {
        this(sqs, auditStore, objectMapper, queueUrl, retryDelay, Clock.systemUTC());
    }

    SoakProducer(
            SqsAsyncClient sqs,
            AuditStore auditStore,
            ObjectMapper objectMapper,
            String queueUrl,
            RetryDelay retryDelay,
            Clock clock) {
        this.sqs = Objects.requireNonNull(sqs, "sqs");
        this.auditStore = Objects.requireNonNull(auditStore, "auditStore");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.queueUrl = required(queueUrl, "queueUrl");
        this.retryDelay = Objects.requireNonNull(retryDelay, "retryDelay");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    void startRun(String runId, Instant startedAt, Instant expiresAt) {
        auditStore.startRun(runId, startedAt, expiresAt).join();
    }

    void checkpointRun(
            String runId,
            long expectedAcceptedCount,
            long lastSequence,
            Instant at,
            boolean finished) {
        auditStore.checkpointRun(runId, expectedAcceptedCount, lastSequence, at, finished).join();
    }

    void publish(List<SoakMessage> messages, Instant preparedAt, Instant expiresAt) {
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty() || messages.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("messages must contain between 1 and 10 entries");
        }
        for (var message : messages) {
            auditStore.prepare(message, preparedAt, expiresAt).join();
        }
        sendPrepared(messages);
    }

    private void sendPrepared(List<SoakMessage> messages) {
        var pending = new LinkedHashMap<String, SoakMessage>();
        messages.forEach(message -> pending.put(message.eventId(), message));
        var delay = INITIAL_RETRY_DELAY;
        while (!pending.isEmpty()) {
            var attempted = new LinkedHashMap<>(pending);
            SendMessageBatchResponse response;
            try {
                response =
                        sqs.sendMessageBatch(
                                        SendMessageBatchRequest.builder()
                                                .queueUrl(queueUrl)
                                                .entries(entries(attempted.values()))
                                                .build())
                                .join();
            } catch (CompletionException exception) {
                // A response may have been lost after SQS accepted the batch. Retrying the same
                // logical event IDs can create physical duplicates, which the ledger measures.
                retryDelay.pause(delay);
                delay = increased(delay);
                continue;
            }
            for (var success : response.successful()) {
                var message = attempted.get(success.id());
                if (message == null) {
                    throw new IllegalStateException(
                            "SQS returned an unknown successful batch id: " + success.id());
                }
                markAcceptedUntilRecorded(message, success.messageId());
                pending.remove(success.id());
            }
            for (var failure : response.failed()) {
                if (!attempted.containsKey(failure.id())) {
                    throw new IllegalStateException(
                            "SQS returned an unknown failed batch id: " + failure.id());
                }
                if (failure.senderFault()) {
                    throw new IllegalStateException(
                            "SQS rejected event %s: %s %s"
                                    .formatted(failure.id(), failure.code(), failure.message()));
                }
            }
            if (!pending.isEmpty()) {
                retryDelay.pause(delay);
                delay = increased(delay);
            }
        }
    }

    private void markAcceptedUntilRecorded(SoakMessage message, String sqsMessageId) {
        var delay = INITIAL_RETRY_DELAY;
        while (true) {
            try {
                auditStore.markAccepted(message, sqsMessageId, clock.instant()).join();
                return;
            } catch (CompletionException exception) {
                // SQS definitely accepted this physical message. Retry only its ledger write;
                // resending here could let a duplicate mask loss of the confirmed copy.
                retryDelay.pause(delay);
                delay = increased(delay);
            }
        }
    }

    private static Duration increased(Duration delay) {
        var increased = delay.multipliedBy(2);
        return increased.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : increased;
    }

    private List<SendMessageBatchRequestEntry> entries(java.util.Collection<SoakMessage> messages) {
        var entries = new ArrayList<SendMessageBatchRequestEntry>(messages.size());
        for (var message : messages) {
            entries.add(
                    SendMessageBatchRequestEntry.builder()
                            .id(message.eventId())
                            .messageBody(json(message))
                            .build());
        }
        return entries;
    }

    private String json(SoakMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Could not serialize soak event " + message.eventId(), exception);
        }
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
