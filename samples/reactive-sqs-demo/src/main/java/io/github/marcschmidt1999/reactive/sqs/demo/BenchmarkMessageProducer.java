package io.github.marcschmidt1999.reactive.sqs.demo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

/** Bounded-concurrency producer for a repeatable demo listener benchmark. */
final class BenchmarkMessageProducer {

    private static final int MAX_BATCH_SIZE = 10;
    private static final String PAYLOAD_TEXT = "x".repeat(900);

    private final SqsAsyncClient sqs;
    private final String queueUrl;

    BenchmarkMessageProducer(SqsAsyncClient sqs, String queueUrl) {
        this.sqs = Objects.requireNonNull(sqs, "sqs");
        this.queueUrl = Objects.requireNonNull(queueUrl, "queueUrl");
    }

    BenchmarkResult send(int messageCount, int batchConcurrency) {
        validate(messageCount, batchConcurrency);
        var startedAtNanos = System.nanoTime();
        var sent = 0;
        var batches = 0;

        while (sent < messageCount) {
            var requests = new ArrayList<BatchRequest>();
            for (var index = 0; index < batchConcurrency && sent < messageCount; index++) {
                var entries = entries(sent, Math.min(MAX_BATCH_SIZE, messageCount - sent));
                var request =
                        SendMessageBatchRequest.builder()
                                .queueUrl(queueUrl)
                                .entries(entries)
                                .build();
                requests.add(new BatchRequest(entries.size(), sqs.sendMessageBatch(request)));
                sent += entries.size();
                batches++;
            }
            awaitSuccessful(requests);
        }

        return new BenchmarkResult(
                messageCount, batches, Duration.ofNanos(System.nanoTime() - startedAtNanos));
    }

    private static void validate(int messageCount, int batchConcurrency) {
        if (messageCount < 1 || messageCount > 100_000) {
            throw new IllegalArgumentException("messageCount must be between 1 and 100000");
        }
        if (batchConcurrency < 1 || batchConcurrency > 64) {
            throw new IllegalArgumentException("batchConcurrency must be between 1 and 64");
        }
    }

    private static List<SendMessageBatchRequestEntry> entries(int firstIndex, int count) {
        var entries = new ArrayList<SendMessageBatchRequestEntry>(count);
        for (var index = 0; index < count; index++) {
            var messageIndex = firstIndex + index + 1;
            entries.add(
                    SendMessageBatchRequestEntry.builder()
                            .id("benchmark-%06d".formatted(messageIndex))
                            .messageBody(
                                    "{\"id\":\"benchmark-%06d\",\"text\":\"%s\",\"shouldFail\":false}"
                                            .formatted(messageIndex, PAYLOAD_TEXT))
                            .build());
        }
        return entries;
    }

    private static void awaitSuccessful(List<BatchRequest> requests) {
        try {
            for (var request : requests) {
                var response = request.response().join();
                if (!response.failed().isEmpty()) {
                    throw new IllegalStateException("SQS batch failed: " + response.failed());
                }
                if (response.successful().size() != request.entryCount()) {
                    throw new IllegalStateException(
                            "SQS batch returned %d successes for %d entries"
                                    .formatted(response.successful().size(), request.entryCount()));
                }
            }
        } catch (CompletionException exception) {
            throw new IllegalStateException("SQS benchmark batch failed", exception.getCause());
        }
    }

    record BenchmarkResult(int messagesSent, int batchesSent, Duration elapsed) {}

    private record BatchRequest(
            int entryCount,
            java.util.concurrent.CompletableFuture<SendMessageBatchResponse> response) {}
}
