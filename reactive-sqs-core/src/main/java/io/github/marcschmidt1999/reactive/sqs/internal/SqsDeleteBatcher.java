package io.github.marcschmidt1999.reactive.sqs.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;

/** Batches acknowledgement deletes for one listener and one queue. */
final class SqsDeleteBatcher {

    static final int MAX_BATCH_SIZE = 10;
    static final Duration MAX_BATCH_WAIT = Duration.ofMillis(5);

    private final SqsAsyncClient sqsClient;
    private final String queueUrl;
    private final Scheduler scheduler;
    private final Consumer<SqsListenerTelemetry.DeleteBatchCompleted> batchCompleted;
    private final Object monitor = new Object();
    private final List<PendingDelete> pendingDeletes = new ArrayList<>();
    private final AtomicLong entrySequence = new AtomicLong();

    private Disposable scheduledFlush = Disposables.disposed();

    SqsDeleteBatcher(SqsAsyncClient sqsClient, String queueUrl, Scheduler scheduler) {
        this(sqsClient, queueUrl, scheduler, ignored -> {});
    }

    SqsDeleteBatcher(
            SqsAsyncClient sqsClient,
            String queueUrl,
            Scheduler scheduler,
            Consumer<SqsListenerTelemetry.DeleteBatchCompleted> batchCompleted) {
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient");
        this.queueUrl = Objects.requireNonNull(queueUrl, "queueUrl");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.batchCompleted = Objects.requireNonNull(batchCompleted, "batchCompleted");
    }

    Mono<Void> delete(Message message) {
        Objects.requireNonNull(message, "message");
        return Mono.create(
                sink -> {
                    var pendingDelete =
                            new PendingDelete(
                                    nextEntryId(),
                                    Objects.requireNonNull(message.receiptHandle()),
                                    sink);
                    sink.onCancel(() -> cancel(pendingDelete));
                    enqueue(pendingDelete);
                });
    }

    void flushNow() {
        flush();
    }

    private String nextEntryId() {
        return "delete-" + entrySequence.incrementAndGet();
    }

    private void enqueue(PendingDelete pendingDelete) {
        var flushImmediately = false;
        synchronized (monitor) {
            if (pendingDelete.state.get() == PendingState.PENDING) {
                pendingDeletes.add(pendingDelete);
                if (pendingDeletes.size() == 1) {
                    scheduleFlushLocked();
                }
                flushImmediately = pendingDeletes.size() >= MAX_BATCH_SIZE;
            }
        }
        if (flushImmediately) {
            flush();
        }
    }

    private void cancel(PendingDelete pendingDelete) {
        synchronized (monitor) {
            if (pendingDelete.state.compareAndSet(PendingState.PENDING, PendingState.CANCELLED)) {
                pendingDeletes.remove(pendingDelete);
                if (pendingDeletes.isEmpty()) {
                    scheduledFlush.dispose();
                    scheduledFlush = Disposables.disposed();
                }
            }
        }
    }

    private void scheduleFlushLocked() {
        scheduledFlush =
                scheduler.schedule(
                        this::flush,
                        MAX_BATCH_WAIT.toMillis(),
                        java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void flush() {
        var batch = new ArrayList<PendingDelete>(MAX_BATCH_SIZE);
        synchronized (monitor) {
            scheduledFlush.dispose();
            scheduledFlush = Disposables.disposed();
            while (!pendingDeletes.isEmpty() && batch.size() < MAX_BATCH_SIZE) {
                var pendingDelete = pendingDeletes.remove(0);
                if (pendingDelete.state.compareAndSet(PendingState.PENDING, PendingState.SENT)) {
                    batch.add(pendingDelete);
                }
            }
            if (!pendingDeletes.isEmpty()) {
                scheduleFlushLocked();
            }
        }
        if (batch.isEmpty()) {
            return;
        }

        var request =
                DeleteMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(
                                batch.stream()
                                        .map(
                                                pendingDelete ->
                                                        DeleteMessageBatchRequestEntry.builder()
                                                                .id(pendingDelete.entryId)
                                                                .receiptHandle(
                                                                        pendingDelete.receiptHandle)
                                                                .build())
                                        .toList())
                        .build();
        var startedNanos = scheduler.now(java.util.concurrent.TimeUnit.NANOSECONDS);
        try {
            sqsClient
                    .deleteMessageBatch(request)
                    .whenComplete(
                            (response, error) -> complete(batch, response, error, startedNanos));
        } catch (RuntimeException error) {
            complete(batch, null, error, startedNanos);
        }
    }

    private void complete(
            List<PendingDelete> batch,
            DeleteMessageBatchResponse response,
            Throwable error,
            long startedNanos) {
        if (error != null) {
            batch.forEach(pendingDelete -> pendingDelete.failure(error));
            batchCompleted.accept(
                    new SqsListenerTelemetry.DeleteBatchCompleted(
                            elapsedSince(startedNanos),
                            batch.size(),
                            SqsListenerTelemetry.DeleteBatchOutcome.ERROR));
            return;
        }
        if (response == null) {
            batch.forEach(
                    pendingDelete ->
                            pendingDelete.failure(
                                    new IllegalStateException(
                                            "SQS returned no delete batch response")));
            batchCompleted.accept(
                    new SqsListenerTelemetry.DeleteBatchCompleted(
                            elapsedSince(startedNanos),
                            batch.size(),
                            SqsListenerTelemetry.DeleteBatchOutcome.ERROR));
            return;
        }
        Set<String> successfulIds = new HashSet<>();
        response.successful().forEach(entry -> successfulIds.add(entry.id()));
        var successfulCount = new AtomicLong();
        batch.forEach(
                pendingDelete -> {
                    if (successfulIds.contains(pendingDelete.entryId)) {
                        pendingDelete.success();
                        successfulCount.incrementAndGet();
                    } else {
                        pendingDelete.failure(
                                new IllegalStateException(
                                        "SQS did not confirm deletion of batch entry "
                                                + pendingDelete.entryId));
                    }
                });
        batchCompleted.accept(
                new SqsListenerTelemetry.DeleteBatchCompleted(
                        elapsedSince(startedNanos),
                        batch.size(),
                        successfulCount.get() == batch.size()
                                ? SqsListenerTelemetry.DeleteBatchOutcome.SUCCESS
                                : SqsListenerTelemetry.DeleteBatchOutcome.PARTIAL_FAILURE));
    }

    private Duration elapsedSince(long startedNanos) {
        return Duration.ofNanos(
                Math.max(
                        0L,
                        scheduler.now(java.util.concurrent.TimeUnit.NANOSECONDS) - startedNanos));
    }

    private enum PendingState {
        PENDING,
        SENT,
        CANCELLED,
        COMPLETED
    }

    private static final class PendingDelete {
        private final String entryId;
        private final String receiptHandle;
        private final MonoSink<Void> sink;
        private final AtomicReference<PendingState> state =
                new AtomicReference<>(PendingState.PENDING);

        private PendingDelete(String entryId, String receiptHandle, MonoSink<Void> sink) {
            this.entryId = entryId;
            this.receiptHandle = receiptHandle;
            this.sink = sink;
        }

        private void success() {
            if (state.compareAndSet(PendingState.SENT, PendingState.COMPLETED)) {
                sink.success();
            }
        }

        private void failure(Throwable error) {
            if (state.compareAndSet(PendingState.SENT, PendingState.COMPLETED)) {
                sink.error(error);
            }
        }
    }
}
