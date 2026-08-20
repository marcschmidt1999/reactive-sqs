package io.github.marcschmidt1999.reactive.sqs.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.test.scheduler.VirtualTimeScheduler;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

class SqsDeleteBatcherTest {

    private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

    @AfterEach
    void disposeScheduler() {
        scheduler.dispose();
    }

    @Test
    void batchesAcknowledgementsThatArriveWithinTheBatchWindow() {
        var client = mock(SqsAsyncClient.class);
        var request = new CompletableFuture<DeleteMessageBatchRequest>();
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var batch = invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            request.complete(batch);
                            return CompletableFuture.completedFuture(
                                    DeleteMessageBatchResponse.builder()
                                            .successful(
                                                    batch.entries().stream()
                                                            .map(
                                                                    entry ->
                                                                            DeleteMessageBatchResultEntry
                                                                                    .builder()
                                                                                    .id(entry.id())
                                                                                    .build())
                                                            .toList())
                                            .build());
                        });
        var batcher = new SqsDeleteBatcher(client, "queue-url", scheduler);
        var completions = new AtomicInteger();

        var first = batcher.delete(message("receipt-1"));
        var second = batcher.delete(message("receipt-2"));

        first.subscribe(null, error -> {}, completions::incrementAndGet);
        second.subscribe(null, error -> {}, completions::incrementAndGet);
        verifyNoInteractions(client);

        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(request).isCompleted();
        assertThat(request.join().queueUrl()).isEqualTo("queue-url");
        assertThat(request.join().entries())
                .extracting(entry -> entry.receiptHandle())
                .containsExactlyInAnyOrder("receipt-1", "receipt-2");
        assertThat(request.join().entries())
                .extracting(entry -> entry.id())
                .doesNotHaveDuplicates();
        assertThat(completions).hasValue(2);
        verify(client).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void listenerAcknowledgementsUseOneDeleteBatchRequest() {
        var client = mock(SqsAsyncClient.class);
        var request = new CompletableFuture<DeleteMessageBatchRequest>();
        var pendingReceive = new CompletableFuture<ReceiveMessageResponse>();
        var receiveCalls = new AtomicInteger();
        var batchEvents = new CopyOnWriteArrayList<SqsListenerTelemetry.DeleteBatchCompleted>();
        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenAnswer(
                        ignored ->
                                receiveCalls.getAndIncrement() == 0
                                        ? CompletableFuture.completedFuture(
                                                ReceiveMessageResponse.builder()
                                                        .messages(
                                                                message("receipt-1"),
                                                                message("receipt-2"))
                                                        .build())
                                        : pendingReceive);
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var batch = invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            request.complete(batch);
                            return CompletableFuture.completedFuture(
                                    DeleteMessageBatchResponse.builder()
                                            .successful(
                                                    batch.entries().stream()
                                                            .map(
                                                                    entry ->
                                                                            DeleteMessageBatchResultEntry
                                                                                    .builder()
                                                                                    .id(entry.id())
                                                                                    .build())
                                                            .toList())
                                            .build());
                        });
        var engine =
                new SqsListenerEngine(
                        client,
                        new SqsListenerEngine.Configuration(
                                "orders", "queue-url", 8, 60, 20, 1, 30),
                        scheduler,
                        new SqsListenerTelemetry() {
                            @Override
                            public void deleteBatchCompleted(DeleteBatchCompleted event) {
                                batchEvents.add(event);
                            }
                        },
                        () -> 1.0,
                        Runnable::run,
                        java.util.function.UnaryOperator.identity());

        engine.start(ignored -> reactor.core.publisher.Mono.empty());
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(request).isCompleted();
        assertThat(request.join().entries())
                .extracting(entry -> entry.receiptHandle())
                .containsExactlyInAnyOrder("receipt-1", "receipt-2");
        assertThat(batchEvents)
                .extracting(SqsListenerTelemetry.DeleteBatchCompleted::outcome)
                .containsExactly(SqsListenerTelemetry.DeleteBatchOutcome.SUCCESS);

        engine.stop().subscribe();
    }

    @Test
    void completesOnlyReceiptsConfirmedByAPartiallySuccessfulBatchResponse() {
        var client = mock(SqsAsyncClient.class);
        var firstCompleted = new CompletableFuture<Void>();
        var secondFailure = new AtomicReference<Throwable>();
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var batch = invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            return CompletableFuture.completedFuture(
                                    DeleteMessageBatchResponse.builder()
                                            .successful(
                                                    DeleteMessageBatchResultEntry.builder()
                                                            .id(batch.entries().getFirst().id())
                                                            .build())
                                            .failed(
                                                    BatchResultErrorEntry.builder()
                                                            .id(batch.entries().get(1).id())
                                                            .code("ReceiptHandleIsInvalid")
                                                            .senderFault(true)
                                                            .build())
                                            .build());
                        });
        var batcher = new SqsDeleteBatcher(client, "queue-url", scheduler);

        batcher.delete(message("receipt-1"))
                .subscribe(
                        null,
                        firstCompleted::completeExceptionally,
                        () -> firstCompleted.complete(null));
        batcher.delete(message("receipt-2")).subscribe(null, secondFailure::set);
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(firstCompleted).isCompleted();
        assertThat(secondFailure.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("did not confirm deletion");
    }

    @Test
    void doesNotSendAnAcknowledgementCancelledBeforeTheBatchWindowCloses() {
        var client = mock(SqsAsyncClient.class);
        var request = new CompletableFuture<DeleteMessageBatchRequest>();
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var batch = invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            request.complete(batch);
                            return CompletableFuture.completedFuture(
                                    DeleteMessageBatchResponse.builder()
                                            .successful(
                                                    batch.entries().stream()
                                                            .map(
                                                                    entry ->
                                                                            DeleteMessageBatchResultEntry
                                                                                    .builder()
                                                                                    .id(entry.id())
                                                                                    .build())
                                                            .toList())
                                            .build());
                        });
        var batcher = new SqsDeleteBatcher(client, "queue-url", scheduler);

        var cancelled = batcher.delete(message("receipt-cancelled")).subscribe();
        batcher.delete(message("receipt-live")).subscribe();
        cancelled.dispose();
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(request).isCompleted();
        assertThat(request.join().entries())
                .extracting(entry -> entry.receiptHandle())
                .containsExactly("receipt-live");
    }

    @Test
    void reportsTheActualDeleteBatchRequestAndItsEntryCount() {
        var client = mock(SqsAsyncClient.class);
        var events = new CopyOnWriteArrayList<SqsListenerTelemetry.DeleteBatchCompleted>();
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation ->
                                CompletableFuture.completedFuture(
                                        successfulResponse(
                                                invocation.getArgument(
                                                        0, DeleteMessageBatchRequest.class))));
        var batcher = new SqsDeleteBatcher(client, "queue-url", scheduler, events::add);

        batcher.delete(message("receipt-1")).subscribe();
        batcher.delete(message("receipt-2")).subscribe();
        scheduler.advanceTimeBy(SqsDeleteBatcher.MAX_BATCH_WAIT);

        assertThat(events)
                .containsExactly(
                        new SqsListenerTelemetry.DeleteBatchCompleted(
                                java.time.Duration.ZERO,
                                2,
                                SqsListenerTelemetry.DeleteBatchOutcome.SUCCESS));
    }

    @Test
    void flushesImmediatelyAtTheSqsMaximumBatchSize() {
        var client = mock(SqsAsyncClient.class);
        var request = new CompletableFuture<DeleteMessageBatchRequest>();
        when(client.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var batch = invocation.getArgument(0, DeleteMessageBatchRequest.class);
                            request.complete(batch);
                            return CompletableFuture.completedFuture(successfulResponse(batch));
                        });
        var batcher = new SqsDeleteBatcher(client, "queue-url", scheduler);

        for (var index = 0; index < SqsDeleteBatcher.MAX_BATCH_SIZE; index++) {
            batcher.delete(message("receipt-" + index)).subscribe();
        }

        assertThat(request).isCompleted();
        assertThat(request.join().entries()).hasSize(SqsDeleteBatcher.MAX_BATCH_SIZE);
    }

    private static Message message(String receiptHandle) {
        return Message.builder().receiptHandle(receiptHandle).build();
    }

    private static DeleteMessageBatchResponse successfulResponse(
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
