package io.github.marcschmidt1999.reactive.sqs.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

class BenchmarkMessageProducerTest {

    private static final String QUEUE_URL = "https://queue.example.test/reactive-sqs-demo-test";

    @Test
    void sendsRequestedWorkloadInSuccessfulBatches() {
        var requests = new CopyOnWriteArrayList<SendMessageBatchRequest>();
        var sqs = mock(SqsAsyncClient.class);
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var request = invocation.getArgument(0, SendMessageBatchRequest.class);
                            requests.add(request);
                            var successful =
                                    request.entries().stream()
                                            .map(
                                                    entry ->
                                                            SendMessageBatchResultEntry.builder()
                                                                    .id(entry.id())
                                                                    .messageId("sqs-" + entry.id())
                                                                    .build())
                                            .toList();
                            return CompletableFuture.completedFuture(
                                    SendMessageBatchResponse.builder()
                                            .successful(successful)
                                            .build());
                        });

        var result = new BenchmarkMessageProducer(sqs, QUEUE_URL).send(25, 2);

        assertThat(result.messagesSent()).isEqualTo(25);
        assertThat(result.batchesSent()).isEqualTo(3);
        assertThat(requests).hasSize(3);
        assertThat(requests)
                .allSatisfy(request -> assertThat(request.queueUrl()).isEqualTo(QUEUE_URL));
        assertThat(requests.stream().map(request -> request.entries().size()).toList())
                .containsExactlyInAnyOrder(10, 10, 5);
        assertThat(requests.stream().flatMap(request -> request.entries().stream()))
                .allSatisfy(
                        entry ->
                                assertThat(entry.messageBody())
                                        .contains("\"shouldFail\":false")
                                        .hasSizeGreaterThan(900));
    }

    @Test
    void rejectsASqsBatchWithFailedEntries() {
        var sqs = mock(SqsAsyncClient.class);
        when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                SendMessageBatchResponse.builder()
                                        .failed(
                                                BatchResultErrorEntry.builder()
                                                        .id("benchmark-000001")
                                                        .code("AccessDenied")
                                                        .build())
                                        .build()));

        assertThatThrownBy(() -> new BenchmarkMessageProducer(sqs, QUEUE_URL).send(1, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("AccessDenied");
    }
}
