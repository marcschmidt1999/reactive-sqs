package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

class DynamoAuditStoreTest {

    private static final Instant NOW = Instant.parse("2026-09-02T12:00:00Z");
    private static final SoakMessage MESSAGE =
            new SoakMessage("run-1", "event-1", 7, SoakMode.NORMAL, 25, 42);

    @Test
    void runManifestMakesTheExpectedAcceptedCountDurableAndIdempotent() {
        var client = mock(DynamoDbAsyncClient.class);
        when(client.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));
        when(client.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));
        var store = new DynamoAuditStore(client, "ledger");

        store.startRun("run-1", NOW, NOW.plusSeconds(86_400)).join();
        store.checkpointRun("run-1", 10, 123, NOW.plusSeconds(1), false).join();
        store.checkpointRun("run-1", 20, 133, NOW.plusSeconds(2), true).join();

        var put = ArgumentCaptor.forClass(PutItemRequest.class);
        var updates = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).putItem(put.capture());
        verify(client, org.mockito.Mockito.times(2)).updateItem(updates.capture());
        assertThat(put.getValue().item())
                .containsEntry("eventId", AttributeValue.fromS("!manifest"))
                .containsEntry("recordType", AttributeValue.fromS("MANIFEST"))
                .containsEntry("expectedAcceptedCount", AttributeValue.fromN("0"));
        assertThat(updates.getAllValues().stream().map(UpdateItemRequest::updateExpression))
                .containsExactly(
                        "SET expectedAcceptedCount = :count, lastSequence = :sequence, checkpointAtMs = :at",
                        "SET expectedAcceptedCount = :count, lastSequence = :sequence, checkpointAtMs = :at, producerFinishedAtMs = :at");
    }

    @Test
    void writesIndependentAuditFactsWithoutReplacingTheLedgerItem() {
        var client = mock(DynamoDbAsyncClient.class);
        when(client.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));
        when(client.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));
        var store = new DynamoAuditStore(client, "ledger");

        store.prepare(MESSAGE, NOW, NOW.plusSeconds(86_400)).join();
        store.markAccepted(MESSAGE, "sqs-1", NOW.plusSeconds(1)).join();
        store.markAttempt(MESSAGE, "sqs-1", 2, NOW.plusSeconds(2)).join();
        store.markProcessed(MESSAGE, "checksum", NOW.plusSeconds(3)).join();
        store.markDlq(MESSAGE, "sqs-dlq-1", 5, NOW.plusSeconds(4)).join();

        var put = ArgumentCaptor.forClass(PutItemRequest.class);
        @SuppressWarnings("unchecked")
        var updates = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).putItem(put.capture());
        verify(client, org.mockito.Mockito.times(4)).updateItem(updates.capture());
        assertThat(
                        new WriteSummary(
                                put.getValue().conditionExpression(),
                                put.getValue().item().keySet(),
                                updates.getAllValues().stream()
                                        .map(UpdateItemRequest::updateExpression)
                                        .toList()))
                .isEqualTo(
                        new WriteSummary(
                                "attribute_not_exists(#eventId)",
                                java.util.Set.of(
                                        "runId",
                                        "eventId",
                                        "recordType",
                                        "sequence",
                                        "mode",
                                        "cpuMillis",
                                        "seed",
                                        "preparedAtMs",
                                        "expiresAt"),
                                List.of(
                                        "SET acceptedAtMs = if_not_exists(acceptedAtMs, :at), sqsMessageId = :sqsMessageId",
                                        "SET lastAttemptAtMs = :at, lastReceiveCount = :receiveCount ADD attemptKeys :attemptKey",
                                        "SET processedAtMs = if_not_exists(processedAtMs, :at), processingChecksum = :checksum",
                                        "SET dlqAtMs = if_not_exists(dlqAtMs, :at), lastDlqReceiveCount = :receiveCount ADD dlqMessageIds :messageId")));
    }

    @Test
    void readsEveryStronglyConsistentPageAndMapsAuditFacts() {
        var client = mock(DynamoDbAsyncClient.class);
        var continuation = Map.of("eventId", AttributeValue.fromS("event-1"));
        when(client.query(any(QueryRequest.class)))
                .thenAnswer(
                        invocation -> {
                            QueryRequest request = invocation.getArgument(0);
                            if (request.exclusiveStartKey().isEmpty()) {
                                return CompletableFuture.completedFuture(
                                        QueryResponse.builder()
                                                .items(
                                                        List.of(
                                                                item(
                                                                        "event-1", "NORMAL", 1_000,
                                                                        2_000, 3_000L, null, "a",
                                                                        "b")))
                                                .lastEvaluatedKey(continuation)
                                                .build());
                            }
                            return CompletableFuture.completedFuture(
                                    QueryResponse.builder()
                                            .items(
                                                    List.of(
                                                            item(
                                                                    "event-2", "POISON", 1_100,
                                                                    2_100, null, 4_100L, "c")))
                                            .build());
                        });
        var store = new DynamoAuditStore(client, "ledger");

        var records = store.records("run-1").join();

        var requests = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client, org.mockito.Mockito.times(2)).query(requests.capture());
        assertThat(records)
                .containsExactly(
                        new AuditRecord(
                                "event-1",
                                SoakMode.NORMAL,
                                Instant.ofEpochMilli(1_000),
                                Instant.ofEpochMilli(2_000),
                                Instant.ofEpochMilli(3_000),
                                null,
                                2),
                        new AuditRecord(
                                "event-2",
                                SoakMode.POISON,
                                Instant.ofEpochMilli(1_100),
                                Instant.ofEpochMilli(2_100),
                                null,
                                Instant.ofEpochMilli(4_100),
                                1));
        assertThat(requests.getAllValues())
                .allSatisfy(request -> assertThat(request.consistentRead()).isTrue());
    }

    private static Map<String, AttributeValue> item(
            String eventId,
            String mode,
            long preparedAt,
            long acceptedAt,
            Long processedAt,
            Long dlqAt,
            String... attemptKeys) {
        var item = new java.util.HashMap<String, AttributeValue>();
        item.put("eventId", AttributeValue.fromS(eventId));
        item.put("mode", AttributeValue.fromS(mode));
        item.put("preparedAtMs", AttributeValue.fromN(Long.toString(preparedAt)));
        item.put("acceptedAtMs", AttributeValue.fromN(Long.toString(acceptedAt)));
        item.put("attemptKeys", AttributeValue.fromSs(List.of(attemptKeys)));
        if (processedAt != null) {
            item.put("processedAtMs", AttributeValue.fromN(Long.toString(processedAt)));
        }
        if (dlqAt != null) {
            item.put("dlqAtMs", AttributeValue.fromN(Long.toString(dlqAt)));
        }
        return Map.copyOf(item);
    }

    private record WriteSummary(
            String putCondition,
            java.util.Set<String> putAttributes,
            List<String> updateExpressions) {}
}
