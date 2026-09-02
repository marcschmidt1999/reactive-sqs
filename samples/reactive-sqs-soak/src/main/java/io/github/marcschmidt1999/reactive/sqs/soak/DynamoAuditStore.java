package io.github.marcschmidt1999.reactive.sqs.soak;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

final class DynamoAuditStore implements AuditStore {

    private static final Map<String, String> EVENT_ID_NAME = Map.of("#eventId", "eventId");

    private final DynamoDbAsyncClient client;
    private final String tableName;

    DynamoAuditStore(DynamoDbAsyncClient client, String tableName) {
        this.client = Objects.requireNonNull(client, "client");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        if (tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
    }

    @Override
    public CompletableFuture<Void> startRun(String runId, Instant at, Instant expiresAt) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("runId", string(runId));
        item.put("eventId", string("!manifest"));
        item.put("recordType", string("MANIFEST"));
        item.put("startedAtMs", number(epochMillis(at)));
        item.put("expectedAcceptedCount", number(0));
        item.put(
                "expiresAt",
                number(Objects.requireNonNull(expiresAt, "expiresAt").getEpochSecond()));
        var request =
                PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .conditionExpression("attribute_not_exists(#eventId)")
                        .expressionAttributeNames(EVENT_ID_NAME)
                        .build();
        return client.putItem(request).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> checkpointRun(
            String runId,
            long expectedAcceptedCount,
            long lastSequence,
            Instant at,
            boolean finished) {
        if (expectedAcceptedCount < 0 || lastSequence < 0) {
            throw new IllegalArgumentException("manifest counts must not be negative");
        }
        var expression =
                "SET expectedAcceptedCount = :count, lastSequence = :sequence, checkpointAtMs = :at";
        if (finished) {
            expression += ", producerFinishedAtMs = :at";
        }
        var request =
                UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(key(runId, "!manifest"))
                        .conditionExpression("attribute_exists(#eventId)")
                        .updateExpression(expression)
                        .expressionAttributeNames(EVENT_ID_NAME)
                        .expressionAttributeValues(
                                Map.of(
                                        ":count", number(expectedAcceptedCount),
                                        ":sequence", number(lastSequence),
                                        ":at", number(epochMillis(at))))
                        .build();
        return client.updateItem(request).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> prepare(SoakMessage message, Instant at, Instant expiresAt) {
        Objects.requireNonNull(message, "message");
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("runId", string(message.runId()));
        item.put("eventId", string(message.eventId()));
        item.put("recordType", string("EVENT"));
        item.put("sequence", number(message.sequence()));
        item.put("mode", string(message.mode().name()));
        item.put("cpuMillis", number(message.cpuMillis()));
        item.put("seed", number(message.seed()));
        item.put("preparedAtMs", number(epochMillis(at)));
        item.put(
                "expiresAt",
                number(Objects.requireNonNull(expiresAt, "expiresAt").getEpochSecond()));
        var request =
                PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .conditionExpression("attribute_not_exists(#eventId)")
                        .expressionAttributeNames(EVENT_ID_NAME)
                        .build();
        return client.putItem(request).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> markAccepted(
            SoakMessage message, String sqsMessageId, Instant at) {
        return update(
                message,
                "SET acceptedAtMs = if_not_exists(acceptedAtMs, :at), sqsMessageId = :sqsMessageId",
                Map.of(
                        ":at", number(epochMillis(at)),
                        ":sqsMessageId", string(sqsMessageId)));
    }

    @Override
    public CompletableFuture<Void> markAttempt(
            SoakMessage message, String sqsMessageId, int receiveCount, Instant at) {
        if (receiveCount < 1) {
            throw new IllegalArgumentException("receiveCount must be positive");
        }
        return update(
                message,
                "SET lastAttemptAtMs = :at, lastReceiveCount = :receiveCount ADD attemptKeys :attemptKey",
                Map.of(
                        ":at", number(epochMillis(at)),
                        ":receiveCount", number(receiveCount),
                        ":attemptKey",
                                AttributeValue.fromSs(List.of(sqsMessageId + "#" + receiveCount))));
    }

    @Override
    public CompletableFuture<Void> markProcessed(SoakMessage message, String checksum, Instant at) {
        return update(
                message,
                "SET processedAtMs = if_not_exists(processedAtMs, :at), processingChecksum = :checksum",
                Map.of(":at", number(epochMillis(at)), ":checksum", string(checksum)));
    }

    @Override
    public CompletableFuture<Void> markDlq(
            SoakMessage message, String sqsMessageId, int receiveCount, Instant at) {
        if (receiveCount < 1) {
            throw new IllegalArgumentException("receiveCount must be positive");
        }
        return update(
                message,
                "SET dlqAtMs = if_not_exists(dlqAtMs, :at), lastDlqReceiveCount = :receiveCount ADD dlqMessageIds :messageId",
                Map.of(
                        ":at", number(epochMillis(at)),
                        ":receiveCount", number(receiveCount),
                        ":messageId", AttributeValue.fromSs(List.of(sqsMessageId))));
    }

    @Override
    public CompletableFuture<List<AuditRecord>> records(String runId) {
        Objects.requireNonNull(runId, "runId");
        if (runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        var records = new ArrayList<AuditRecord>();
        return queryPage(runId, Map.of(), records).thenApply(ignored -> List.copyOf(records));
    }

    private CompletableFuture<Void> update(
            SoakMessage message,
            String updateExpression,
            Map<String, AttributeValue> expressionValues) {
        Objects.requireNonNull(message, "message");
        var request =
                UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(key(message.runId(), message.eventId()))
                        .conditionExpression("attribute_exists(#eventId)")
                        .updateExpression(updateExpression)
                        .expressionAttributeNames(EVENT_ID_NAME)
                        .expressionAttributeValues(expressionValues)
                        .build();
        return client.updateItem(request).thenApply(ignored -> null);
    }

    private CompletableFuture<Void> queryPage(
            String runId,
            Map<String, AttributeValue> exclusiveStartKey,
            List<AuditRecord> destination) {
        var request =
                QueryRequest.builder()
                        .tableName(tableName)
                        .consistentRead(true)
                        .keyConditionExpression("#runId = :runId")
                        .expressionAttributeNames(Map.of("#runId", "runId"))
                        .expressionAttributeValues(Map.of(":runId", string(runId)))
                        .exclusiveStartKey(exclusiveStartKey)
                        .build();
        return client.query(request)
                .thenCompose(
                        response -> {
                            response.items().stream()
                                    .filter(DynamoAuditStore::isEvent)
                                    .map(DynamoAuditStore::record)
                                    .forEach(destination::add);
                            if (response.lastEvaluatedKey().isEmpty()) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return queryPage(runId, response.lastEvaluatedKey(), destination);
                        });
    }

    private static AuditRecord record(Map<String, AttributeValue> item) {
        return new AuditRecord(
                requiredString(item, "eventId"),
                SoakMode.valueOf(requiredString(item, "mode")),
                requiredInstant(item, "preparedAtMs"),
                optionalInstant(item, "acceptedAtMs"),
                optionalInstant(item, "processedAtMs"),
                optionalInstant(item, "dlqAtMs"),
                item.getOrDefault("attemptKeys", AttributeValue.fromSs(List.of())).ss().size());
    }

    private static boolean isEvent(Map<String, AttributeValue> item) {
        var recordType = item.get("recordType");
        return recordType == null || !"MANIFEST".equals(recordType.s());
    }

    private static Map<String, AttributeValue> key(String runId, String eventId) {
        return Map.of("runId", string(runId), "eventId", string(eventId));
    }

    private static AttributeValue string(String value) {
        Objects.requireNonNull(value, "attribute value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("string attribute must not be blank");
        }
        return AttributeValue.fromS(value);
    }

    private static AttributeValue number(long value) {
        return AttributeValue.fromN(Long.toString(value));
    }

    private static long epochMillis(Instant value) {
        return Objects.requireNonNull(value, "timestamp").toEpochMilli();
    }

    private static String requiredString(Map<String, AttributeValue> item, String name) {
        var value = item.get(name);
        if (value == null || value.s() == null) {
            throw new IllegalStateException("Audit record is missing " + name);
        }
        return value.s();
    }

    private static Instant requiredInstant(Map<String, AttributeValue> item, String name) {
        var value = optionalInstant(item, name);
        if (value == null) {
            throw new IllegalStateException("Audit record is missing " + name);
        }
        return value;
    }

    private static Instant optionalInstant(Map<String, AttributeValue> item, String name) {
        var value = item.get(name);
        return value == null ? null : Instant.ofEpochMilli(Long.parseLong(value.n()));
    }
}
