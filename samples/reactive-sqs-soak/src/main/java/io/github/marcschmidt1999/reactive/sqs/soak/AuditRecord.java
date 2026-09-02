package io.github.marcschmidt1999.reactive.sqs.soak;

import java.time.Instant;
import java.util.Objects;

record AuditRecord(
        String eventId,
        SoakMode mode,
        Instant preparedAt,
        Instant acceptedAt,
        Instant processedAt,
        Instant dlqAt,
        int attempts) {

    AuditRecord {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(preparedAt, "preparedAt");
        if (eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (attempts < 0) {
            throw new IllegalArgumentException("attempts must not be negative");
        }
    }
}
