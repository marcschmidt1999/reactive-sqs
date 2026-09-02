package io.github.marcschmidt1999.reactive.sqs.soak;

import java.util.Objects;

/** Stable logical event carried by the soak-test queue. */
public record SoakMessage(
        String runId, String eventId, long sequence, SoakMode mode, int cpuMillis, long seed) {

    public SoakMessage {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(mode, "mode");
        if (runId.isBlank() || eventId.isBlank()) {
            throw new IllegalArgumentException("runId and eventId must not be blank");
        }
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence must be positive");
        }
        if (cpuMillis < 1) {
            throw new IllegalArgumentException("cpuMillis must be positive");
        }
    }
}
