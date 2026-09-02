package io.github.marcschmidt1999.reactive.sqs.soak;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

record SoakReport(
        int totalPrepared,
        List<String> processed,
        List<String> dlq,
        List<String> processedAndDlq,
        List<String> inFlight,
        List<String> missing,
        List<String> sendUncertain,
        List<String> outcomeMismatches,
        List<String> retried) {

    SoakReport {
        processed = immutable(processed);
        dlq = immutable(dlq);
        processedAndDlq = immutable(processedAndDlq);
        inFlight = immutable(inFlight);
        missing = immutable(missing);
        sendUncertain = immutable(sendUncertain);
        outcomeMismatches = immutable(outcomeMismatches);
        retried = immutable(retried);
    }

    static SoakReport classify(
            Collection<AuditRecord> records, Instant now, Duration unresolvedGrace) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(unresolvedGrace, "unresolvedGrace");
        if (unresolvedGrace.isNegative()) {
            throw new IllegalArgumentException("unresolvedGrace must not be negative");
        }

        var processed = new ArrayList<String>();
        var dlq = new ArrayList<String>();
        var both = new ArrayList<String>();
        var inFlight = new ArrayList<String>();
        var missing = new ArrayList<String>();
        var sendUncertain = new ArrayList<String>();
        var mismatches = new ArrayList<String>();
        var retried = new ArrayList<String>();
        var unresolvedCutoff = now.minus(unresolvedGrace);

        for (var record : records) {
            var wasProcessed = record.processedAt() != null;
            var reachedDlq = record.dlqAt() != null;
            if (wasProcessed) {
                processed.add(record.eventId());
            }
            if (reachedDlq) {
                dlq.add(record.eventId());
            }
            if (wasProcessed && reachedDlq) {
                both.add(record.eventId());
            }
            if (record.acceptedAt() == null) {
                sendUncertain.add(record.eventId());
            } else if (!wasProcessed && !reachedDlq) {
                if (record.acceptedAt().isBefore(unresolvedCutoff)) {
                    missing.add(record.eventId());
                } else {
                    inFlight.add(record.eventId());
                }
            }
            if (outcomeMismatch(record.mode(), wasProcessed, reachedDlq)) {
                mismatches.add(record.eventId());
            }
            if (record.attempts() > 1) {
                retried.add(record.eventId());
            }
        }

        return new SoakReport(
                records.size(),
                processed,
                dlq,
                both,
                inFlight,
                missing,
                sendUncertain,
                mismatches,
                retried);
    }

    private static boolean outcomeMismatch(
            SoakMode mode, boolean wasProcessed, boolean reachedDlq) {
        return switch (mode) {
            case NORMAL, RETRY_ONCE -> reachedDlq;
            case POISON -> wasProcessed;
            case BOUNDARY -> false;
        };
    }

    private static List<String> immutable(List<String> values) {
        var sorted = new ArrayList<>(values);
        sorted.sort(String::compareTo);
        return List.copyOf(sorted);
    }
}
