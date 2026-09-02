package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class SoakReportTest {

    private static final Instant NOW = Instant.parse("2026-09-02T12:00:00Z");

    @Test
    void classifiesEveryPreparedMessageFromIndependentAuditFacts() {
        var records =
                List.of(
                        record("processed", SoakMode.NORMAL, acceptedAgo(60), NOW, null, 1),
                        record("dlq", SoakMode.POISON, acceptedAgo(60), null, NOW, 5),
                        record("both", SoakMode.BOUNDARY, acceptedAgo(60), NOW, NOW, 2),
                        record("young", SoakMode.NORMAL, acceptedAgo(10), null, null, 1),
                        record("missing", SoakMode.NORMAL, acceptedAgo(600), null, null, 1),
                        record("uncertain", SoakMode.NORMAL, null, null, null, 0),
                        record("normal-in-dlq", SoakMode.NORMAL, acceptedAgo(60), null, NOW, 5),
                        record("poison-processed", SoakMode.POISON, acceptedAgo(60), NOW, null, 1));

        var report = SoakReport.classify(records, NOW, Duration.ofMinutes(5));

        assertThat(report)
                .isEqualTo(
                        new SoakReport(
                                8,
                                List.of("both", "poison-processed", "processed"),
                                List.of("both", "dlq", "normal-in-dlq"),
                                List.of("both"),
                                List.of("young"),
                                List.of("missing"),
                                List.of("uncertain"),
                                List.of("normal-in-dlq", "poison-processed"),
                                List.of("both", "dlq", "normal-in-dlq")));
    }

    private static AuditRecord record(
            String eventId,
            SoakMode mode,
            Instant acceptedAt,
            Instant processedAt,
            Instant dlqAt,
            int attempts) {
        return new AuditRecord(
                eventId, mode, NOW.minusSeconds(700), acceptedAt, processedAt, dlqAt, attempts);
    }

    private static Instant acceptedAgo(long seconds) {
        return NOW.minusSeconds(seconds);
    }
}
