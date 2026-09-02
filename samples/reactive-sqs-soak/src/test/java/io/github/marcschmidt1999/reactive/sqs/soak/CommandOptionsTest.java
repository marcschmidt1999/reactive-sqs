package io.github.marcschmidt1999.reactive.sqs.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class CommandOptionsTest {

    @Test
    void producerOptionsHaveSafeDefaultsAndAcceptExplicitBounds() {
        var defaults =
                SoakProducerMain.ProducerOptions.parse(
                        required("--region=eu-central-1", "--run-id=run-1"));
        var explicit =
                SoakProducerMain.ProducerOptions.parse(
                        required(
                                "--messages-per-second=10",
                                "--duration=P7D",
                                "--ttl-days=365",
                                "--seed=-7"));

        assertThat(defaults)
                .isEqualTo(
                        new SoakProducerMain.ProducerOptions(
                                "eu-central-1",
                                "queue-url",
                                "ledger",
                                "run-1",
                                2,
                                Duration.ofHours(24),
                                30,
                                42));
        assertThat(explicit.messagesPerSecond()).isEqualTo(10);
        assertThat(explicit.duration()).isEqualTo(Duration.ofDays(7));
        assertThat(explicit.ttlDays()).isEqualTo(365);
        assertThat(explicit.seed()).isEqualTo(-7);
    }

    @Test
    void producerOptionsRejectUnsafeOrIncompleteInput() {
        assertThatThrownBy(() -> SoakProducerMain.ProducerOptions.parse(new String[0]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                SoakProducerMain.ProducerOptions.parse(
                                        required("--messages-per-second=0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 10");
        assertThatThrownBy(
                        () -> SoakProducerMain.ProducerOptions.parse(required("--duration=PT0S")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duration");
        assertThatThrownBy(() -> SoakProducerMain.ProducerOptions.parse(required("--ttl-days=1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ttl-days");
        assertThatThrownBy(
                        () ->
                                SoakProducerMain.ProducerOptions.parse(
                                        required("--messages-per-second=nope")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid integer");
        assertThatThrownBy(
                        () -> SoakProducerMain.ProducerOptions.parse(required("--unexpected=true")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void reportOptionsOnlySupportExplicitlyLiveSnapshots() {
        var live = SoakReportMain.ReportOptions.parse(reportRequired());

        assertThat(live.region()).isEqualTo("eu-central-1");
        assertThat(live.tableName()).isEqualTo("ledger");
        assertThat(live.runId()).isEqualTo("run-1");
        assertThat(live.unresolvedGrace()).isEqualTo(Duration.ofMinutes(5));
        assertThatThrownBy(
                        () ->
                                SoakReportMain.ReportOptions.parse(
                                        new String[] {
                                            "--region=eu-central-1",
                                            "--table=ledger",
                                            "--run-id=run-1",
                                            "--final"
                                        }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void reportOptionsRejectInvalidInput() {
        assertThatThrownBy(() -> SoakReportMain.ReportOptions.parse(new String[0]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                SoakReportMain.ReportOptions.parse(
                                        new String[] {
                                            "--region=eu-central-1",
                                            "--table=ledger",
                                            "--run-id=run-1",
                                            "--grace=PT0S"
                                        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("grace");
        assertThatThrownBy(
                        () ->
                                SoakReportMain.ReportOptions.parse(
                                        new String[] {
                                            "--region=eu-central-1",
                                            "--table=ledger",
                                            "--run-id=run-1",
                                            "--unknown"
                                        }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static String[] required(String... replacements) {
        var values =
                new java.util.ArrayList<>(
                        java.util.List.of(
                                "--region=eu-central-1",
                                "--queue-url=queue-url",
                                "--table=ledger",
                                "--run-id=run-1"));
        for (var replacement : replacements) {
            var name = replacement.substring(0, replacement.indexOf('=') + 1);
            values.removeIf(value -> value.startsWith(name));
            values.add(replacement);
        }
        return values.toArray(String[]::new);
    }

    private static String[] reportRequired() {
        return new String[] {"--region=eu-central-1", "--table=ledger", "--run-id=run-1"};
    }
}
