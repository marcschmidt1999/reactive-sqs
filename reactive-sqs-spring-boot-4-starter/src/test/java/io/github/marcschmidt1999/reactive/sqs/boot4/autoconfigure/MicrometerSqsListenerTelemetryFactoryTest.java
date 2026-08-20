package io.github.marcschmidt1999.reactive.sqs.boot4.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerEngine;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class MicrometerSqsListenerTelemetryFactoryTest {

    @Test
    void olderConcurrentStateCannotOverwriteNewerTerminalGauges() {
        var registry = new SimpleMeterRegistry();
        var telemetry =
                new MicrometerSqsListenerTelemetryFactory(registry)
                        .create(
                                new SqsListenerEngine.Configuration(
                                        "orders", "queue-url", 1, 60, 20, 30, 100));

        telemetry.stateChanged(new SqsListenerTelemetry.ListenerState(2L, 0, 0, 1, false, true));
        telemetry.stateChanged(new SqsListenerTelemetry.ListenerState(1L, 1, 1, 1, true, false));

        assertThat(gauge(registry, "reactive.sqs.listener.active")).isZero();
        assertThat(gauge(registry, "reactive.sqs.listener.inflight")).isZero();
        assertThat(gauge(registry, "reactive.sqs.listener.running")).isZero();
        assertThat(gauge(registry, "reactive.sqs.listener.failed")).isEqualTo(1.0);
    }

    @Test
    void terminalDeliveryCauseIsExposedAsABoundedCounter() {
        var registry = new SimpleMeterRegistry();
        var telemetry =
                new MicrometerSqsListenerTelemetryFactory(registry)
                        .create(
                                new SqsListenerEngine.Configuration(
                                        "orders", "queue-url", 1, 60, 20, 30, 100));

        telemetry.deliveryCompleted(
                new SqsListenerTelemetry.DeliveryCompleted(
                        SqsListenerTelemetry.DeliveryOutcome.SHUTDOWN_CANCELLED,
                        Duration.ofSeconds(1),
                        SqsListenerTelemetry.ProcessingOutcome.SUCCESS,
                        SqsListenerTelemetry.DeleteCompleted.notAttempted()));

        assertThat(
                        registry.get("reactive.sqs.delivery")
                                .tags("listener", "orders", "outcome", "shutdown_cancelled")
                                .counter()
                                .count())
                .isEqualTo(1.0);
        assertThat(registry.find("reactive.sqs.delivery").counters())
                .hasSize(SqsListenerTelemetry.DeliveryOutcome.values().length)
                .allSatisfy(
                        counter ->
                                assertThat(counter.getId().getTags())
                                        .extracting(tag -> tag.getKey())
                                        .containsExactlyInAnyOrder("listener", "outcome"));
    }

    private static double gauge(SimpleMeterRegistry registry, String name) {
        return registry.get(name).tag("listener", "orders").gauge().value();
    }
}
