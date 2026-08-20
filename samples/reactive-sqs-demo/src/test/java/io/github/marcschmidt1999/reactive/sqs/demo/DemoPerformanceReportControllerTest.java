package io.github.marcschmidt1999.reactive.sqs.demo;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerEngine;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;

class DemoPerformanceReportControllerTest {

    private static final String LISTENER = "demoMessageListener.consume";

    @Test
    void performanceEndpointSummarisesListenerThroughputLatencyAndOutcomes() {
        var registry = new SimpleMeterRegistry();
        var gauges =
                List.of(
                        registerGauge(registry, "reactive.sqs.listener.active", 3),
                        registerGauge(registry, "reactive.sqs.listener.inflight.limit", 8),
                        registerGauge(registry, "reactive.sqs.listener.running", 1),
                        registerGauge(registry, "reactive.sqs.listener.failed", 0));

        var nanos = new AtomicLong(Duration.ofSeconds(10).toNanos());
        var clock = Clock.fixed(Instant.parse("2026-08-20T15:00:00Z"), ZoneOffset.UTC);
        var window = new DemoPerformanceMeasurementWindow(clock, nanos::get);
        var telemetry = window.create(configuration());
        telemetry.receiveCompleted(
                new SqsListenerTelemetry.ReceiveCompleted(
                        Duration.ofMillis(10), SqsListenerTelemetry.ReceiveOutcome.MESSAGES, 40));
        repeatDelivery(telemetry, 36, SqsListenerTelemetry.DeliveryOutcome.ACKNOWLEDGED);
        repeatDelivery(telemetry, 4, SqsListenerTelemetry.DeliveryOutcome.HANDLER_ERROR);
        telemetry.retryScheduled(SqsListenerTelemetry.RetryOperation.RECEIVE);
        telemetry.retryScheduled(SqsListenerTelemetry.RetryOperation.RECEIVE);
        var controller = new DemoPerformanceReportController(registry, window, clock);
        nanos.set(Duration.ofSeconds(12).toNanos());

        WebTestClient.bindToController(controller)
                .build()
                .get()
                .uri("/demo/performance")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.measurementSeconds")
                .isEqualTo(2.0)
                .jsonPath("$.messagesReceived")
                .isEqualTo(40)
                .jsonPath("$.deliveries.acknowledged")
                .isEqualTo(36)
                .jsonPath("$.deliveries.handler_error")
                .isEqualTo(4)
                .jsonPath("$.acknowledgedPerSecond")
                .isEqualTo(18.0)
                .jsonPath("$.processing.count")
                .isEqualTo(36)
                .jsonPath("$.processing.meanMilliseconds")
                .isEqualTo(10.0)
                .jsonPath("$.processing.maxMilliseconds")
                .isEqualTo(10.0)
                .jsonPath("$.delete.count")
                .isEqualTo(36)
                .jsonPath("$.retries.receive")
                .isEqualTo(2)
                .jsonPath("$.listener.active")
                .isEqualTo(3)
                .jsonPath("$.listener.capacity")
                .isEqualTo(8)
                .jsonPath("$.listener.running")
                .isEqualTo(true)
                .jsonPath("$.listener.failed")
                .isEqualTo(false);
        assertThat(gauges).hasSize(4);
    }

    @Test
    void resetStartsAnIndependentMeasurementWindow() {
        var registry = new SimpleMeterRegistry();
        var nanos = new AtomicLong(Duration.ofSeconds(10).toNanos());
        var clock = Clock.fixed(Instant.parse("2026-08-20T15:00:00Z"), ZoneOffset.UTC);
        var window = new DemoPerformanceMeasurementWindow(clock, nanos::get);
        var telemetry = window.create(configuration());
        repeatDelivery(telemetry, 10, SqsListenerTelemetry.DeliveryOutcome.ACKNOWLEDGED);
        nanos.set(Duration.ofSeconds(15).toNanos());
        var controller = new DemoPerformanceReportController(registry, window, clock);

        var client = WebTestClient.bindToController(controller).build();
        client.post()
                .uri("/demo/performance/reset")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.deliveries.acknowledged")
                .isEqualTo(0)
                .jsonPath("$.processing.count")
                .isEqualTo(0)
                .jsonPath("$.delete.count")
                .isEqualTo(0);

        nanos.set(Duration.ofSeconds(17).toNanos());
        repeatDelivery(telemetry, 4, SqsListenerTelemetry.DeliveryOutcome.ACKNOWLEDGED);

        client.get()
                .uri("/demo/performance")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.measurementSeconds")
                .isEqualTo(2.0)
                .jsonPath("$.deliveries.acknowledged")
                .isEqualTo(4)
                .jsonPath("$.acknowledgedPerSecond")
                .isEqualTo(2.0)
                .jsonPath("$.processing.count")
                .isEqualTo(4)
                .jsonPath("$.delete.count")
                .isEqualTo(4);
    }

    private static void repeatDelivery(
            SqsListenerTelemetry telemetry,
            int count,
            SqsListenerTelemetry.DeliveryOutcome deliveryOutcome) {
        for (var index = 0; index < count; index++) {
            telemetry.deliveryCompleted(
                    new SqsListenerTelemetry.DeliveryCompleted(
                            deliveryOutcome,
                            Duration.ofMillis(10),
                            deliveryOutcome == SqsListenerTelemetry.DeliveryOutcome.HANDLER_ERROR
                                    ? SqsListenerTelemetry.ProcessingOutcome.HANDLER_ERROR
                                    : SqsListenerTelemetry.ProcessingOutcome.SUCCESS,
                            new SqsListenerTelemetry.DeleteCompleted(
                                    Duration.ofMillis(5),
                                    deliveryOutcome
                                                    == SqsListenerTelemetry.DeliveryOutcome
                                                            .ACKNOWLEDGED
                                            ? SqsListenerTelemetry.DeleteOutcome.SUCCESS
                                            : SqsListenerTelemetry.DeleteOutcome.NOT_ATTEMPTED)));
        }
    }

    private static AtomicInteger registerGauge(
            SimpleMeterRegistry registry, String name, int value) {
        var gaugeValue = new AtomicInteger(value);
        Gauge.builder(name, gaugeValue, AtomicInteger::get)
                .tag("listener", LISTENER)
                .register(registry);
        return gaugeValue;
    }

    private static SqsListenerEngine.Configuration configuration() {
        return new SqsListenerEngine.Configuration(
                LISTENER, "https://queue.example.test/demo", 8, 60, 20, 10, 55);
    }
}
