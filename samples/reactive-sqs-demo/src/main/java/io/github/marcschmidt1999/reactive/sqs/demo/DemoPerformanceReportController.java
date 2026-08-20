package io.github.marcschmidt1999.reactive.sqs.demo;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** Human-readable report for the local demo listener's current measurement window. */
@RestController
final class DemoPerformanceReportController {

    private static final String PREFIX = "reactive.sqs.";

    private final MeterRegistry registry;
    private final DemoPerformanceMeasurementWindow measurementWindow;
    private final Clock clock;

    @Autowired
    DemoPerformanceReportController(
            MeterRegistry registry, DemoPerformanceMeasurementWindow measurementWindow) {
        this(registry, measurementWindow, Clock.systemUTC());
    }

    DemoPerformanceReportController(
            MeterRegistry registry,
            DemoPerformanceMeasurementWindow measurementWindow,
            Clock clock) {
        this.registry = registry;
        this.measurementWindow = measurementWindow;
        this.clock = clock;
    }

    @GetMapping("/demo/performance")
    DemoPerformanceReport performance() {
        var measurement = measurementWindow.snapshot();
        var seconds = measurement.duration().toNanos() / 1_000_000_000.0;
        var acknowledged = measurement.deliveries().get("acknowledged");

        return new DemoPerformanceReport(
                measurement.startedAt(),
                clock.instant(),
                seconds,
                measurement.messagesReceived(),
                measurement.deliveries(),
                seconds == 0.0 ? 0.0 : acknowledged / seconds,
                latency(measurement.processing()),
                latency(measurement.delete()),
                new DeleteBatch(
                        measurement.deleteBatches(),
                        measurement.deleteBatchEntries(),
                        averageEntriesPerRequest(measurement),
                        latency(measurement.deleteBatch())),
                latency(measurement.receive()),
                measurement.retries(),
                new ListenerState(
                        gauge(PREFIX + "listener.active"),
                        gauge(PREFIX + "listener.inflight.limit"),
                        gauge(PREFIX + "listener.running") == 1,
                        gauge(PREFIX + "listener.failed") == 1));
    }

    @PostMapping("/demo/performance/reset")
    DemoPerformanceReport reset() {
        measurementWindow.reset();
        return performance();
    }

    private int gauge(String name) {
        var gauge = search(name).gauge();
        return gauge == null ? 0 : (int) Math.round(gauge.value());
    }

    private static Latency latency(DemoPerformanceMeasurementWindow.Latency latency) {
        return new Latency(
                latency.count(),
                latency.meanMilliseconds(),
                latency.maxMilliseconds(),
                latency.p50Milliseconds(),
                latency.p95Milliseconds(),
                latency.p99Milliseconds());
    }

    private static double averageEntriesPerRequest(
            DemoPerformanceMeasurementWindow.Measurement measurement) {
        var requests =
                measurement.deleteBatches().values().stream().mapToLong(Long::longValue).sum();
        return requests == 0 ? 0.0 : (double) measurement.deleteBatchEntries() / requests;
    }

    private io.micrometer.core.instrument.search.Search search(String name, String... tags) {
        var search = registry.find(name).tag("listener", "demoMessageListener.consume");
        for (var index = 0; index < tags.length; index += 2) {
            search = search.tag(tags[index], tags[index + 1]);
        }
        return search;
    }

    record DemoPerformanceReport(
            java.time.Instant startedAt,
            java.time.Instant reportedAt,
            double measurementSeconds,
            long messagesReceived,
            Map<String, Long> deliveries,
            double acknowledgedPerSecond,
            Latency processing,
            Latency delete,
            DeleteBatch deleteBatch,
            Latency receive,
            Map<String, Long> retries,
            ListenerState listener) {}

    record Latency(
            long count,
            double meanMilliseconds,
            double maxMilliseconds,
            double p50Milliseconds,
            double p95Milliseconds,
            double p99Milliseconds) {}

    record DeleteBatch(
            Map<String, Long> requests,
            long entries,
            double averageEntriesPerRequest,
            Latency latency) {}

    record ListenerState(int active, int capacity, boolean running, boolean failed) {}
}
