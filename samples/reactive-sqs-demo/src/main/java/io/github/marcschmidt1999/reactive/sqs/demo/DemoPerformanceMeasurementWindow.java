package io.github.marcschmidt1999.reactive.sqs.demo;

import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.DeleteBatchCompleted;
import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.DeleteBatchOutcome;
import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.DeleteOutcome;
import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.DeliveryCompleted;
import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.DeliveryOutcome;
import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.ProcessingOutcome;
import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.ReceiveCompleted;
import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.ReceiveOutcome;
import static io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry.RetryOperation;

import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerEngine;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Isolated, resettable measurement window for the local demo's single listener.
 *
 * <p>The production Micrometer meters are intentionally not reset. This recorder receives the same
 * completed listener events and owns a separate in-memory histogram, so percentile values are exact
 * for the selected benchmark window.
 */
final class DemoPerformanceMeasurementWindow {

    private final Clock clock;
    private final LongSupplier nanoTime;
    private final AtomicReference<Window> current;

    DemoPerformanceMeasurementWindow() {
        this(Clock.systemUTC(), System::nanoTime);
    }

    DemoPerformanceMeasurementWindow(Clock clock, LongSupplier nanoTime) {
        this.clock = clock;
        this.nanoTime = nanoTime;
        current = new AtomicReference<>(new Window(clock.instant(), nanoTime.getAsLong()));
    }

    SqsListenerTelemetry create(SqsListenerEngine.Configuration configuration) {
        return new WindowTelemetry();
    }

    void reset() {
        current.set(new Window(clock.instant(), nanoTime.getAsLong()));
    }

    Measurement snapshot() {
        return current.get().snapshot(nanoTime.getAsLong());
    }

    CompletableFuture<Void> nextDelivery() {
        return current.get().nextDelivery;
    }

    private final class WindowTelemetry implements SqsListenerTelemetry {

        @Override
        public void receiveCompleted(ReceiveCompleted event) {
            current.get().receiveCompleted(event);
        }

        @Override
        public void deliveryCompleted(DeliveryCompleted event) {
            current.get().deliveryCompleted(event);
        }

        @Override
        public void deleteBatchCompleted(DeleteBatchCompleted event) {
            current.get().deleteBatchCompleted(event);
        }

        @Override
        public void retryScheduled(RetryOperation operation) {
            current.get().retryScheduled(operation);
        }
    }

    private static final class Window {
        private final Instant startedAt;
        private final long startedAtNanos;
        private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
        private final LongAdder messagesReceived = new LongAdder();
        private final EnumMap<DeliveryOutcome, LongAdder> deliveries =
                adders(DeliveryOutcome.class);
        private final EnumMap<RetryOperation, LongAdder> retries = adders(RetryOperation.class);
        private final EnumMap<DeleteBatchOutcome, LongAdder> deleteBatches =
                adders(DeleteBatchOutcome.class);
        private final LongAdder deleteBatchEntries = new LongAdder();
        private final Timer processing = timer("processing");
        private final Timer delete = timer("delete");
        private final Timer deleteBatch = timer("delete.batch");
        private final Timer receive = timer("receive");
        private final CompletableFuture<Void> nextDelivery = new CompletableFuture<>();

        private Window(Instant startedAt, long startedAtNanos) {
            this.startedAt = startedAt;
            this.startedAtNanos = startedAtNanos;
        }

        private void receiveCompleted(ReceiveCompleted event) {
            if (event.outcome() == ReceiveOutcome.MESSAGES) {
                messagesReceived.add(event.messageCount());
                receive.record(event.duration());
            }
        }

        private void deliveryCompleted(DeliveryCompleted event) {
            deliveries.get(event.outcome()).increment();
            if (event.processingOutcome() == ProcessingOutcome.SUCCESS) {
                processing.record(event.processingDuration());
            }
            if (event.delete().outcome() == DeleteOutcome.SUCCESS) {
                delete.record(event.delete().duration());
            }
            nextDelivery.complete(null);
        }

        private void retryScheduled(RetryOperation operation) {
            retries.get(operation).increment();
        }

        private void deleteBatchCompleted(DeleteBatchCompleted event) {
            deleteBatches.get(event.outcome()).increment();
            deleteBatchEntries.add(event.entryCount());
            deleteBatch.record(event.duration());
        }

        private Measurement snapshot(long nowNanos) {
            var deliveryCounts = new LinkedHashMap<String, Long>();
            for (var outcome : DeliveryOutcome.values()) {
                deliveryCounts.put(tagValue(outcome), deliveries.get(outcome).sum());
            }
            var retryCounts = new LinkedHashMap<String, Long>();
            for (var operation : RetryOperation.values()) {
                retryCounts.put(tagValue(operation), retries.get(operation).sum());
            }
            var deleteBatchCounts = new LinkedHashMap<String, Long>();
            for (var outcome : DeleteBatchOutcome.values()) {
                deleteBatchCounts.put(tagValue(outcome), deleteBatches.get(outcome).sum());
            }
            return new Measurement(
                    startedAt,
                    Duration.ofNanos(Math.max(0L, nowNanos - startedAtNanos)),
                    messagesReceived.sum(),
                    Collections.unmodifiableMap(deliveryCounts),
                    latency(processing),
                    latency(delete),
                    Collections.unmodifiableMap(deleteBatchCounts),
                    deleteBatchEntries.sum(),
                    latency(deleteBatch),
                    latency(receive),
                    Collections.unmodifiableMap(retryCounts));
        }

        private Timer timer(String operation) {
            return Timer.builder("demo.performance.window." + operation)
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .register(registry);
        }

        private static <E extends Enum<E>> EnumMap<E, LongAdder> adders(Class<E> type) {
            var result = new EnumMap<E, LongAdder>(type);
            for (var value : type.getEnumConstants()) {
                result.put(value, new LongAdder());
            }
            return result;
        }
    }

    private static Latency latency(Timer timer) {
        var snapshot = timer.takeSnapshot();
        return new Latency(
                timer.count(),
                timer.mean(TimeUnit.MILLISECONDS),
                timer.max(TimeUnit.MILLISECONDS),
                percentile(snapshot, 0.5),
                percentile(snapshot, 0.95),
                percentile(snapshot, 0.99));
    }

    private static double percentile(HistogramSnapshot snapshot, double percentile) {
        for (ValueAtPercentile value : snapshot.percentileValues()) {
            if (Double.compare(value.percentile(), percentile) == 0) {
                return value.value(TimeUnit.MILLISECONDS);
            }
        }
        return 0.0;
    }

    private static String tagValue(Enum<?> value) {
        return value.name().toLowerCase(java.util.Locale.ROOT);
    }

    record Measurement(
            Instant startedAt,
            Duration duration,
            long messagesReceived,
            Map<String, Long> deliveries,
            Latency processing,
            Latency delete,
            Map<String, Long> deleteBatches,
            long deleteBatchEntries,
            Latency deleteBatch,
            Latency receive,
            Map<String, Long> retries) {}

    record Latency(
            long count,
            double meanMilliseconds,
            double maxMilliseconds,
            double p50Milliseconds,
            double p95Milliseconds,
            double p99Milliseconds) {}
}
