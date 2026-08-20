package io.github.marcschmidt1999.reactive.sqs.boot3.autoconfigure;

import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerEngine;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetry;
import io.github.marcschmidt1999.reactive.sqs.internal.SqsListenerTelemetryFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.EnumMap;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

final class MicrometerSqsListenerTelemetryFactory implements SqsListenerTelemetryFactory {

    private static final String PREFIX = "reactive.sqs.";

    private final MeterRegistry registry;
    private final ConcurrentMap<String, MicrometerSqsListenerTelemetry> listeners =
            new ConcurrentHashMap<>();

    MicrometerSqsListenerTelemetryFactory(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public SqsListenerTelemetry create(SqsListenerEngine.Configuration configuration) {
        return listeners.compute(
                configuration.listenerId(),
                (listenerId, existing) -> {
                    if (existing != null) {
                        existing.requireCompatible(configuration);
                        return existing;
                    }
                    return new MicrometerSqsListenerTelemetry(registry, configuration);
                });
    }

    private static final class MicrometerSqsListenerTelemetry implements SqsListenerTelemetry {

        private final String listenerId;
        private final AtomicInteger inFlightLimit;
        private final Counter received;
        private final EnumMap<DeliveryOutcome, Counter> deliveryCounters;
        private final EnumMap<ReceiveOutcome, Timer> receiveTimers;
        private final EnumMap<ProcessingOutcome, Timer> processingTimers;
        private final EnumMap<DeleteOutcome, Timer> deleteTimers;
        private final EnumMap<DeleteBatchOutcome, Counter> deleteBatchRequestCounters;
        private final EnumMap<DeleteBatchOutcome, Timer> deleteBatchTimers;
        private final Counter deleteBatchEntries;
        private final EnumMap<VisibilityOutcome, Timer> visibilityTimers;
        private final EnumMap<RetryOperation, Counter> retryCounters;
        private final EnumMap<VisibilitySkipReason, Counter> visibilitySkippedCounters;
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger running = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        private long lastStateSequence;

        private MicrometerSqsListenerTelemetry(
                MeterRegistry registry, SqsListenerEngine.Configuration configuration) {
            listenerId = configuration.listenerId();
            inFlightLimit = new AtomicInteger(configuration.maxInFlight());
            received =
                    Counter.builder(PREFIX + "messages.received")
                            .description("SQS messages accepted by the listener")
                            .tag("listener", listenerId)
                            .register(registry);
            deliveryCounters =
                    counters(
                            registry,
                            PREFIX + "delivery",
                            "outcome",
                            DeliveryOutcome.values(),
                            listenerId);
            receiveTimers =
                    timers(registry, PREFIX + "receive", ReceiveOutcome.values(), listenerId);
            processingTimers =
                    timers(registry, PREFIX + "processing", ProcessingOutcome.values(), listenerId);
            deleteTimers =
                    timers(
                            registry,
                            PREFIX + "delete",
                            new DeleteOutcome[] {
                                DeleteOutcome.SUCCESS, DeleteOutcome.ERROR, DeleteOutcome.CANCELLED
                            },
                            listenerId);
            deleteBatchRequestCounters =
                    counters(
                            registry,
                            PREFIX + "delete.batch.requests",
                            "outcome",
                            DeleteBatchOutcome.values(),
                            listenerId);
            deleteBatchTimers =
                    timers(
                            registry,
                            PREFIX + "delete.batch",
                            DeleteBatchOutcome.values(),
                            listenerId);
            deleteBatchEntries =
                    Counter.builder(PREFIX + "delete.batch.entries")
                            .description("SQS receipt handles sent in delete batches")
                            .tag("listener", listenerId)
                            .register(registry);
            visibilityTimers =
                    timers(
                            registry,
                            PREFIX + "visibility.renewal",
                            VisibilityOutcome.values(),
                            listenerId);
            retryCounters =
                    counters(
                            registry,
                            PREFIX + "retry.scheduled",
                            "operation",
                            RetryOperation.values(),
                            listenerId);
            visibilitySkippedCounters =
                    counters(
                            registry,
                            PREFIX + "visibility.renewal.skipped",
                            "reason",
                            VisibilitySkipReason.values(),
                            listenerId);
            gauge(registry, PREFIX + "listener.active", listenerId, active);
            gauge(registry, PREFIX + "listener.inflight", listenerId, inFlight);
            gauge(registry, PREFIX + "listener.running", listenerId, running);
            gauge(registry, PREFIX + "listener.failed", listenerId, failed);
            gauge(registry, PREFIX + "listener.inflight.limit", listenerId, inFlightLimit);
        }

        @Override
        public void receiveCompleted(ReceiveCompleted event) {
            receiveTimers.get(event.outcome()).record(event.duration());
            if (event.messageCount() != 0) {
                received.increment(event.messageCount());
            }
        }

        @Override
        public void deliveryCompleted(DeliveryCompleted event) {
            deliveryCounters.get(event.outcome()).increment();
            processingTimers.get(event.processingOutcome()).record(event.processingDuration());
            if (event.delete().outcome() != DeleteOutcome.NOT_ATTEMPTED) {
                deleteTimers.get(event.delete().outcome()).record(event.delete().duration());
            }
        }

        @Override
        public void deleteBatchCompleted(DeleteBatchCompleted event) {
            deleteBatchRequestCounters.get(event.outcome()).increment();
            deleteBatchEntries.increment(event.entryCount());
            deleteBatchTimers.get(event.outcome()).record(event.duration());
        }

        @Override
        public void visibilityRenewalCompleted(VisibilityRenewalCompleted event) {
            visibilityTimers.get(event.outcome()).record(event.duration());
        }

        @Override
        public void retryScheduled(RetryOperation operation) {
            retryCounters.get(operation).increment();
        }

        @Override
        public void visibilityRenewalSkipped(VisibilitySkipReason reason) {
            visibilitySkippedCounters.get(reason).increment();
        }

        @Override
        public synchronized void stateChanged(ListenerState state) {
            if (state.sequence() <= lastStateSequence) {
                return;
            }
            active.set(state.active());
            inFlight.set(state.inFlight());
            running.set(state.running() ? 1 : 0);
            failed.set(state.failed() ? 1 : 0);
            lastStateSequence = state.sequence();
        }

        private void requireCompatible(SqsListenerEngine.Configuration configuration) {
            if (configuration.maxInFlight() != inFlightLimit.get()) {
                throw new IllegalStateException(
                        "Listener " + listenerId + " changed maxInFlight across restarts");
            }
        }

        private static <E extends Enum<E>> EnumMap<E, Timer> timers(
                MeterRegistry registry, String name, E[] outcomes, String listenerId) {
            var result = new EnumMap<E, Timer>(outcomes[0].getDeclaringClass());
            for (var outcome : outcomes) {
                result.put(
                        outcome,
                        Timer.builder(name)
                                .description("Reactive SQS operation duration")
                                .tags("listener", listenerId, "outcome", tagValue(outcome))
                                .register(registry));
            }
            return result;
        }

        private static <E extends Enum<E>> EnumMap<E, Counter> counters(
                MeterRegistry registry,
                String name,
                String dimension,
                E[] values,
                String listenerId) {
            var result = new EnumMap<E, Counter>(values[0].getDeclaringClass());
            for (var value : values) {
                result.put(
                        value,
                        Counter.builder(name)
                                .description("Reactive SQS operation count")
                                .tags("listener", listenerId, dimension, tagValue(value))
                                .register(registry));
            }
            return result;
        }

        private static void gauge(
                MeterRegistry registry, String name, String listenerId, AtomicInteger value) {
            Gauge.builder(name, value, AtomicInteger::get)
                    .description("Reactive SQS listener state")
                    .tag("listener", listenerId)
                    .register(registry);
        }

        private static String tagValue(Enum<?> value) {
            return value.name().toLowerCase(Locale.ROOT);
        }
    }
}
