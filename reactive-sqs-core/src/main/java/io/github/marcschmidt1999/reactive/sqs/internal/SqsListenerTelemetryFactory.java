package io.github.marcschmidt1999.reactive.sqs.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Creates one telemetry recorder for each configured listener. */
@FunctionalInterface
public interface SqsListenerTelemetryFactory {

    SqsListenerTelemetry create(SqsListenerEngine.Configuration configuration);

    static SqsListenerTelemetryFactory noOp() {
        return ignored -> SqsListenerTelemetry.noOp();
    }

    /** Combines independent telemetry consumers without changing listener processing semantics. */
    static SqsListenerTelemetryFactory composite(List<SqsListenerTelemetryFactory> factories) {
        var delegates = List.copyOf(factories);
        if (delegates.isEmpty()) {
            return noOp();
        }
        return configuration -> {
            var telemetry = new ArrayList<SqsListenerTelemetry>(delegates.size());
            for (var delegate : delegates) {
                telemetry.add(requireTelemetry(delegate, configuration));
            }
            return new FanOutTelemetry(telemetry);
        };
    }

    static SqsListenerTelemetry requireTelemetry(
            SqsListenerTelemetryFactory factory, SqsListenerEngine.Configuration configuration) {
        Objects.requireNonNull(factory, "factory");
        return Objects.requireNonNull(
                factory.create(configuration), "SQS listener telemetry factory returned null");
    }

    final class FanOutTelemetry implements SqsListenerTelemetry {
        private final List<SqsListenerTelemetry> delegates;

        private FanOutTelemetry(List<SqsListenerTelemetry> delegates) {
            this.delegates = List.copyOf(delegates);
        }

        @Override
        public void receiveCompleted(ReceiveCompleted event) {
            for (var delegate : delegates) {
                delegate.receiveCompleted(event);
            }
        }

        @Override
        public void deliveryCompleted(DeliveryCompleted event) {
            for (var delegate : delegates) {
                delegate.deliveryCompleted(event);
            }
        }

        @Override
        public void deleteBatchCompleted(DeleteBatchCompleted event) {
            for (var delegate : delegates) {
                delegate.deleteBatchCompleted(event);
            }
        }

        @Override
        public void visibilityRenewalCompleted(VisibilityRenewalCompleted event) {
            for (var delegate : delegates) {
                delegate.visibilityRenewalCompleted(event);
            }
        }

        @Override
        public void retryScheduled(RetryOperation operation) {
            for (var delegate : delegates) {
                delegate.retryScheduled(operation);
            }
        }

        @Override
        public void visibilityRenewalSkipped(VisibilitySkipReason reason) {
            for (var delegate : delegates) {
                delegate.visibilityRenewalSkipped(reason);
            }
        }

        @Override
        public void stateChanged(ListenerState state) {
            for (var delegate : delegates) {
                delegate.stateChanged(state);
            }
        }
    }
}
