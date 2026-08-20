package io.github.marcschmidt1999.reactive.sqs.internal;

import java.time.Duration;
import java.util.Objects;

/**
 * Internal, framework-neutral telemetry boundary for the SQS listener engine.
 *
 * <p>Implementations must be thread-safe and non-blocking. The engine treats telemetry as
 * best-effort and isolates non-fatal implementation failures from message processing.
 */
public interface SqsListenerTelemetry {

    SqsListenerTelemetry NOOP = new SqsListenerTelemetry() {};

    default void receiveCompleted(ReceiveCompleted event) {}

    default void deliveryCompleted(DeliveryCompleted event) {}

    default void deleteBatchCompleted(DeleteBatchCompleted event) {}

    default void visibilityRenewalCompleted(VisibilityRenewalCompleted event) {}

    default void retryScheduled(RetryOperation operation) {}

    default void visibilityRenewalSkipped(VisibilitySkipReason reason) {}

    default void stateChanged(ListenerState state) {}

    static SqsListenerTelemetry noOp() {
        return NOOP;
    }

    record ReceiveCompleted(Duration duration, ReceiveOutcome outcome, int messageCount) {
        public ReceiveCompleted {
            requireNonNegative(duration, "duration");
            Objects.requireNonNull(outcome, "outcome");
            if (messageCount < 0) {
                throw new IllegalArgumentException("messageCount must not be negative");
            }
        }
    }

    record DeliveryCompleted(
            DeliveryOutcome outcome,
            Duration processingDuration,
            ProcessingOutcome processingOutcome,
            DeleteCompleted delete) {
        public DeliveryCompleted {
            Objects.requireNonNull(outcome, "outcome");
            requireNonNegative(processingDuration, "processingDuration");
            Objects.requireNonNull(processingOutcome, "processingOutcome");
            Objects.requireNonNull(delete, "delete");
        }
    }

    record DeleteCompleted(Duration duration, DeleteOutcome outcome) {
        public DeleteCompleted {
            requireNonNegative(duration, "duration");
            Objects.requireNonNull(outcome, "outcome");
        }

        public static DeleteCompleted notAttempted() {
            return new DeleteCompleted(Duration.ZERO, DeleteOutcome.NOT_ATTEMPTED);
        }
    }

    record DeleteBatchCompleted(Duration duration, int entryCount, DeleteBatchOutcome outcome) {
        public DeleteBatchCompleted {
            requireNonNegative(duration, "duration");
            if (entryCount < 1 || entryCount > 10) {
                throw new IllegalArgumentException("entryCount must be between 1 and 10");
            }
            Objects.requireNonNull(outcome, "outcome");
        }
    }

    record VisibilityRenewalCompleted(Duration duration, VisibilityOutcome outcome) {
        public VisibilityRenewalCompleted {
            requireNonNegative(duration, "duration");
            Objects.requireNonNull(outcome, "outcome");
        }
    }

    record ListenerState(
            long sequence,
            int active,
            int inFlight,
            int inFlightLimit,
            boolean running,
            boolean failed) {
        public ListenerState {
            if (sequence < 1 || active < 0 || inFlight < 0 || inFlightLimit < 1) {
                throw new IllegalArgumentException(
                        "listener state counts are outside their bounds");
            }
        }
    }

    enum ReceiveOutcome {
        MESSAGES,
        EMPTY,
        RETRYABLE_ERROR,
        TERMINAL_ERROR,
        CANCELLED
    }

    enum ProcessingOutcome {
        SUCCESS,
        MAPPING_ERROR,
        HANDLER_ERROR,
        PROCESSING_TIMEOUT,
        VISIBILITY_TIMEOUT,
        SHUTDOWN_CANCELLED
    }

    enum DeliveryOutcome {
        ACKNOWLEDGED,
        MAPPING_ERROR,
        HANDLER_ERROR,
        PROCESSING_TIMEOUT,
        VISIBILITY_TIMEOUT,
        SHUTDOWN_CANCELLED,
        DELETE_ERROR
    }

    enum DeleteOutcome {
        SUCCESS,
        ERROR,
        CANCELLED,
        NOT_ATTEMPTED
    }

    enum DeleteBatchOutcome {
        SUCCESS,
        PARTIAL_FAILURE,
        ERROR
    }

    enum VisibilityOutcome {
        SUCCESS,
        RETRYABLE_ERROR,
        TERMINAL_ERROR,
        CANCELLED
    }

    enum RetryOperation {
        RECEIVE,
        VISIBILITY
    }

    enum VisibilitySkipReason {
        LEASE_EXPIRING,
        BUDGET_EXHAUSTED,
        SCHEDULER_ERROR
    }

    private static void requireNonNegative(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }
}
