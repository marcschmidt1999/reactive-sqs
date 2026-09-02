package io.github.marcschmidt1999.reactive.sqs.soak;

/** Deliberate workload failure used to verify retry and DLQ behavior. */
@SuppressWarnings("serial")
final class ExpectedSoakFailure extends RuntimeException {

    ExpectedSoakFailure(SoakMode mode, String eventId, int receiveCount) {
        super(
                "Expected %s failure for event %s on receive %d"
                        .formatted(mode, eventId, receiveCount));
    }
}
