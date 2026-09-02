package io.github.marcschmidt1999.reactive.sqs.soak;

import io.github.marcschmidt1999.reactive.sqs.SqsMessage;
import io.github.marcschmidt1999.reactive.sqs.annotation.ReactiveSqsListener;
import java.time.Clock;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

@Component
final class SoakMessageListener {

    private final AuditStore auditStore;
    private final CpuOperation cpuOperation;
    private final Scheduler cpuScheduler;
    private final Clock clock;
    private final String runId;

    SoakMessageListener(
            AuditStore auditStore,
            CpuOperation cpuOperation,
            @Qualifier("soakCpuScheduler") Scheduler cpuScheduler,
            Clock clock,
            @Value("${soak.run-id}") String runId) {
        this.auditStore = auditStore;
        this.cpuOperation = cpuOperation;
        this.cpuScheduler = cpuScheduler;
        this.clock = clock;
        this.runId = Objects.requireNonNull(runId, "runId");
    }

    @ReactiveSqsListener(
            queue = "${soak.source-queue-url}",
            maxInFlight = 8,
            visibilityTimeoutSeconds = 15,
            shutdownGraceSeconds = 10,
            maxProcessingDurationSeconds = 5)
    Mono<Void> consumeSource(SqsMessage<SoakMessage> envelope) {
        var message = envelope.payload();
        validateRun(message);
        var receiveCount = receiveCount(envelope);
        return Mono.fromFuture(
                        auditStore.markAttempt(
                                message, envelope.messageId(), receiveCount, clock.instant()))
                .then(
                        Mono.fromCallable(
                                        () -> cpuOperation.run(message.cpuMillis(), message.seed()))
                                .subscribeOn(cpuScheduler))
                .flatMap(
                        checksum -> {
                            if (shouldFail(message.mode(), receiveCount)) {
                                return Mono.error(
                                        new ExpectedSoakFailure(
                                                message.mode(), message.eventId(), receiveCount));
                            }
                            return Mono.fromFuture(
                                    auditStore.markProcessed(message, checksum, clock.instant()));
                        })
                .then();
    }

    @ReactiveSqsListener(
            queue = "${soak.dlq-queue-url}",
            maxInFlight = 2,
            visibilityTimeoutSeconds = 15,
            shutdownGraceSeconds = 10,
            maxProcessingDurationSeconds = 10)
    Mono<Void> consumeDlq(SqsMessage<SoakMessage> envelope) {
        validateRun(envelope.payload());
        return Mono.fromFuture(
                        auditStore.markDlq(
                                envelope.payload(),
                                envelope.messageId(),
                                receiveCount(envelope),
                                clock.instant()))
                .then();
    }

    private void validateRun(SoakMessage message) {
        if (!runId.equals(message.runId())) {
            throw new IllegalArgumentException(
                    "Message %s belongs to run %s, not %s"
                            .formatted(message.eventId(), message.runId(), runId));
        }
    }

    private static boolean shouldFail(SoakMode mode, int receiveCount) {
        return mode == SoakMode.POISON || (mode == SoakMode.RETRY_ONCE && receiveCount == 1);
    }

    private static int receiveCount(SqsMessage<?> message) {
        var value =
                message.systemAttributes()
                        .get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
        if (value == null) {
            return 1;
        }
        try {
            return Math.max(1, Integer.parseInt(value));
        } catch (NumberFormatException ignored) {
            return 1;
        }
    }
}
