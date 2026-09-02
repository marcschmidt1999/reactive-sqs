package io.github.marcschmidt1999.reactive.sqs.soak;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Local continuous producer for a deployed soak stack. */
public final class SoakProducerMain {

    private SoakProducerMain() {}

    public static void main(String[] args) {
        var options = ProducerOptions.parse(args);
        try (var sqs = SoakApplication.sqsClient(options.region());
                var dynamo = SoakApplication.dynamoClient(options.region())) {
            var auditStore = new DynamoAuditStore(dynamo, options.tableName());
            var producer =
                    new SoakProducer(
                            sqs,
                            auditStore,
                            new ObjectMapper(),
                            options.queueUrl(),
                            SoakProducerMain::sleep);
            run(options, producer);
        }
    }

    static void run(ProducerOptions options, SoakProducer producer) {
        var startedAt = Instant.now();
        var deadline = startedAt.plus(options.duration());
        var expiresAt = startedAt.plus(options.ttlDays(), ChronoUnit.DAYS);
        var sequence = Math.multiplyExact(startedAt.toEpochMilli(), 1_000L);
        var sent = 0L;
        var nextProgress = startedAt.plusSeconds(60);
        var stopRequested = new AtomicBoolean();
        var stopped = new CountDownLatch(1);
        var shutdownHook =
                new Thread(
                        () -> {
                            stopRequested.set(true);
                            try {
                                if (!stopped.await(90, TimeUnit.SECONDS)) {
                                    System.err.println(
                                            "Producer did not reach a durable checkpoint before shutdown timeout");
                                }
                            } catch (InterruptedException exception) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        "soak-producer-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        var runStarted = false;
        var failed = false;
        try {
            producer.startRun(options.runId(), startedAt, expiresAt);
            runStarted = true;
            while (!stopRequested.get() && Instant.now().isBefore(deadline)) {
                var tickStarted = System.nanoTime();
                var messages = new ArrayList<SoakMessage>(options.messagesPerSecond());
                for (var index = 0; index < options.messagesPerSecond(); index++) {
                    sequence++;
                    messages.add(SoakWorkload.message(options.runId(), sequence, options.seed()));
                }
                producer.publish(List.copyOf(messages), Instant.now(), expiresAt);
                sent += messages.size();
                producer.checkpointRun(options.runId(), sent, sequence, Instant.now(), false);
                if (!Instant.now().isBefore(nextProgress)) {
                    System.out.printf(
                            "run=%s accepted=%d elapsed=%s%n",
                            options.runId(), sent, Duration.between(startedAt, Instant.now()));
                    nextProgress = Instant.now().plusSeconds(60);
                }
                var elapsedNanos = System.nanoTime() - tickStarted;
                var remainingNanos = TimeUnit.SECONDS.toNanos(1) - elapsedNanos;
                if (remainingNanos > 0) {
                    sleep(Duration.ofNanos(remainingNanos));
                }
            }
        } catch (RuntimeException | Error exception) {
            failed = true;
            throw exception;
        } finally {
            try {
                if (runStarted) {
                    producer.checkpointRun(options.runId(), sent, sequence, Instant.now(), !failed);
                    if (failed) {
                        System.err.printf(
                                "run=%s producer-aborted accepted=%d%n", options.runId(), sent);
                    } else {
                        System.out.printf(
                                "run=%s producer-finished accepted=%d%n", options.runId(), sent);
                    }
                }
            } finally {
                stopped.countDown();
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // The JVM is already executing this hook.
                }
            }
        }
    }

    private static void sleep(Duration duration) {
        try {
            TimeUnit.NANOSECONDS.sleep(duration.toNanos());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Producer interrupted");
        }
    }

    record ProducerOptions(
            String region,
            String queueUrl,
            String tableName,
            String runId,
            int messagesPerSecond,
            Duration duration,
            int ttlDays,
            long seed) {

        private static final int DEFAULT_MESSAGES_PER_SECOND = 2;
        private static final Duration DEFAULT_DURATION = Duration.ofHours(24);
        private static final int DEFAULT_TTL_DAYS = 30;
        private static final long DEFAULT_SEED = 42;

        static ProducerOptions parse(String[] args) {
            String region = null;
            String queueUrl = null;
            String tableName = null;
            String runId = null;
            var rate = DEFAULT_MESSAGES_PER_SECOND;
            var duration = DEFAULT_DURATION;
            var ttlDays = DEFAULT_TTL_DAYS;
            var seed = DEFAULT_SEED;
            for (var argument : args) {
                if (argument.startsWith("--region=")) {
                    region = value(argument, "--region=");
                } else if (argument.startsWith("--queue-url=")) {
                    queueUrl = value(argument, "--queue-url=");
                } else if (argument.startsWith("--table=")) {
                    tableName = value(argument, "--table=");
                } else if (argument.startsWith("--run-id=")) {
                    runId = value(argument, "--run-id=");
                } else if (argument.startsWith("--messages-per-second=")) {
                    rate = integer(argument, "--messages-per-second=");
                } else if (argument.startsWith("--duration=")) {
                    duration = Duration.parse(value(argument, "--duration="));
                } else if (argument.startsWith("--ttl-days=")) {
                    ttlDays = integer(argument, "--ttl-days=");
                } else if (argument.startsWith("--seed=")) {
                    seed = Long.parseLong(value(argument, "--seed="));
                } else {
                    throw new IllegalArgumentException(usage());
                }
            }
            if (region == null || queueUrl == null || tableName == null || runId == null) {
                throw new IllegalArgumentException(usage());
            }
            if (rate < 1 || rate > 10) {
                throw new IllegalArgumentException("messages-per-second must be between 1 and 10");
            }
            if (duration.isZero()
                    || duration.isNegative()
                    || duration.compareTo(Duration.ofDays(7)) > 0) {
                throw new IllegalArgumentException("duration must be between PT0S and P7D");
            }
            if (ttlDays < 2 || ttlDays > 365) {
                throw new IllegalArgumentException("ttl-days must be between 2 and 365");
            }
            var runDays = Math.floorDiv(duration.toSeconds() - 1, 86_400) + 1;
            if (ttlDays < runDays + 2) {
                throw new IllegalArgumentException(
                        "ttl-days must leave at least two full days after the producer duration");
            }
            return new ProducerOptions(
                    region, queueUrl, tableName, runId, rate, duration, ttlDays, seed);
        }

        private static int integer(String argument, String prefix) {
            try {
                return Integer.parseInt(value(argument, prefix));
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException("Invalid integer for " + prefix, exception);
            }
        }

        private static String value(String argument, String prefix) {
            var value = argument.substring(prefix.length());
            if (value.isBlank()) {
                throw new IllegalArgumentException(usage());
            }
            return value;
        }

        private static String usage() {
            return "Required: --region=... --queue-url=... --table=... --run-id=...; "
                    + "optional: --messages-per-second=2 --duration=PT24H --ttl-days=30 --seed=42";
        }
    }
}
