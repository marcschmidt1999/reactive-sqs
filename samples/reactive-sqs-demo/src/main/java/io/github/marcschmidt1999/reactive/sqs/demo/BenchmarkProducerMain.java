package io.github.marcschmidt1999.reactive.sqs.demo;

/** Runs a fixed-payload, batched SQS workload against the demo queue. */
public final class BenchmarkProducerMain {

    private static final int DEFAULT_MESSAGES = 5_000;
    private static final int DEFAULT_BATCH_CONCURRENCY = 16;

    private BenchmarkProducerMain() {}

    public static void main(String[] args) {
        var options = Options.parse(args);
        try (var sqs = DemoApplication.createSqsClient(options.region())) {
            var result =
                    new BenchmarkMessageProducer(sqs, options.queueUrl())
                            .send(options.messages(), options.concurrency());
            System.out.printf(
                    "Sent %d benchmark messages in %d batches in %.3f seconds.%n",
                    result.messagesSent(),
                    result.batchesSent(),
                    result.elapsed().toNanos() / 1_000_000_000.0);
        }
    }

    private record Options(String queueUrl, String region, int messages, int concurrency) {

        private static Options parse(String[] args) {
            String queueUrl = null;
            String region = null;
            var messages = DEFAULT_MESSAGES;
            var concurrency = DEFAULT_BATCH_CONCURRENCY;
            for (var argument : args) {
                if (argument.startsWith("--queue-url=")) {
                    queueUrl = requiredValue(argument, "--queue-url=");
                } else if (argument.startsWith("--region=")) {
                    region = requiredValue(argument, "--region=");
                } else if (argument.startsWith("--messages=")) {
                    messages = parsePositive(argument, "--messages=");
                } else if (argument.startsWith("--concurrency=")) {
                    concurrency = parsePositive(argument, "--concurrency=");
                } else {
                    throw new IllegalArgumentException(usage());
                }
            }
            if (queueUrl == null || region == null) {
                throw new IllegalArgumentException(usage());
            }
            return new Options(queueUrl, region, messages, concurrency);
        }

        private static String requiredValue(String argument, String prefix) {
            var value = argument.substring(prefix.length());
            if (value.isBlank()) {
                throw new IllegalArgumentException(usage());
            }
            return value;
        }

        private static String usage() {
            return "Usage: --queue-url=<url> --region=<region> "
                    + "[--messages=<1..100000>] [--concurrency=<1..64>]";
        }

        private static int parsePositive(String argument, String prefix) {
            try {
                return Integer.parseInt(argument.substring(prefix.length()));
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException("Invalid value for " + prefix, exception);
            }
        }
    }
}
