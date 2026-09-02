package io.github.marcschmidt1999.reactive.sqs.soak;

import java.time.Duration;
import java.time.Instant;

/** Reads the strongly consistent DynamoDB ledger and prints a reconciliation report. */
public final class SoakReportMain {

    private SoakReportMain() {}

    public static void main(String[] args) {
        var options = ReportOptions.parse(args);
        try (var dynamo = SoakApplication.dynamoClient(options.region())) {
            var records =
                    new DynamoAuditStore(dynamo, options.tableName())
                            .records(options.runId())
                            .join();
            var report = SoakReport.classify(records, Instant.now(), options.unresolvedGrace());
            print(options.runId(), report);
        }
    }

    private static void print(String runId, SoakReport report) {
        System.out.printf("run_id=%s%n", runId);
        System.out.printf("prepared=%d%n", report.totalPrepared());
        System.out.printf(
                "send_confirmed=%d%n", report.totalPrepared() - report.sendUncertain().size());
        print("processed", report.processed());
        print("dlq", report.dlq());
        print("processed_and_dlq", report.processedAndDlq());
        print("retried", report.retried());
        print("in_flight", report.inFlight());
        print("overdue_loss_candidates", report.missing());
        print("send_uncertain", report.sendUncertain());
        print("outcome_mismatches", report.outcomeMismatches());
        System.out.println("result=LIVE_SNAPSHOT_NOT_A_LOSS_PROOF");
    }

    private static void print(String name, java.util.List<String> ids) {
        System.out.printf("%s=%d", name, ids.size());
        if (!ids.isEmpty()) {
            System.out.printf(" sample=%s", ids.subList(0, Math.min(ids.size(), 20)));
        }
        System.out.println();
    }

    record ReportOptions(String region, String tableName, String runId, Duration unresolvedGrace) {

        static ReportOptions parse(String[] args) {
            String region = null;
            String tableName = null;
            String runId = null;
            var unresolvedGrace = Duration.ofMinutes(5);
            for (var argument : args) {
                if (argument.startsWith("--region=")) {
                    region = value(argument, "--region=");
                } else if (argument.startsWith("--table=")) {
                    tableName = value(argument, "--table=");
                } else if (argument.startsWith("--run-id=")) {
                    runId = value(argument, "--run-id=");
                } else if (argument.startsWith("--grace=")) {
                    unresolvedGrace = Duration.parse(value(argument, "--grace="));
                } else {
                    throw new IllegalArgumentException(usage());
                }
            }
            if (region == null || tableName == null || runId == null) {
                throw new IllegalArgumentException(usage());
            }
            if (unresolvedGrace.isNegative() || unresolvedGrace.isZero()) {
                throw new IllegalArgumentException("grace must be positive");
            }
            return new ReportOptions(region, tableName, runId, unresolvedGrace);
        }

        private static String value(String argument, String prefix) {
            var value = argument.substring(prefix.length());
            if (value.isBlank()) {
                throw new IllegalArgumentException(usage());
            }
            return value;
        }

        private static String usage() {
            return "Required: --region=... --table=... --run-id=...; "
                    + "optional: --grace=PT5M. This command only prints a live snapshot; "
                    + "use infra/reconcile.sh for a final loss proof";
        }
    }
}
