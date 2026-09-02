# AWS soak-test infrastructure

This stack runs only the consumers on one `t4g.micro` Amazon Linux 2023 ARM64
instance. A producer started on your local machine continuously sends uniquely
identified messages. DynamoDB is the evidence ledger used to distinguish normal
processing, an intentionally poisoned message reaching the DLQ, and an actually
missing message.

The important final invariant is:

```text
SQS-confirmed event IDs - (processed event IDs union archived-DLQ event IDs) = empty
```

This is stronger than looking at SQS's approximate queue counters. It is only a
proof after the producer has stopped and both queues have drained. During a live
run, temporary unmatched events are expected.

## What is deployed

- A standard source queue and standard DLQ with a redrive policy. Both use
  SQS-managed encryption and 14-day message retention.
- A DynamoDB `PAY_PER_REQUEST` ledger keyed by `runId` (partition key) and
  `eventId` (sort key). `expiresAt` is its TTL attribute.
- One `t4g.micro` instance in a supplied public subnet, managed by a one-instance
  Auto Scaling group so an artifact update replaces the instance.
- An 8 GiB encrypted `gp3` root disk, IMDSv2-only metadata, and a security group
  with no ingress. Outbound HTTPS is needed for SSM and the AWS APIs.
- Systems Manager access with no SSH port. The application and randomized restart
  chaos timer are both systemd units.
- A CloudWatch log group containing bootstrap and application logs.

The instance role can receive, delete, inspect, and change visibility on only the
two queues; update only existing rows in the ledger; read only the selected S3
artifact; publish only to the stack log group; and use the AWS-managed SSM core
policy. It cannot send workload messages or create producer ledger rows.

The DynamoDB ledger and CloudWatch log group are retained when the stack is
deleted. Ledger rows expire after their configured TTL; CloudWatch logs use the
stack's retention setting. The private, versioned artifact bucket is intentionally
outside the stack and is not deleted automatically.

## Prerequisites

- AWS CLI v2 credentials for the target account
- `jq`, Java 21, and a POSIX environment with Bash
- A default VPC/public default subnet in the target region, or an explicit public
  subnet passed with `--subnet-id`
- Deployment permissions for CloudFormation, IAM, EC2/Auto Scaling, SQS,
  DynamoDB, S3, Logs, and SSM
- Local producer permissions for `sqs:SendMessage`, `dynamodb:PutItem`, and
  `dynamodb:UpdateItem` on the stack queue/table; the status/reconciliation
  scripts additionally read queue/table state and use SSM Run Command to freeze
  the consumer for a final snapshot

The scripts default to `eu-central-1`. Use `--profile` and `--region` when needed.
They do not deploy anything merely by being checked into the repository.

## 1. Build and deploy

From the repository root:

```bash
./gradlew :reactive-sqs-soak:check :reactive-sqs-soak:distTar
samples/reactive-sqs-soak/infra/deploy.sh \
  --region eu-central-1 \
  --run-id soak-20260902-01
```

`deploy.sh` verifies the archive layout, hashes it, uploads it under a
content-addressed key in a private encrypted/versioned S3 bucket, discovers and
validates a public default subnet, and deploys `template.yaml`. Pass `--bucket` to
reuse a particular private deployment bucket, or `--subnet-id` when the account
has no suitable default subnet.

Each stack name is permanently associated with one run ID. Updating code for an
existing stack reuses that ID; `deploy.sh` refuses to switch an existing stack to
a different run because its queues may still contain messages from the old run.
Use a new `--stack` name for a separate experiment.

The instance verifies the SHA-256 digest before extracting the installDist
archive. Bootstrap also proves that the instance role can reach both queues and
the DynamoDB update path, then waits for Spring's application-ready log before
signalling CloudFormation. Its service starts
`/opt/reactive-sqs-soak/bin/reactive-sqs-soak`. The timer restarts the consumer at
a random interval between 5 and 20 minutes by default; tune that range with
`--chaos-min-seconds` and `--chaos-jitter-seconds`.

## 2. Check bootstrap and consumer health

```bash
samples/reactive-sqs-soak/infra/status.sh \
  --stack reactive-sqs-soak \
  --region eu-central-1
```

This prints stack/instance state, all three approximate SQS counters for both
queues, a systemd snapshot fetched through SSM, and a live ledger summary. A live
summary is deliberately labelled “not a loss proof.” The first boot installs
packages and can take several minutes. Cloud-init failures appear in the
`bootstrap` CloudWatch stream.

## 3. Run the producer locally

The wrapper discovers the source queue, ledger table, and run ID from stack
outputs. It also uses the evidence TTL chosen at deployment unless
`--ttl-days` overrides it. It never runs the producer on EC2.

```bash
samples/reactive-sqs-soak/infra/producer.sh start \
  --stack reactive-sqs-soak \
  --region eu-central-1 \
  --rate 10 \
  --duration PT24H \
  --seed 42
```

The producer creates a ledger row before each send and records `acceptedAtMs`
only after SQS confirms the send. The workload deterministically mixes normal,
retry-once, visibility-boundary, and poison messages with randomized CPU work.
The EC2 consumer records `processedAtMs` before a successful handler completion.
Its DLQ listener records `dlqAtMs` before deleting an intentionally failed
message. Those write-before-delete relationships are what make the final ledger
useful for detecting library-level loss.

Local producer control and logs:

```bash
samples/reactive-sqs-soak/infra/producer.sh status --run-id soak-20260902-01
samples/reactive-sqs-soak/infra/producer.sh stop --run-id soak-20260902-01
```

Background state and logs live under the module's ignored `build/soak-state/`
directory. Use `producer.sh run` instead of `start` to keep it in the foreground.

## 4. Reconcile after the producer stops

Stop the local producer first, leave the EC2 consumer running, and reconcile:

```bash
samples/reactive-sqs-soak/infra/reconcile.sh \
  --stack reactive-sqs-soak \
  --region eu-central-1 \
  --wait-seconds 1800 \
  --min-accepted 10000
```

The command first requires the producer's final manifest, waits for 18 consecutive
empty samples from both queues, then disables the restart timer and gracefully
stops the consumer. It repeats the three-minute empty-window check while the
consumer is frozen before performing a strongly consistent DynamoDB query. A
clean result leaves the consumer stopped so the evidence cannot race a late
duplicate update. A failed or interrupted reconciliation automatically resumes
it. The command exits nonzero for any of these cases:

- an SQS-confirmed event has neither `processedAtMs` nor `dlqAtMs`;
- a prepared send has no acceptance evidence, so its delivery is ambiguous;
- an event has both terminal states, a terminal state without acceptance, or an
  outcome contrary to its workload mode;
- the producer's immutable `!manifest` row is absent/incomplete or its final
  accepted count differs from the event rows;
- the queues did not drain; or
- the run is too small to meet `--min-accepted`.

It prints up to 20 offending event IDs for investigation. A poison message with
`dlqAtMs` is a tested failure path, not a missing message. Standard-queue
duplicates are also expected; the ledger's attempt set exposes retries
without treating them as loss.

Do not claim a clean result from `--skip-queue-check` or `--report-only`; those
options are for diagnosis while work may still be in flight.

## 5. Tear down

Cleanup is intentionally gated by `--yes`, and a clean reconciliation is required
by default:

```bash
samples/reactive-sqs-soak/infra/stop.sh \
  --stack reactive-sqs-soak \
  --region eu-central-1 \
  --min-accepted 10000 \
  --yes
```

This removes the instance, queues, role, and networking resources but retains the
ledger and logs. `--purge-evidence --yes` permanently deletes those too. Use
`--force --yes` only when deliberately abandoning a failed/incomplete run. Stop
the local producer before deleting its queue.

AWS charges can accrue for EC2, SQS requests, DynamoDB requests/storage and point-
in-time recovery, CloudWatch Logs, and S3. Tear the stack down when the experiment
ends and remove retained evidence/artifact versions once they are no longer
needed.
