# Reactive SQS demo

This is a small Spring Boot 3 app that reads messages from a standard SQS queue.

It maps JSON to `DemoMessage`. When the handler completes, the message is deleted. Deletes are
sent to SQS in small batches.

## Requirements

- JDK 21+
- AWS CLI v2
- AWS credentials that can access your SQS queue

The app uses the normal AWS SDK credential chain. For AWS SSO, log in first:

```shell
aws sso login --profile <profile>
```

## Create a queue

Choose a region and a queue name:

```shell
DEMO_REGION=eu-central-1
DEMO_QUEUE=reactive-sqs-demo-example

./samples/reactive-sqs-demo/demo-queue.sh \
  "$DEMO_REGION" "$DEMO_QUEUE" create
```

Get the queue URL:

```shell
DEMO_QUEUE_URL=$(./samples/reactive-sqs-demo/demo-queue.sh \
  "$DEMO_REGION" "$DEMO_QUEUE" url)
```

The helper creates a standard queue with a 60-second visibility timeout and 20-second long
polling.

## Run the app

```shell
./gradlew :reactive-sqs-demo:run \
  --args="--demo.aws-region=$DEMO_REGION --demo.queue-url=$DEMO_QUEUE_URL" \
  --no-daemon
```

Useful endpoints:

```shell
curl http://localhost:8080/actuator/health
curl http://localhost:8080/actuator/metrics/reactive.sqs.delivery
curl http://localhost:8080/actuator/prometheus
```

## Send messages

In another terminal:

```shell
./samples/reactive-sqs-demo/demo-queue.sh \
  "$DEMO_REGION" "$DEMO_QUEUE" send 25

./samples/reactive-sqs-demo/demo-queue.sh \
  "$DEMO_REGION" "$DEMO_QUEUE" status
```

To send a message that fails in the handler:

```shell
./samples/reactive-sqs-demo/demo-queue.sh \
  "$DEMO_REGION" "$DEMO_QUEUE" send-failing 1
```

It is not deleted, so SQS will deliver it again.

## Performance report

Run the app with less logging:

```shell
./gradlew :reactive-sqs-demo:run \
  --args="--spring.profiles.active=benchmark --demo.aws-region=$DEMO_REGION --demo.queue-url=$DEMO_QUEUE_URL" \
  --no-daemon
```

Reset the report, send work, then fetch it:

```shell
curl -X POST http://localhost:8080/demo/performance/reset

./gradlew :reactive-sqs-demo:benchmarkSend \
  --args="--queue-url=$DEMO_QUEUE_URL --region=$DEMO_REGION --messages=5000 --concurrency=16" \
  --no-daemon

curl http://localhost:8080/demo/performance
```

The report shows throughput, handler time, receive time, delete time, delete-batch size, retries,
and listener state.

## Delete the queue

Stop the app first.

```shell
./samples/reactive-sqs-demo/demo-queue.sh \
  "$DEMO_REGION" "$DEMO_QUEUE" delete --yes
```

To remove messages but keep the queue:

```shell
./samples/reactive-sqs-demo/demo-queue.sh \
  "$DEMO_REGION" "$DEMO_QUEUE" purge --yes
```

## IAM permissions

The app needs:

```text
sqs:ReceiveMessage
sqs:DeleteMessage
sqs:ChangeMessageVisibility
```

The helper also needs:

```text
sqs:CreateQueue
sqs:GetQueueUrl
sqs:GetQueueAttributes
sqs:SendMessage
sqs:PurgeQueue
sqs:DeleteQueue
```
