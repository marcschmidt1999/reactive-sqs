#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
    cat <<'USAGE'
Usage: demo-queue.sh <region> <queue-name> <command> [argument]

Commands:
  create                 Create the standard demo queue and print its URL.
  url                    Print the queue URL.
  send [count]           Send 10 (or count) successful JSON messages.
  send-failing [count]   Send 1 (or count) deliberately failing messages.
  status                 Show approximate queue counts and settings.
  purge --yes            Permanently remove all queue messages.
  delete --yes           Permanently delete the queue.

Example:
  demo-queue.sh eu-central-1 reactive-sqs-demo-alice create
USAGE
}

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

[[ "${1:-}" != "help" && "${1:-}" != "--help" && "${1:-}" != "-h" ]] || {
    usage
    exit 0
}

[[ $# -ge 3 ]] || {
    usage >&2
    exit 1
}

region="$1"
queue_name="$2"
command_name="$3"
argument="${4:-}"

command -v aws >/dev/null 2>&1 || die "AWS CLI is required"
[[ "$queue_name" != *.fifo ]] || die "this demo supports standard queues only"

aws_cli() {
    aws --region "$region" --no-cli-pager "$@"
}

queue_url() {
    aws_cli sqs get-queue-url --queue-name "$queue_name" --query QueueUrl --output text
}

validate_count() {
    [[ "$1" =~ ^[0-9]+$ ]] || die "message count must be a positive integer"
    (( $1 >= 1 && $1 <= 1000 )) || die "message count must be between 1 and 1000"
}

send_messages() {
    local count="$1"
    local should_fail="$2"
    local url run_id index body

    validate_count "$count"
    url="$(queue_url)"
    run_id="$(date -u +%Y%m%dT%H%M%SZ)"
    for ((index = 1; index <= count; index++)); do
        body="$(printf '{"id":"demo-%s-%04d","text":"hello from aws-cli","shouldFail":%s}' \
            "$run_id" "$index" "$should_fail")"
        aws_cli sqs send-message --queue-url "$url" --message-body "$body" \
            --query MessageId --output text
    done
}

require_yes() {
    [[ "$argument" == "--yes" ]] || die "destructive command requires --yes"
}

case "$command_name" in
    create)
        aws_cli sqs create-queue \
            --queue-name "$queue_name" \
            --attributes 'VisibilityTimeout=60,ReceiveMessageWaitTimeSeconds=20,MessageRetentionPeriod=86400,SqsManagedSseEnabled=true' \
            --query QueueUrl \
            --output text
        ;;
    url)
        queue_url
        ;;
    send)
        send_messages "${argument:-10}" false
        ;;
    send-failing)
        send_messages "${argument:-1}" true
        ;;
    status)
        aws_cli sqs get-queue-attributes \
            --queue-url "$(queue_url)" \
            --attribute-names \
                ApproximateNumberOfMessages \
                ApproximateNumberOfMessagesNotVisible \
                ApproximateNumberOfMessagesDelayed \
                VisibilityTimeout \
                ReceiveMessageWaitTimeSeconds \
            --query Attributes \
            --output table
        ;;
    purge)
        require_yes
        aws_cli sqs purge-queue --queue-url "$(queue_url)"
        ;;
    delete)
        require_yes
        aws_cli sqs delete-queue --queue-url "$(queue_url)"
        ;;
    *)
        usage >&2
        die "unknown command: $command_name"
        ;;
esac
