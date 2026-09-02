#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

usage() {
  cat <<'USAGE'
Usage: reconcile.sh [options]

Prove that every SQS-accepted event has either completed normally or was archived
from the DLQ. Run this after stopping the local producer.

Options:
  --stack NAME          CloudFormation stack (default: reactive-sqs-soak)
  --run-id ID           ledger partition (default: stack RunId output)
  --region REGION       AWS region (default: AWS_REGION/AWS_DEFAULT_REGION/eu-central-1)
  --profile PROFILE     AWS CLI profile
  --wait-seconds N      maximum drain wait (default: 600)
  --poll-seconds N      queue polling interval (default: 10)
  --stable-samples N    consecutive empty samples required (default: 18)
  --min-accepted N      reject vacuous runs below this count (default: 1)
  --skip-queue-check    live snapshot only; cannot produce a clean result
  --report-only         live snapshot; always exit zero and do not wait
  -h, --help            show this help

A clean proof requires:
  accepted event IDs - (processed event IDs union archived-DLQ event IDs) = empty
It also rejects ambiguous sends, double terminal states, and a non-drained queue.
USAGE
}

stack_name=reactive-sqs-soak
run_id=""
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-eu-central-1}}
profile=""
wait_seconds=600
poll_seconds=10
stable_samples=18
min_accepted=1
skip_queue_check=false
report_only=false

while (($#)); do
  case "$1" in
    --stack) stack_name=${2:?missing value for --stack}; shift 2 ;;
    --run-id) run_id=${2:?missing value for --run-id}; shift 2 ;;
    --region) region=${2:?missing value for --region}; shift 2 ;;
    --profile) profile=${2:?missing value for --profile}; shift 2 ;;
    --wait-seconds) wait_seconds=${2:?missing value for --wait-seconds}; shift 2 ;;
    --poll-seconds) poll_seconds=${2:?missing value for --poll-seconds}; shift 2 ;;
    --stable-samples) stable_samples=${2:?missing value for --stable-samples}; shift 2 ;;
    --min-accepted) min_accepted=${2:?missing value for --min-accepted}; shift 2 ;;
    --skip-queue-check) skip_queue_check=true; report_only=true; shift ;;
    --report-only) report_only=true; skip_queue_check=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

require_command aws
require_command jq
validate_stack_name "$stack_name"
[[ $wait_seconds =~ ^[0-9]+$ ]] || die "--wait-seconds must be an integer"
[[ $poll_seconds =~ ^[1-9][0-9]*$ ]] || die "--poll-seconds must be a positive integer"
[[ $stable_samples =~ ^[1-9][0-9]*$ ]] || die "--stable-samples must be a positive integer"
[[ $min_accepted =~ ^[0-9]+$ ]] || die "--min-accepted must be an integer"

if [[ -n $profile ]]; then
  export AWS_PROFILE=$profile
fi
export AWS_PAGER=""

stack_exists "$region" "$stack_name" || die "stack not found: $stack_name"
if [[ -z $run_id ]]; then
  run_id=$(stack_output "$region" "$stack_name" RunId)
fi
validate_run_id "$run_id"

source_queue_url=$(stack_output "$region" "$stack_name" SourceQueueUrl)
dlq_queue_url=$(stack_output "$region" "$stack_name" DlqQueueUrl)
table_name=$(stack_output "$region" "$stack_name" LedgerTableName)
asg_name=$(stack_output "$region" "$stack_name" AutoScalingGroupName)

ledger_file=""
consumer_quiesced=false
keep_consumer_stopped=false
instance_id=""

run_remote() {
  local comment=$1
  local parameters=$2
  local command_id
  local status

  command_id=$(aws_region "$region" ssm send-command \
    --instance-ids "$instance_id" \
    --document-name AWS-RunShellScript \
    --comment "$comment" \
    --parameters "$parameters" \
    --query 'Command.CommandId' --output text) || return 1
  aws_region "$region" ssm wait command-executed \
    --command-id "$command_id" --instance-id "$instance_id" || return 1
  status=$(aws_region "$region" ssm get-command-invocation \
    --command-id "$command_id" --instance-id "$instance_id" \
    --query Status --output text) || return 1
  [[ $status == Success ]]
}

resume_consumer() {
  local parameters
  parameters=$(jq -cn '{commands: [
    "systemctl enable --now reactive-sqs-soak.service",
    "systemctl enable --now reactive-sqs-soak-chaos.timer",
    "systemctl is-active --quiet reactive-sqs-soak.service",
    "systemctl is-active --quiet reactive-sqs-soak-chaos.timer"
  ]}')
  run_remote 'resume reactive-sqs soak after incomplete reconciliation' "$parameters"
}

cleanup() {
  local exit_status=$?
  [[ -z $ledger_file ]] || rm -f "$ledger_file"
  if [[ $consumer_quiesced == true && $keep_consumer_stopped == false ]]; then
    printf '\nReconciliation was not clean; resuming the EC2 consumer.\n' >&2
    resume_consumer ||
      printf 'WARNING: could not resume the consumer through SSM; inspect it with status.sh.\n' >&2
  fi
  exit "$exit_status"
}
trap cleanup EXIT

wait_for_empty_queues() {
  local label=$1
  local stable=0
  local deadline=$((SECONDS + wait_seconds))
  local source_counts
  local dlq_counts
  local source_total
  local dlq_total

  while ((SECONDS <= deadline)); do
    source_counts=$(queue_counts_json "$region" "$source_queue_url")
    dlq_counts=$(queue_counts_json "$region" "$dlq_queue_url")
    source_total=$(queue_total <<<"$source_counts")
    dlq_total=$(queue_total <<<"$dlq_counts")

    printf '%s: source=%s dlq=%s stable=%s/%s\n' \
      "$label" "$source_total" "$dlq_total" "$stable" "$stable_samples"
    if ((source_total == 0 && dlq_total == 0)); then
      stable=$((stable + 1))
      if ((stable >= stable_samples)); then
        return 0
      fi
    else
      stable=0
    fi
    sleep "$poll_seconds"
  done
  return 1
}

if [[ $report_only == false ]]; then
  manifest_key=$(jq -cn --arg run_id "$run_id" \
    '{runId: {S: $run_id}, eventId: {S: "!manifest"}}')
  manifest=$(aws_region "$region" dynamodb get-item \
    --table-name "$table_name" \
    --key "$manifest_key" \
    --consistent-read \
    --projection-expression 'producerFinishedAtMs, expectedAcceptedCount' \
    --output json)
  jq -e '.Item.producerFinishedAtMs.N? != null and .Item.expectedAcceptedCount.N? != null' \
    <<<"$manifest" >/dev/null ||
    die "producer manifest is not finished; stop the local producer before final reconciliation"
fi

queues_drained=false
if [[ $skip_queue_check == false ]]; then
  if wait_for_empty_queues 'Active drain sample'; then
    instance_id=$(instance_for_asg "$region" "$asg_name")
    [[ -n $instance_id && $instance_id != None ]] ||
      die "cannot quiesce the consumer because the stack has no InService instance"
    ping_status=$(aws_region "$region" ssm describe-instance-information \
      --filters "Key=InstanceIds,Values=$instance_id" \
      --query 'InstanceInformationList[0].PingStatus' --output text)
    [[ $ping_status == Online ]] ||
      die "cannot quiesce consumer $instance_id because SSM status is $ping_status"
    stop_parameters=$(jq -cn '{commands: [
      "systemctl disable --now reactive-sqs-soak-chaos.timer",
      "systemctl stop reactive-sqs-soak.service",
      "test \"$(systemctl is-active reactive-sqs-soak.service)\" = inactive"
    ]}')
    run_remote 'quiesce reactive-sqs soak for final reconciliation' "$stop_parameters" ||
      die "failed to quiesce consumer $instance_id through SSM"
    consumer_quiesced=true
    printf 'Consumer %s is quiesced; checking for late-visible messages.\n' "$instance_id"
    if wait_for_empty_queues 'Frozen drain sample'; then
      queues_drained=true
    fi
  fi
fi

ledger_file=$(mktemp)
expression_values=$(jq -cn --arg run_id "$run_id" '{":runId": {"S": $run_id}}')

aws_region "$region" dynamodb query \
  --table-name "$table_name" \
  --consistent-read \
  --key-condition-expression '#runId = :runId' \
  --expression-attribute-names '{"#runId":"runId"}' \
  --expression-attribute-values "$expression_values" \
  --output json > "$ledger_file"

summary=$(jq -c '
  def has_number($name): .[$name].N? != null;
  .Items as $allItems |
  ([$allItems[] | select(.eventId.S? == "!manifest")][0] // {}) as $manifest |
  [$allItems[] | select(.eventId.S? != "!manifest")] as $items |
  {
    rows: ($items | length),
    manifestPresent: ($manifest.eventId.S? == "!manifest"),
    producerFinished: ($manifest.producerFinishedAtMs.N? != null),
    expectedAccepted: (
      if $manifest.expectedAcceptedCount.N? == null
      then null
      else ($manifest.expectedAcceptedCount.N | tonumber)
      end),
    prepared: ([$items[] | select(has_number("preparedAtMs"))] | length),
    accepted: ([$items[] | select(has_number("acceptedAtMs"))] | length),
    processed: ([$items[] | select(has_number("processedAtMs"))] | length),
    dlq: ([$items[] | select(has_number("dlqAtMs"))] | length),
    acceptedTerminal: ([$items[] |
      select(has_number("acceptedAtMs") and
        (has_number("processedAtMs") or has_number("dlqAtMs")))] | length),
    missing: ([$items[] |
      select(has_number("acceptedAtMs") and
        (has_number("processedAtMs") | not) and
        (has_number("dlqAtMs") | not))] | length),
    ambiguousSend: ([$items[] |
      select(has_number("preparedAtMs") and (has_number("acceptedAtMs") | not))] | length),
    doubleTerminal: ([$items[] |
      select(has_number("processedAtMs") and has_number("dlqAtMs"))] | length),
    terminalWithoutAccepted: ([$items[] |
      select((has_number("acceptedAtMs") | not) and
        (has_number("processedAtMs") or has_number("dlqAtMs")))] | length),
    outcomeMismatch: ([$items[] |
      select(
        (((.mode.S? == "NORMAL") or (.mode.S? == "RETRY_ONCE")) and
          has_number("dlqAtMs")) or
        ((.mode.S? == "POISON") and has_number("processedAtMs"))
      )] | length),
    retried: ([$items[] |
      select(
        ((.attempts.N? // "0" | tonumber) > 1) or
        ((.attemptKeys.SS? // []) | length > 1)
      )] | length),
    modes: ([$items[] | (.mode.S? // "unknown")] | sort | group_by(.) |
      map({mode: .[0], count: length}))
  }
' "$ledger_file")

rows=$(jq -r .rows <<<"$summary")
manifest_present=$(jq -r .manifestPresent <<<"$summary")
producer_finished=$(jq -r .producerFinished <<<"$summary")
expected_accepted=$(jq -r '.expectedAccepted // empty' <<<"$summary")
prepared=$(jq -r .prepared <<<"$summary")
accepted=$(jq -r .accepted <<<"$summary")
processed=$(jq -r .processed <<<"$summary")
dlq=$(jq -r .dlq <<<"$summary")
accepted_terminal=$(jq -r .acceptedTerminal <<<"$summary")
missing=$(jq -r .missing <<<"$summary")
ambiguous_send=$(jq -r .ambiguousSend <<<"$summary")
double_terminal=$(jq -r .doubleTerminal <<<"$summary")
terminal_without_accepted=$(jq -r .terminalWithoutAccepted <<<"$summary")
outcome_mismatch=$(jq -r .outcomeMismatch <<<"$summary")
retried=$(jq -r .retried <<<"$summary")

cat <<REPORT

Ledger reconciliation for run $run_id
  Rows:                       $rows
  Manifest present:           $manifest_present
  Producer finished:          $producer_finished
  Manifest expected accepted: ${expected_accepted:-unknown}
  Prepared by producer:       $prepared
  Confirmed accepted by SQS:  $accepted
  Terminal accepted events:   $accepted_terminal
  Processed normally:         $processed
  Archived from DLQ:          $dlq
  Retried (attempts > 1):     $retried
  MISSING accepted events:    $missing
  Ambiguous prepared sends:   $ambiguous_send
  Double terminal states:     $double_terminal
  Terminal without accepted:  $terminal_without_accepted
  Unexpected mode outcomes:   $outcome_mismatch
REPORT

printf '  Modes:                      '
jq -r '[.modes[] | "\(.mode)=\(.count)"] | if length == 0 then "none" else join(", ") end' \
  <<<"$summary"

print_event_sample() {
  local label=$1
  local filter=$2
  local event_ids
  event_ids=$(jq -r --argjson limit 20 "[
    .Items[] | select($filter) | .eventId.S
  ][0:\$limit][]" "$ledger_file")
  if [[ -n $event_ids ]]; then
    printf '\n%s (up to 20):\n%s\n' "$label" "$event_ids"
  fi
}

print_event_sample 'Missing event IDs' \
  '(.acceptedAtMs.N? != null) and (.processedAtMs.N? == null) and (.dlqAtMs.N? == null)'
print_event_sample 'Ambiguous-send event IDs' \
  '(.preparedAtMs.N? != null) and (.acceptedAtMs.N? == null)'
print_event_sample 'Double-terminal event IDs' \
  '(.processedAtMs.N? != null) and (.dlqAtMs.N? != null)'
print_event_sample 'Unexpected-outcome event IDs' \
  '((((.mode.S? == "NORMAL") or (.mode.S? == "RETRY_ONCE")) and (.dlqAtMs.N? != null)) or ((.mode.S? == "POISON") and (.processedAtMs.N? != null)))'

if [[ $report_only == true ]]; then
  printf '\nResult: LIVE SNAPSHOT (not a loss proof; producer/consumer may still be active)\n'
  exit 0
fi

failure=false
if [[ $queues_drained != true ]]; then
  printf '\nFAIL: source and DLQ were not both empty for %s consecutive samples.\n' \
    "$stable_samples" >&2
  failure=true
fi
if ((accepted < min_accepted)); then
  printf '\nFAIL: only %s accepted events; minimum is %s.\n' "$accepted" "$min_accepted" >&2
  failure=true
fi
if [[ $manifest_present != true ]]; then
  printf '\nFAIL: the immutable !manifest ledger item is missing.\n' >&2
  failure=true
elif [[ $producer_finished != true || -z $expected_accepted ]]; then
  printf '\nFAIL: the producer manifest is incomplete; producerFinishedAtMs/count are required.\n' >&2
  failure=true
elif ((accepted != expected_accepted)); then
  printf '\nFAIL: ledger contains %s accepted events, but the manifest records %s.\n' \
    "$accepted" "$expected_accepted" >&2
  failure=true
fi
if ((missing > 0)); then
  printf '\nFAIL: %s SQS-accepted event(s) have no processed or DLQ terminal evidence.\n' \
    "$missing" >&2
  failure=true
fi
if ((ambiguous_send > 0)); then
  printf '\nFAIL: %s prepared send(s) lack acceptance evidence; their delivery is uncertain.\n' \
    "$ambiguous_send" >&2
  failure=true
fi
if ((double_terminal > 0)); then
  printf '\nFAIL: %s event(s) have both processed and DLQ terminal evidence.\n' \
    "$double_terminal" >&2
  failure=true
fi
if ((terminal_without_accepted > 0)); then
  printf '\nFAIL: %s terminal event(s) lack producer acceptance evidence.\n' \
    "$terminal_without_accepted" >&2
  failure=true
fi
if ((outcome_mismatch > 0)); then
  printf '\nFAIL: %s event(s) reached a terminal state contrary to their workload mode.\n' \
    "$outcome_mismatch" >&2
  failure=true
fi

if [[ $failure == true ]]; then
  printf '\nResult: NOT RECONCILED\n' >&2
  exit 1
fi

keep_consumer_stopped=true
printf '\nResult: CLEAN — every one of %s accepted event IDs is processed or archived from DLQ.\n' \
  "$accepted"
printf 'Consumer %s remains stopped so this evidence snapshot cannot change.\n' "$instance_id"
