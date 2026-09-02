#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

usage() {
  cat <<'USAGE'
Usage: status.sh [options]

Options:
  --stack NAME       CloudFormation stack (default: reactive-sqs-soak)
  --run-id ID        ledger partition (default: stack RunId output)
  --region REGION    AWS region (default: AWS_REGION/AWS_DEFAULT_REGION/eu-central-1)
  --profile PROFILE  AWS CLI profile
  --skip-remote      do not inspect systemd through SSM Run Command
  --skip-ledger      do not print the live ledger reconciliation snapshot
  -h, --help         show this help
USAGE
}

stack_name=reactive-sqs-soak
run_id=""
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-eu-central-1}}
profile=""
inspect_remote=true
inspect_ledger=true

while (($#)); do
  case "$1" in
    --stack) stack_name=${2:?missing value for --stack}; shift 2 ;;
    --run-id) run_id=${2:?missing value for --run-id}; shift 2 ;;
    --region) region=${2:?missing value for --region}; shift 2 ;;
    --profile) profile=${2:?missing value for --profile}; shift 2 ;;
    --skip-remote) inspect_remote=false; shift ;;
    --skip-ledger) inspect_ledger=false; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

require_command aws
require_command jq
validate_stack_name "$stack_name"
if [[ -n $run_id ]]; then
  validate_run_id "$run_id"
fi

if [[ -n $profile ]]; then
  export AWS_PROFILE=$profile
fi
export AWS_PAGER=""

stack_exists "$region" "$stack_name" || die "stack not found: $stack_name"
stack_status=$(aws_region "$region" cloudformation describe-stacks \
  --stack-name "$stack_name" --query 'Stacks[0].StackStatus' --output text)
if [[ -z $run_id ]]; then
  run_id=$(stack_output "$region" "$stack_name" RunId)
fi
source_queue_url=$(stack_output "$region" "$stack_name" SourceQueueUrl)
dlq_queue_url=$(stack_output "$region" "$stack_name" DlqQueueUrl)
table_name=$(stack_output "$region" "$stack_name" LedgerTableName)
asg_name=$(stack_output "$region" "$stack_name" AutoScalingGroupName)
log_group=$(stack_output "$region" "$stack_name" LogGroupName)

source_counts=$(queue_counts_json "$region" "$source_queue_url")
dlq_counts=$(queue_counts_json "$region" "$dlq_queue_url")
format_counts='"available=\(.ApproximateNumberOfMessages // "0") in-flight=\(.ApproximateNumberOfMessagesNotVisible // "0") delayed=\(.ApproximateNumberOfMessagesDelayed // "0")"'

cat <<STATUS
Stack:           $stack_name ($stack_status)
Region:          $region
Run ID:          $run_id
Ledger table:    $table_name
CloudWatch logs: $log_group
Source queue:    $(jq -r "$format_counts" <<<"$source_counts")
DLQ:             $(jq -r "$format_counts" <<<"$dlq_counts")
STATUS

instance_id=$(instance_for_asg "$region" "$asg_name")
if [[ -z $instance_id || $instance_id == None ]]; then
  printf 'Instance:         no InService instance in %s\n' "$asg_name"
else
  instance_state=$(aws_region "$region" ec2 describe-instances \
    --instance-ids "$instance_id" \
    --query 'Reservations[0].Instances[0].State.Name' --output text)
  ping_status=$(aws_region "$region" ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=$instance_id" \
    --query 'InstanceInformationList[0].PingStatus' --output text)
  printf 'Instance:         %s (EC2=%s, SSM=%s)\n' \
    "$instance_id" "$instance_state" "$ping_status"

  if [[ $inspect_remote == true && $ping_status == Online ]]; then
    command_parameters=$(jq -cn '{commands: [
      "systemctl show reactive-sqs-soak.service --property=ActiveState,SubState,NRestarts,ExecMainStartTimestamp --no-pager",
      "systemctl show reactive-sqs-soak-chaos.timer --property=ActiveState,SubState,NextElapseUSecRealtime --no-pager"
    ]}')
    command_id=$(aws_region "$region" ssm send-command \
      --instance-ids "$instance_id" \
      --document-name AWS-RunShellScript \
      --comment 'reactive-sqs soak status' \
      --parameters "$command_parameters" \
      --query 'Command.CommandId' --output text)
    aws_region "$region" ssm wait command-executed \
      --command-id "$command_id" --instance-id "$instance_id" || true
    remote_status=$(aws_region "$region" ssm get-command-invocation \
      --command-id "$command_id" --instance-id "$instance_id" \
      --query '{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}' \
      --output json)
    printf '\nRemote systemd status (%s):\n' "$(jq -r .Status <<<"$remote_status")"
    jq -r '.Output, .Error | select(length > 0)' <<<"$remote_status"
  fi
fi

if [[ $inspect_ledger == true ]]; then
  printf '\n'
  "$SCRIPT_DIR/reconcile.sh" \
    --stack "$stack_name" --region "$region" --run-id "$run_id" --report-only
fi

cat <<NEXT

Tail application logs with:
  aws --region $region logs tail '$log_group' --follow
NEXT
