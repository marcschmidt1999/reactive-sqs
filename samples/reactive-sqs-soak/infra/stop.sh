#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

usage() {
  cat <<'USAGE'
Usage: stop.sh --yes [options]

By default the script first requires a clean reconciliation, deletes the active
stack, and retains the DynamoDB ledger and CloudWatch logs as evidence.

Options:
  --yes                required confirmation for destructive cleanup
  --stack NAME         CloudFormation stack (default: reactive-sqs-soak)
  --region REGION      AWS region (default: AWS_REGION/AWS_DEFAULT_REGION/eu-central-1)
  --profile PROFILE    AWS CLI profile
  --force              delete even if reconciliation fails or cannot run
  --purge-evidence     also permanently delete the retained ledger and log group
  --min-accepted N     minimum accepted events for reconciliation (default: 1)
  -h, --help           show this help
USAGE
}

confirmed=false
force=false
purge_evidence=false
stack_name=reactive-sqs-soak
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-eu-central-1}}
profile=""
min_accepted=1

while (($#)); do
  case "$1" in
    --yes) confirmed=true; shift ;;
    --force) force=true; shift ;;
    --purge-evidence) purge_evidence=true; shift ;;
    --stack) stack_name=${2:?missing value for --stack}; shift 2 ;;
    --region) region=${2:?missing value for --region}; shift 2 ;;
    --profile) profile=${2:?missing value for --profile}; shift 2 ;;
    --min-accepted) min_accepted=${2:?missing value for --min-accepted}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ $confirmed == true ]] || die "refusing destructive cleanup without --yes"
require_command aws
require_command jq
validate_stack_name "$stack_name"
[[ $min_accepted =~ ^[0-9]+$ ]] || die "--min-accepted must be an integer"

if [[ -n $profile ]]; then
  export AWS_PROFILE=$profile
fi
export AWS_PAGER=""

stack_exists "$region" "$stack_name" || die "stack not found: $stack_name"
table_name=$(stack_output "$region" "$stack_name" LedgerTableName)
log_group=$(stack_output "$region" "$stack_name" LogGroupName)

if [[ $force == false ]]; then
  "$SCRIPT_DIR/reconcile.sh" \
    --stack "$stack_name" --region "$region" --min-accepted "$min_accepted"
else
  printf 'WARNING: skipping the clean-reconciliation requirement because --force was supplied.\n' >&2
fi

printf 'Deleting active stack resources for %s in %s\n' "$stack_name" "$region"
aws_region "$region" cloudformation delete-stack --stack-name "$stack_name"
aws_region "$region" cloudformation wait stack-delete-complete --stack-name "$stack_name"

if [[ $purge_evidence == true ]]; then
  printf 'Permanently deleting retained DynamoDB table %s\n' "$table_name"
  aws_region "$region" dynamodb delete-table --table-name "$table_name" >/dev/null
  aws_region "$region" dynamodb wait table-not-exists --table-name "$table_name"
  printf 'Permanently deleting retained CloudWatch log group %s\n' "$log_group"
  aws_region "$region" logs delete-log-group --log-group-name "$log_group"
else
  cat <<RETAINED
Stack deleted. Evidence was retained:
  DynamoDB table:     $table_name
  CloudWatch log group: $log_group

Delete it later only after exporting anything you need. A future teardown can purge
evidence by supplying both --yes and --purge-evidence before its stack is deleted.
RETAINED
fi
