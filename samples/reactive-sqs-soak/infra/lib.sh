#!/usr/bin/env bash

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

validate_stack_name() {
  [[ $1 =~ ^[A-Za-z][A-Za-z0-9-]{0,127}$ ]] ||
    die "invalid CloudFormation stack name: $1"
}

validate_run_id() {
  [[ $1 =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    die "run ID must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}"
}

aws_region() {
  local region=$1
  shift
  command aws --no-cli-pager --region "$region" "$@"
}

stack_output() {
  local region=$1
  local stack_name=$2
  local output_key=$3
  local value

  value=$(aws_region "$region" cloudformation describe-stacks \
    --stack-name "$stack_name" \
    --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue | [0]" \
    --output text)
  [[ -n $value && $value != None ]] ||
    die "stack $stack_name has no $output_key output"
  printf '%s\n' "$value"
}

stack_exists() {
  local region=$1
  local stack_name=$2
  aws_region "$region" cloudformation describe-stacks \
    --stack-name "$stack_name" >/dev/null 2>&1
}

queue_counts_json() {
  local region=$1
  local queue_url=$2
  aws_region "$region" sqs get-queue-attributes \
    --queue-url "$queue_url" \
    --attribute-names \
      ApproximateNumberOfMessages \
      ApproximateNumberOfMessagesDelayed \
      ApproximateNumberOfMessagesNotVisible \
    --query Attributes \
    --output json
}

queue_total() {
  jq -r '(
    (.ApproximateNumberOfMessages // "0" | tonumber) +
    (.ApproximateNumberOfMessagesDelayed // "0" | tonumber) +
    (.ApproximateNumberOfMessagesNotVisible // "0" | tonumber)
  )'
}

instance_for_asg() {
  local region=$1
  local asg_name=$2
  aws_region "$region" autoscaling describe-auto-scaling-groups \
    --auto-scaling-group-names "$asg_name" \
    --query "AutoScalingGroups[0].Instances[?LifecycleState=='InService'].InstanceId | [0]" \
    --output text
}

