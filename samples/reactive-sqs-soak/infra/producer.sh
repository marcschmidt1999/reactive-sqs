#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

usage() {
  cat <<'USAGE'
Usage: producer.sh COMMAND [options]

Commands:
  run                 run the producer in the foreground
  start               run the producer in the background
  status              show local producer status and its last log lines
  stop                gracefully stop the local background producer

Options:
  --stack NAME         CloudFormation stack (default: reactive-sqs-soak)
  --run-id ID          ledger partition (default: stack RunId output)
  --region REGION      AWS region (default: AWS_REGION/AWS_DEFAULT_REGION/eu-central-1)
  --profile PROFILE    AWS CLI profile
  --rate N             messages per second (default: 2)
  --duration ISO-8601  producer duration (default: PT24H)
  --ttl-days N         DynamoDB evidence TTL (default: stack LedgerTtlDays output)
  --seed N             deterministic workload seed (default: 42)
  --skip-build         reuse the existing local installDist
  -h, --help           show this help

The producer always runs on this machine. The EC2 instance only runs consumers.
USAGE
}

(($#)) || { usage >&2; exit 2; }
if [[ $1 == -h || $1 == --help ]]; then
  usage
  exit 0
fi
command_name=$1
shift

stack_name=reactive-sqs-soak
run_id=""
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-eu-central-1}}
profile=""
rate=2
duration=PT24H
ttl_days=""
seed=42
skip_build=false

while (($#)); do
  case "$1" in
    --stack) stack_name=${2:?missing value for --stack}; shift 2 ;;
    --run-id) run_id=${2:?missing value for --run-id}; shift 2 ;;
    --region) region=${2:?missing value for --region}; shift 2 ;;
    --profile) profile=${2:?missing value for --profile}; shift 2 ;;
    --rate) rate=${2:?missing value for --rate}; shift 2 ;;
    --duration) duration=${2:?missing value for --duration}; shift 2 ;;
    --ttl-days) ttl_days=${2:?missing value for --ttl-days}; shift 2 ;;
    --seed) seed=${2:?missing value for --seed}; shift 2 ;;
    --skip-build) skip_build=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

case "$command_name" in
  run|start|status|stop) ;;
  *) die "unknown command: $command_name" ;;
esac

validate_stack_name "$stack_name"
[[ $rate =~ ^[1-9][0-9]*$ ]] && ((rate <= 10)) ||
  die "--rate must be an integer between 1 and 10"
[[ $seed =~ ^-?[0-9]+$ ]] || die "--seed must be an integer"

if [[ -n $profile ]]; then
  export AWS_PROFILE=$profile
fi
export AWS_PAGER=""
export AWS_REGION=$region

if [[ -z $run_id ]]; then
  require_command aws
  stack_exists "$region" "$stack_name" || die "stack not found: $stack_name"
  run_id=$(stack_output "$region" "$stack_name" RunId)
fi
validate_run_id "$run_id"

state_dir="$REPO_ROOT/samples/reactive-sqs-soak/build/soak-state"
pid_file="$state_dir/${stack_name}-${run_id}.pid"
log_file="$state_dir/${stack_name}-${run_id}.producer.log"

read_live_pid() {
  local pid
  [[ -f $pid_file ]] || return 1
  pid=$(<"$pid_file")
  [[ $pid =~ ^[1-9][0-9]*$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  ps -p "$pid" -o command= 2>/dev/null | grep -Fq SoakProducerMain || return 1
  printf '%s\n' "$pid"
}

if [[ $command_name == status ]]; then
  if pid=$(read_live_pid); then
    printf 'Producer is running (PID %s). Log: %s\n' "$pid" "$log_file"
    [[ -f $log_file ]] && tail -n 20 "$log_file"
    exit 0
  fi
  printf 'Producer is not running.\n'
  [[ -f $log_file ]] && tail -n 20 "$log_file"
  exit 1
fi

if [[ $command_name == stop ]]; then
  pid=$(read_live_pid) || die "producer is not running"
  printf 'Stopping local producer PID %s\n' "$pid"
  kill -TERM "$pid"
  for _ in {1..30}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$pid_file"
      printf 'Producer stopped.\n'
      exit 0
    fi
    sleep 1
  done
  die "producer did not stop within 30 seconds; inspect PID $pid"
fi

if [[ -z $ttl_days ]]; then
  require_command aws
  stack_exists "$region" "$stack_name" || die "stack not found: $stack_name"
  ttl_days=$(stack_output "$region" "$stack_name" LedgerTtlDays)
fi
[[ $ttl_days =~ ^[1-9][0-9]*$ ]] && ((ttl_days >= 2 && ttl_days <= 365)) ||
  die "--ttl-days must be an integer between 2 and 365"

require_command aws
require_command java
require_command tee
stack_exists "$region" "$stack_name" || die "stack not found: $stack_name"
queue_url=$(stack_output "$region" "$stack_name" SourceQueueUrl)
table_name=$(stack_output "$region" "$stack_name" LedgerTableName)

if pid=$(read_live_pid); then
  die "producer is already running as PID $pid"
fi

if [[ $skip_build == false ]]; then
  "$REPO_ROOT/gradlew" -p "$REPO_ROOT" :reactive-sqs-soak:installDist
fi

classpath="$REPO_ROOT/samples/reactive-sqs-soak/build/install/reactive-sqs-soak/lib/*"
[[ -d ${classpath%/\*} ]] || die "installDist not found; run without --skip-build first"
producer_args=(
  "--region=$region"
  "--queue-url=$queue_url"
  "--table=$table_name"
  "--run-id=$run_id"
  "--messages-per-second=$rate"
  "--duration=$duration"
  "--ttl-days=$ttl_days"
  "--seed=$seed"
)

if [[ $command_name == run ]]; then
  mkdir -p "$state_dir"
  java -cp "$classpath" \
    io.github.marcschmidt1999.reactive.sqs.soak.SoakProducerMain "${producer_args[@]}" \
    > >(tee -a "$log_file") 2>&1 &
  producer_pid=$!
  printf '%s\n' "$producer_pid" > "$pid_file"
  forward_signal() {
    kill -TERM "$producer_pid" 2>/dev/null || true
  }
  trap forward_signal INT TERM HUP
  set +e
  wait "$producer_pid"
  exit_status=$?
  set -e
  trap - INT TERM HUP
  if [[ -f $pid_file && $(<"$pid_file") == "$producer_pid" ]]; then
    rm -f "$pid_file"
  fi
  exit "$exit_status"
fi

mkdir -p "$state_dir"
nohup java -cp "$classpath" \
  io.github.marcschmidt1999.reactive.sqs.soak.SoakProducerMain "${producer_args[@]}" \
  </dev/null >"$log_file" 2>&1 &
pid=$!
printf '%s\n' "$pid" > "$pid_file"
sleep 1
if ! read_live_pid >/dev/null; then
  tail -n 50 "$log_file" >&2 || true
  die "producer exited during startup"
fi
printf 'Producer started locally as PID %s. Log: %s\n' "$pid" "$log_file"
