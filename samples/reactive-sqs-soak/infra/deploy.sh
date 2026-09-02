#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

usage() {
  cat <<'USAGE'
Usage: deploy.sh [options]

Build the soak app first, then upload and deploy it to one small ARM64 instance.

Options:
  --artifact PATH          installDist archive (default: ../build/distributions/reactive-sqs-soak.tar.gz)
  --bucket NAME            private deployment bucket (default: account/region-derived bucket)
  --key KEY                artifact key (default: content-addressed key)
  --stack NAME             CloudFormation stack (default: reactive-sqs-soak)
  --run-id ID              ledger partition (default: UTC timestamp plus random suffix)
  --region REGION          AWS region (default: AWS_REGION/AWS_DEFAULT_REGION/eu-central-1)
  --subnet-id ID           public subnet (default: first public default subnet)
  --vpc-id ID              VPC to search (default: account default VPC)
  --profile PROFILE        AWS CLI profile
  --chaos-min-seconds N    minimum restart interval (default: 300)
  --chaos-jitter-seconds N restart jitter (default: 900)
  --ledger-ttl-days N      DynamoDB evidence TTL (default: 30)
  -h, --help               show this help
USAGE
}

artifact="$SCRIPT_DIR/../build/distributions/reactive-sqs-soak.tar.gz"
bucket=""
artifact_key=""
stack_name=reactive-sqs-soak
run_id=""
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-eu-central-1}}
subnet_id=""
vpc_id=""
profile=""
chaos_min_seconds=300
chaos_jitter_seconds=900
ledger_ttl_days=30

while (($#)); do
  case "$1" in
    --artifact) artifact=${2:?missing value for --artifact}; shift 2 ;;
    --bucket) bucket=${2:?missing value for --bucket}; shift 2 ;;
    --key) artifact_key=${2:?missing value for --key}; shift 2 ;;
    --stack) stack_name=${2:?missing value for --stack}; shift 2 ;;
    --run-id) run_id=${2:?missing value for --run-id}; shift 2 ;;
    --region) region=${2:?missing value for --region}; shift 2 ;;
    --subnet-id) subnet_id=${2:?missing value for --subnet-id}; shift 2 ;;
    --vpc-id) vpc_id=${2:?missing value for --vpc-id}; shift 2 ;;
    --profile) profile=${2:?missing value for --profile}; shift 2 ;;
    --chaos-min-seconds) chaos_min_seconds=${2:?missing value for --chaos-min-seconds}; shift 2 ;;
    --chaos-jitter-seconds) chaos_jitter_seconds=${2:?missing value for --chaos-jitter-seconds}; shift 2 ;;
    --ledger-ttl-days) ledger_ttl_days=${2:?missing value for --ledger-ttl-days}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

require_command aws
require_command jq
require_command tar
validate_stack_name "$stack_name"

if [[ -n $profile ]]; then
  export AWS_PROFILE=$profile
fi
export AWS_PAGER=""

[[ -f $artifact ]] || die "artifact not found: $artifact"
[[ $chaos_min_seconds =~ ^[0-9]+$ ]] || die "--chaos-min-seconds must be an integer"
[[ $chaos_jitter_seconds =~ ^[0-9]+$ ]] || die "--chaos-jitter-seconds must be an integer"
[[ $ledger_ttl_days =~ ^[1-9][0-9]*$ ]] &&
  ((ledger_ttl_days >= 3 && ledger_ttl_days <= 365)) ||
  die "--ledger-ttl-days must be an integer between 3 and 365"

archive_listing=$(mktemp)
trap 'rm -f "$archive_listing"' EXIT
tar -tzf "$artifact" > "$archive_listing"
archive_root_count=$(awk -F/ 'NF {print $1}' "$archive_listing" | sort -u | wc -l | tr -d ' ')
[[ $archive_root_count == 1 ]] || die "artifact must contain exactly one installDist root directory"
grep -Eq '^[^/]+/bin/reactive-sqs-soak$' "$archive_listing" ||
  die "artifact does not contain bin/reactive-sqs-soak"

if command -v sha256sum >/dev/null 2>&1; then
  artifact_sha=$(sha256sum "$artifact" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
  artifact_sha=$(shasum -a 256 "$artifact" | awk '{print $1}')
else
  die "sha256sum or shasum is required"
fi

if stack_exists "$region" "$stack_name"; then
  existing_run_id=$(stack_output "$region" "$stack_name" RunId)
  if [[ -z $run_id ]]; then
    run_id=$existing_run_id
  elif [[ $run_id != "$existing_run_id" ]]; then
    die "stack $stack_name belongs to run $existing_run_id; use a new stack name for run $run_id"
  fi
elif [[ -z $run_id ]]; then
  run_id="soak-$(date -u +%Y%m%dT%H%M%SZ)-${artifact_sha:0:8}"
fi
validate_run_id "$run_id"

account_id=$(aws_region "$region" sts get-caller-identity --query Account --output text)
[[ $account_id =~ ^[0-9]{12}$ ]] || die "could not determine the AWS account ID"

if [[ -z $bucket ]]; then
  bucket="reactive-sqs-soak-${account_id}-${region}"
fi
if [[ -z $artifact_key ]]; then
  artifact_key="artifacts/${artifact_sha}/reactive-sqs-soak.tar.gz"
fi

if ! aws_region "$region" s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
  printf 'Creating private artifact bucket %s in %s\n' "$bucket" "$region"
  if [[ $region == us-east-1 ]]; then
    aws_region "$region" s3api create-bucket --bucket "$bucket" >/dev/null
  else
    aws_region "$region" s3api create-bucket \
      --bucket "$bucket" \
      --create-bucket-configuration "LocationConstraint=$region" >/dev/null
  fi
fi

aws_region "$region" s3api put-public-access-block \
  --bucket "$bucket" \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws_region "$region" s3api put-bucket-encryption \
  --bucket "$bucket" \
  --server-side-encryption-configuration \
    'Rules=[{ApplyServerSideEncryptionByDefault={SSEAlgorithm=AES256}}]'
aws_region "$region" s3api put-bucket-versioning \
  --bucket "$bucket" \
  --versioning-configuration Status=Enabled
artifact_size=$(wc -c < "$artifact" | tr -d ' ')
if remote_size=$(aws_region "$region" s3api head-object \
  --bucket "$bucket" --key "$artifact_key" \
  --query ContentLength --output text 2>/dev/null); then
  [[ $remote_size == "$artifact_size" ]] ||
    die "existing artifact has size $remote_size, expected $artifact_size: s3://${bucket}/${artifact_key}"
  printf 'Reusing existing content-addressed artifact %s\n' "s3://${bucket}/${artifact_key}"
else
  printf 'Uploading %s\n' "s3://${bucket}/${artifact_key}"
  aws_region "$region" s3api put-object \
    --bucket "$bucket" \
    --key "$artifact_key" \
    --body "$artifact" \
    --server-side-encryption AES256 >/dev/null
fi

if [[ -n $subnet_id ]]; then
  discovered_vpc=$(aws_region "$region" ec2 describe-subnets \
    --subnet-ids "$subnet_id" \
    --query 'Subnets[0].VpcId' --output text)
  [[ $discovered_vpc != None ]] || die "subnet not found: $subnet_id"
  if [[ -n $vpc_id && $vpc_id != "$discovered_vpc" ]]; then
    die "subnet $subnet_id belongs to $discovered_vpc, not $vpc_id"
  fi
  vpc_id=$discovered_vpc
else
  if [[ -z $vpc_id ]]; then
    vpc_id=$(aws_region "$region" ec2 describe-vpcs \
      --filters Name=is-default,Values=true \
      --query 'Vpcs[0].VpcId' --output text)
    [[ $vpc_id != None ]] ||
      die "no default VPC in $region; pass --subnet-id and optionally --vpc-id"
  fi
  subnet_id=$(aws_region "$region" ec2 describe-subnets \
    --filters \
      "Name=vpc-id,Values=$vpc_id" \
      Name=default-for-az,Values=true \
      Name=map-public-ip-on-launch,Values=true \
      Name=state,Values=available \
    --query 'sort_by(Subnets, &AvailabilityZone)[0].SubnetId' --output text)
  [[ $subnet_id != None ]] ||
    die "no available public default subnet found in VPC $vpc_id"
fi

route_table_id=$(aws_region "$region" ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=$subnet_id" \
  --query 'RouteTables[0].RouteTableId' --output text)
if [[ $route_table_id == None ]]; then
  route_table_id=$(aws_region "$region" ec2 describe-route-tables \
    --filters "Name=vpc-id,Values=$vpc_id" Name=association.main,Values=true \
    --query 'RouteTables[0].RouteTableId' --output text)
fi
[[ $route_table_id != None ]] || die "could not find a route table for subnet $subnet_id"
default_gateway=$(aws_region "$region" ec2 describe-route-tables \
  --route-table-ids "$route_table_id" \
  --query "RouteTables[0].Routes[?DestinationCidrBlock=='0.0.0.0/0'].GatewayId | [0]" \
  --output text)
[[ $default_gateway == igw-* ]] ||
  die "subnet $subnet_id does not have a public IPv4 default route through an Internet gateway"

printf 'Deploying stack %s (run %s) in %s\n' "$stack_name" "$run_id" "$region"
aws_region "$region" cloudformation deploy \
  --template-file "$SCRIPT_DIR/template.yaml" \
  --stack-name "$stack_name" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset \
  --tags Purpose=reactive-sqs-soak RunId="$run_id" \
  --parameter-overrides \
    RunId="$run_id" \
    VpcId="$vpc_id" \
    SubnetId="$subnet_id" \
    ArtifactBucket="$bucket" \
    ArtifactKey="$artifact_key" \
    ArtifactSha256="$artifact_sha" \
    ChaosMinimumSeconds="$chaos_min_seconds" \
    ChaosJitterSeconds="$chaos_jitter_seconds" \
    LedgerTtlDays="$ledger_ttl_days"

source_queue_url=$(stack_output "$region" "$stack_name" SourceQueueUrl)
table_name=$(stack_output "$region" "$stack_name" LedgerTableName)

cat <<SUMMARY

Deployment submitted successfully.
  Stack:        $stack_name
  Run ID:       $run_id
  Source queue: $source_queue_url
  Ledger table: $table_name

Wait for cloud-init and inspect health with:
  $SCRIPT_DIR/status.sh --stack $stack_name --region $region

Run the producer locally with the same stack and run ID. See README.md.
SUMMARY
