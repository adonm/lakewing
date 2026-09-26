#!/usr/bin/env bash
# An EC2 kind cluster backed by Aurora Serverless v2. The only difference from
# local/CI kind is the PostgreSQL URL supplied via a Kubernetes Secret.
set -euo pipefail
export AWS_PAGER='' AWS_CLI_AUTO_PROMPT=off

cd "$(dirname "$0")/../.."
profile=${RIG_AWS_PROFILE:-}
region=${RIG_AWS_REGION:-}
stack=pgvs3-kind-rig
key_name=$stack
key_file=${RIG_SSH_KEY:-.tmp/pgvs3/rig-key.pem}
known_hosts=.tmp/pgvs3/rig-known-hosts
ssh_user=${RIG_SSH_USER:-ec2-user}
aws=(aws --profile "$profile" --region "$region")

die() { echo "rig: $*" >&2; exit 1; }
need() { [ -n "${!1:-}" ] || die "set $1 in .env (see .env.example)"; }

check_account() {
  need RIG_AWS_PROFILE
  need RIG_AWS_REGION
  need RIG_AWS_ACCOUNT
  local account
  account=$("${aws[@]}" sts get-caller-identity --query Account --output text)
  [ "$account" = "$RIG_AWS_ACCOUNT" ] || die "profile $profile is account $account, expected $RIG_AWS_ACCOUNT"
}

by_tag() { # ec2 noun, tag, JMESPath array
  local value
  value=$("${aws[@]}" ec2 "$1" --filters "Name=tag:Name,Values=$2" --query "$3" --output text)
  [[ "$value" =~ ^(vpc|subnet)-[0-9a-f]+$ ]] || die "expected one resource named $2; found: $value"
  echo "$value"
}

output() { # stack output by key; never a secret
  local value
  value=$("${aws[@]}" cloudformation describe-stacks --stack-name "$stack" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue | [0]" --output text)
  [ -n "$value" ] && [ "$value" != None ] || die "$stack has no $1 output"
  echo "$value"
}

ssh_run() {
  local ip
  ip=$(output PrivateIp)
  ssh -i "$key_file" -o UserKnownHostsFile="$known_hosts" \
    -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=5 \
    "$ssh_user@$ip" "$@"
}

copy_results() {
  mkdir -p .tmp/pgvs3/rig-out
  local ip
  ip=$(output PrivateIp)
  scp -i "$key_file" -o UserKnownHostsFile="$known_hosts" \
    -o StrictHostKeyChecking=accept-new \
    "$ssh_user@$ip:pgvs3/.tmp/pgvs3/kind-bench.jsonl" \
    ".tmp/pgvs3/rig-out/kind-bench-$(date -u +%Y%m%dT%H%M%SZ).jsonl"
}

case ${1:-} in
  up)
    check_account
    need RIG_VPC_TAG
    need RIG_DB_SUBNET_A_TAG
    need RIG_DB_SUBNET_B_TAG
    need RIG_SSH_SG
    need RIG_DB_CLIENT_SG
    need RIG_AURORA_SG
    mkdir -p .tmp/pgvs3
    vpc=$(by_tag describe-vpcs "$RIG_VPC_TAG" 'Vpcs[].VpcId')
    private_a=$(by_tag describe-subnets "$RIG_DB_SUBNET_A_TAG" 'Subnets[].SubnetId')
    private_b=$(by_tag describe-subnets "$RIG_DB_SUBNET_B_TAG" 'Subnets[].SubnetId')
    for subnet in "$private_a" "$private_b"; do
      actual=$("${aws[@]}" ec2 describe-subnets --subnet-ids "$subnet" --query 'Subnets[0].VpcId' --output text)
      [ "$actual" = "$vpc" ] || die "$subnet is not in $vpc"
    done
    az=$("${aws[@]}" ec2 describe-subnets --subnet-ids "$private_a" --query 'Subnets[0].AvailabilityZone' --output text)
    other_az=$("${aws[@]}" ec2 describe-subnets --subnet-ids "$private_b" --query 'Subnets[0].AvailabilityZone' --output text)
    [ "$az" != "$other_az" ] || die "Aurora needs private subnets in two AZs"
    # Use only the approved groups named in the local .env. Do not create or
    # change SG rules; verify private-network SSH and the database path.
    for sg in "$RIG_SSH_SG" "$RIG_DB_CLIENT_SG" "$RIG_AURORA_SG"; do
      actual=$("${aws[@]}" ec2 describe-security-groups --group-ids "$sg" --query 'SecurityGroups[0].VpcId' --output text)
      [ "$actual" = "$vpc" ] || die "$sg is not in $vpc"
    done
    cidr=$("${aws[@]}" ec2 describe-vpcs --vpc-ids "$vpc" --query 'Vpcs[0].CidrBlock' --output text)
    route=$(ip route get "${cidr%.*}.1")
    route_source=$(sed -n 's/.* src \([^ ]*\).*/\1/p' <<< "$route")
    ssh_cidrs=$("${aws[@]}" ec2 describe-security-groups --group-ids "$RIG_SSH_SG" \
      --query 'SecurityGroups[0].IpPermissions[?FromPort==`22` && ToPort==`22`].IpRanges[].CidrIp' --output text)
    python3 -c 'import ipaddress,sys; s=ipaddress.ip_address(sys.argv[1]); sys.exit(0 if any(s in ipaddress.ip_network(c) for c in sys.argv[2].split()) else 1)' \
      "$route_source" "$ssh_cidrs" || die "private-network source $route_source is not permitted to SSH by $RIG_SSH_SG"
    db_sources=$("${aws[@]}" ec2 describe-security-groups --group-ids "$RIG_AURORA_SG" \
      --query 'SecurityGroups[0].IpPermissions[?FromPort==`5432` && ToPort==`5432`].UserIdGroupPairs[].GroupId' --output text)
    grep -Fxq "$RIG_DB_CLIENT_SG" < <(printf '%s\n' "$db_sources" | tr '\t ' '\n\n') \
      || die "$RIG_AURORA_SG does not allow port 5432 from $RIG_DB_CLIENT_SG"
    db_ranges=$("${aws[@]}" ec2 describe-security-groups --group-ids "$RIG_AURORA_SG" \
      --query 'SecurityGroups[0].IpPermissions[?FromPort==`5432` && ToPort==`5432`].IpRanges[].CidrIp' --output text)
    if grep -Fxq '0.0.0.0/0' < <(printf '%s\n' "$db_ranges" | tr '\t ' '\n\n'); then
      die "$RIG_AURORA_SG exposes port 5432 publicly"
    fi
    status=$("${aws[@]}" cloudformation describe-stacks --stack-name "$stack" \
      --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true)
    if [ "$status" = ROLLBACK_COMPLETE ]; then
      project=$("${aws[@]}" cloudformation describe-stacks --stack-name "$stack" \
        --query 'Stacks[0].Tags[?Key==`Project`].Value | [0]' --output text)
      [ "$project" = pgvs3 ] || die "refusing to replace untagged stack $stack"
      "${aws[@]}" cloudformation delete-stack --stack-name "$stack"
      "${aws[@]}" cloudformation wait stack-delete-complete --stack-name "$stack"
    fi
    ami=$("${aws[@]}" ssm get-parameter \
      --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
      --query Parameter.Value --output text)
    [ -s "$key_file" ] || {
      if "${aws[@]}" ec2 describe-key-pairs --key-names "$key_name" >/dev/null 2>&1; then
        die "AWS key pair $key_name exists, but $key_file is missing; will not replace it"
      fi
      umask 077
      "${aws[@]}" ec2 create-key-pair --key-name "$key_name" --key-type ed25519 \
        --tag-specifications "ResourceType=key-pair,Tags=[{Key=Project,Value=pgvs3}]" \
        --query KeyMaterial --output text > "$key_file"
      chmod 600 "$key_file"
    }
    echo "rig: $profile/$region private $az — m7i.4xlarge kind + Aurora Serverless v2 I/O-Optimized (2–16 ACU)"
    "${aws[@]}" cloudformation deploy --stack-name "$stack" \
      --template-file deploy/kind/rig.yaml --no-fail-on-empty-changeset \
      --tags Project=pgvs3 "Environment=${RIG_ENVIRONMENT:-benchmark}" \
      --parameter-overrides "DbSubnetA=$private_a" "DbSubnetB=$private_b" \
        "SshSecurityGroupId=$RIG_SSH_SG" \
        "DbClientSecurityGroupId=$RIG_DB_CLIENT_SG" \
        "ClusterSecurityGroupId=$RIG_AURORA_SG" "AmiId=$ami" \
        "KeyName=$key_name" \
        "InstanceType=${RIG_INSTANCE_TYPE:-m7i.4xlarge}" \
        "RootVolumeGiB=${RIG_ROOT_GIB:-250}"
    instance=$(output InstanceId)
    # CFN's EC2 Instance block-device mapping exposes Iops but not the gp3
    # Throughput field. Raise it after launch so local disk is not the cap
    # when evaluating Aurora reads through kind.
    volume=$("${aws[@]}" ec2 describe-instances --instance-ids "$instance" \
      --query 'Reservations[0].Instances[0].BlockDeviceMappings[?DeviceName==`/dev/xvda`].Ebs.VolumeId | [0]' --output text)
    [ -n "$volume" ] && [ "$volume" != None ] || die "cannot find the rig's root volume"
    throughput=$("${aws[@]}" ec2 describe-volumes --volume-ids "$volume" \
      --query 'Volumes[0].Throughput' --output text)
    if [ "$throughput" != 500 ]; then
      "${aws[@]}" ec2 modify-volume --volume-id "$volume" --throughput 500 \
        --query 'VolumeModification.[VolumeId,TargetThroughput,ModificationState]' --output text
    fi
    "${aws[@]}" ec2 wait instance-status-ok --instance-ids "$instance"
    echo 'rig: waiting for cloud-init (mise installer)'
    ready=0
    for _ in $(seq 1 40); do
      if ssh_run 'test -f /var/tmp/pgvs3-bootstrap-ready && test -x ~/.local/bin/mise' 2>/dev/null; then
        ready=1; break
      fi
      sleep 10
    done
    [ "$ready" = 1 ] || die "EC2 bootstrap did not finish; inspect /var/log/cloud-init-output.log via SSH"
    ;&
  sync)
    check_account
    [ -s "$key_file" ] || die "missing SSH key: $key_file"
    tar czf - Cargo.toml Cargo.lock .cargo crates deploy Dockerfile .dockerignore \
      mise.toml mise.ec2.toml hk.pkl justfile README.md \
      | ssh_run 'mkdir -p pgvs3 && tar xzf - -C pgvs3'
    ssh_run 'cd pgvs3 && ~/.local/bin/mise trust mise.toml && ~/.local/bin/mise trust mise.ec2.toml && ~/.local/bin/mise -E ec2 bootstrap --yes'
    ssh_run 'docker info >/dev/null && echo "rig: Docker ready after mise bootstrap"'
    ssh_run "cd pgvs3 && ~/.local/bin/mise exec -- bash -c 'kind get clusters | grep -qx pgvs3 || kind create cluster --name pgvs3 --config deploy/kind/cluster.yaml'"
    ssh_run 'cd pgvs3 && ~/.local/bin/mise exec -- kubectl --context kind-pgvs3 create namespace pgvs3 --dry-run=client -o yaml | ~/.local/bin/mise exec -- kubectl --context kind-pgvs3 apply -f -'
    cluster=$(output DbClusterIdentifier)
    endpoint=$(output DbEndpoint)
    secret_arn=$("${aws[@]}" rds describe-db-clusters --db-cluster-identifier "$cluster" \
      --query 'DBClusters[0].MasterUserSecret.SecretArn' --output text)
    [ -n "$secret_arn" ] && [ "$secret_arn" != None ] || die "Aurora managed master secret not available"
    "${aws[@]}" secretsmanager get-secret-value --secret-id "$secret_arn" \
      --query SecretString --output text \
      | python3 deploy/kind/rig-secret.py "$endpoint" \
      | ssh_run 'cd pgvs3 && ~/.local/bin/mise exec -- kubectl --context kind-pgvs3 apply -f -'
    searchers=${QUICKWIT_SEARCHERS:-0}
    [[ "$searchers" =~ ^[0-9]+$ ]] || die 'QUICKWIT_SEARCHERS must be a non-negative integer'
    ssh_run "cd pgvs3 && PGVS3_DB_SECRET=pgvs3-aurora QUICKWIT_SEARCHERS=$searchers ~/.local/bin/mise exec -- just kind-up"
    ssh_run 'cd pgvs3 && PGVS3_DB_SECRET=pgvs3-aurora ~/.local/bin/mise exec -- just kind-validate'
    instance=$(output InstanceId)
    echo "rig: ready on $instance (kind on EC2, Aurora $endpoint)"
    ;;
  validate)
    check_account
    searchers=${QUICKWIT_SEARCHERS:-0}
    [[ "$searchers" =~ ^[0-9]+$ ]] || die 'QUICKWIT_SEARCHERS must be a non-negative integer'
    ssh_run "cd pgvs3 && PGVS3_DB_SECRET=pgvs3-aurora QUICKWIT_SEARCHERS=$searchers ~/.local/bin/mise exec -- just smoke"
    copy_results
    ;;
  contract)
    check_account
    ssh_run 'cd pgvs3 && PGVS3_DB_SECRET=pgvs3-aurora ~/.local/bin/mise exec -- just kind-contract'
    ;;
  churn)
    check_account
    remote='cd pgvs3 && PGVS3_DB_SECRET=pgvs3-aurora'
    for k in PGVS3_CHURN_ROUNDS PGVS3_CHURN_MIB; do
      if [ -n "${!k:-}" ]; then
        [[ "${!k}" =~ ^[0-9]+$ ]] || die "$k must be a positive integer"
        remote+=" $k=${!k}"
      fi
    done
    mkdir -p .tmp/pgvs3/rig-out
    ssh_run "$remote ~/.local/bin/mise exec -- just kind-churn" \
      | tee ".tmp/pgvs3/rig-out/churn-$(date -u +%Y%m%dT%H%M%SZ).log"
    ;;
  bench)
    check_account
    # Export only the same suite knobs kind-bench accepts; no database secret
    # or AWS credential is sent in the command line.
    remote='cd pgvs3 && PGVS3_DB_SECRET=pgvs3-aurora'
    for k in SUITES QUICK SCALE CLIENTS SECONDS_RUN SF PASSES PARTS QUERIES DOCS WORKERS WINDOW_FRAC SEARCH_INDEX SEED_GB REQUESTS CONCURRENCY SIZES SPATIAL_SF SPATIAL_QUERIES SPATIAL_QUERY_TIMEOUT DUCKDB_MEMORY_LIMIT; do
      if [ -n "${!k:-}" ]; then remote+=" $k=$(printf '%q' "${!k}")"; fi
    done
    ssh_run "$remote ~/.local/bin/mise exec -- just kind-bench"
    copy_results
    ;;
  status)
    check_account
    "${aws[@]}" cloudformation describe-stacks --stack-name "$stack" \
      --query 'Stacks[0].[StackStatus,Outputs[].[OutputKey,OutputValue]]' --output json
    ;;
  results)
    check_account
    copy_results
    ;;
  reset)
    check_account
    [ -s "$key_file" ] || die "missing SSH key: $key_file"
    tar czf - deploy/kind/reset.sh deploy/kind/db.sh deploy/kind/db-job.yaml \
      | ssh_run 'cd pgvs3 && tar xzf -'
    ssh_run 'cd pgvs3 && PGVS3_DB_SECRET=pgvs3-aurora ~/.local/bin/mise exec -- bash deploy/kind/reset.sh'
    ;;
  ssh)
    check_account
    ssh_run
    ;;
  teardown)
    check_account
    project=$("${aws[@]}" cloudformation describe-stacks --stack-name "$stack" \
      --query 'Stacks[0].Tags[?Key==`Project`].Value | [0]' --output text)
    [ "$project" = pgvs3 ] || die "refusing to delete untagged stack $stack"
    "${aws[@]}" cloudformation delete-stack --stack-name "$stack"
    "${aws[@]}" cloudformation wait stack-delete-complete --stack-name "$stack"
    "${aws[@]}" ec2 delete-key-pair --key-name "$key_name"
    rm -f "$key_file" "$known_hosts"
    echo "rig: deleted $stack and its key pair"
    ;;
  *) die 'usage: rig.sh up|sync|contract|churn|validate|bench|results|status|ssh|reset|teardown' ;;
esac
