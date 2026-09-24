#!/usr/bin/env bash
# Tear down the pgvs3-perf test rig in AWS account 356071200662 (platform-dev).
# Everything is tagged project=pgvs3-perf, owner=adon.metcalfe, expires=2026-09-26.
set -euo pipefail
P=${AWS_PROFILE:-platform-dev}

# Every instance in the cluster: the writer's name changed when it moved AZs.
for db in $(aws rds describe-db-clusters --profile "$P" --db-cluster-identifier pgvs3-perf \
              --query 'DBClusters[0].DBClusterMembers[].DBInstanceIdentifier' --output text); do
  aws rds delete-db-instance --profile "$P" --db-instance-identifier "$db" \
    --skip-final-snapshot --delete-automated-backups
done
aws rds delete-db-cluster --profile "$P" --db-cluster-identifier pgvs3-perf --skip-final-snapshot
aws ec2 terminate-instances --profile "$P" --instance-ids i-037e91b323d000a1b
aws ec2 delete-key-pair --profile "$P" --key-name pgvs3-perf-key

echo "deleted. confirm with:"
echo "  aws rds describe-db-clusters --profile $P --db-cluster-identifier pgvs3-perf"
echo "  aws ec2 describe-instances --profile $P --instance-ids i-037e91b323d000a1b"
