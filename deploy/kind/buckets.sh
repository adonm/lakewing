#!/usr/bin/env bash
set -euo pipefail

endpoint=${1:?pass the gateway endpoint}
shift
for bucket in "$@"; do
  curl -fsS --max-time 15 --retry 3 --retry-delay 1 \
    --aws-sigv4 'aws:amz:us-east-1:s3' \
    --user "${PGVS3_ACCESS_KEY:-cachebench}:${PGVS3_SECRET_KEY:-cachebench-local-only}" \
    -X PUT -o /dev/null "$endpoint/$bucket"
  echo "$bucket ready"
done
