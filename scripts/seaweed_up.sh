#!/usr/bin/env bash
# Local SeaweedFS lake rig: S3-compatible store for direct writes.
# Idempotent: safe to re-run. Rig state lives under /tmp/opencode.
# Readers mount the bucket via ./scripts/lake_mount.sh (mount-s3); writers
# talk S3 directly. Replaces the old MinIO + Cachey HTTP rig.
set -euo pipefail

RIG=/tmp/opencode
SEAWEED_IMAGE="${SEAWEED_IMAGE:-chrislusf/seaweedfs:4.13}"
S3_PORT="${S3_PORT:-8333}"
# shellcheck disable=SC1091
source "$(dirname "$0")/dev-s3.env"

# S3 identity config: SeaweedFS rejects unknown access keys, so provision
# the same dev-only pair the rig, rclone and mount-s3 use.
mkdir -p "$RIG/seaweed-config"
cat > "$RIG/seaweed-config/s3.json" <<EOF
{"identities": [{"name": "dev", "actions": ["Admin"], "credentials": [{"accessKey": "$S3_USER", "secretKey": "$S3_PASS"}]}]}
EOF

want="image=$SEAWEED_IMAGE config=$(md5sum "$RIG/seaweed-config/s3.json" | cut -d' ' -f1)"
if docker ps --format '{{.Names}}' | grep -qx lake-seaweed; then
  prev="$(cat "$RIG/seaweed-want" 2>/dev/null || true)"
  [ "$prev" = "$want" ] || docker rm -f lake-seaweed >/dev/null
fi
if ! docker ps --format '{{.Names}}' | grep -qx lake-seaweed; then
  mkdir -p "$RIG/seaweed"
  docker rm -f lake-seaweed >/dev/null 2>&1 || true
  docker run -d --name lake-seaweed \
    -p "127.0.0.1:$S3_PORT:8333" -p "127.0.0.1:9333:9333" -p "127.0.0.1:19333:19333" \
    -p "127.0.0.1:8888:8888" -p "127.0.0.1:18888:18888" \
    -v "$RIG/seaweed:/data" \
    -v "$RIG/seaweed-config/s3.json:/etc/seaweed/s3.json:ro" \
    "$SEAWEED_IMAGE" server -dir /data -s3 -s3.port=8333 -s3.config=/etc/seaweed/s3.json >/dev/null
  printf '%s' "$want" > "$RIG/seaweed-want"
  for _ in $(seq 1 30); do
    curl -sf "http://127.0.0.1:$S3_PORT/" >/dev/null 2>&1 && break
    sleep 1
  done
fi

export AWS_ACCESS_KEY_ID="$S3_USER" AWS_SECRET_ACCESS_KEY="$S3_PASS" AWS_EC2_METADATA_DISABLED=true
# Ensure the lake bucket exists (additive publishes never create it).
if ! aws --endpoint-url "http://127.0.0.1:$S3_PORT" s3 ls "s3://lake" >/dev/null 2>&1; then
  aws --endpoint-url "http://127.0.0.1:$S3_PORT" s3 mb s3://lake >/dev/null
fi
echo "seaweed s3 at http://127.0.0.1:$S3_PORT (bucket: lake)"
