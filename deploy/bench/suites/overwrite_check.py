#!/usr/bin/env python3
"""Overwrite behind the gateway, then GET again: the metadata cache must
self-heal instead of failing the client.

`pgvs3 seed` writes straight to the database, which is exactly how a stale
cache entry appears — an overwrite reaps the old rows in the same transaction,
so a stale entry shows up as missing rows (500) or as a range clamped against
the old size (416). curl does not retry, so a stale cache fails this check
the way a client sees it. Its own bucket: the stress suite samples `lake`.
"""
import os
import subprocess
import sys

URL = os.environ.get("PGVS3_URL", "http://pgvs3:8014")
PG = os.environ.get("PG_URL", "postgresql://postgres:postgres@postgres:5432/pgvs3")
BUCKET = "smoke"
KEY = "obj-00000.bin"


def seed(object_mib):
    env = {**os.environ, "PGVS3_POOL_MIN": "2", "PGVS3_POOL_MAX": "8"}
    subprocess.run(
        ["pgvs3", "--url", PG, "seed", "--bucket", BUCKET,
         "--gigabytes", str(object_mib * 4 / 1024), "--object-mib", str(object_mib),
         "--tasks", "2"],
        env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def get_len(byte_range):
    out = subprocess.run(
        ["curl", "-fsS", "--aws-sigv4", "aws:amz:us-east-1:s3",
         "--user", f"{os.environ.get('AWS_ACCESS_KEY_ID', 'cachebench')}:"
                   f"{os.environ.get('AWS_SECRET_ACCESS_KEY', 'cachebench-local-only')}",
         "-H", f"Range: bytes={byte_range}", f"{URL}/{BUCKET}/{KEY}"],
        check=True, stdout=subprocess.PIPE,
    )
    return len(out.stdout)


CROSS_ROW = "8000-17999"  # crosses the 8120-byte row boundary: want 10000
PAST_OLD_EOF = "10485760-10486759"  # 10 MiB in: past the old size, want 1000

results = {}
seed(8)  # 8 MiB objects
results["read"] = get_len(CROSS_ROW)
seed(8)  # same-size overwrite behind the running gateway
results["same-size overwrite"] = get_len(CROSS_ROW)
seed(64)  # grown behind the gateway: a range past the old EOF must serve
results["grow, range past old EOF"] = get_len(PAST_OLD_EOF)

print("overwrite self-heal: "
      + ", ".join(f"{k}={v}" for k, v in results.items()))
sys.exit(0 if results == {"read": 10000, "same-size overwrite": 10000,
                          "grow, range past old EOF": 1000} else 1)
