#!/usr/bin/env python3
"""Run the bounded sample gate on the existing kind-lake-cache NVMe rig."""

import argparse
import datetime
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cachebench.rig import Rig, command

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "lakewing-lancebench:dev"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", default="lake-cache")
    parser.add_argument("--directory", default=str(ROOT / ".tmp/cache-bench"))
    parser.add_argument("--snapshot", type=int, default=6)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--external-file-cache", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--queries", default="ID,CITY,BROAD,FULL,CURSOR,DEEP")
    parser.add_argument("--backends", default="ducklake,wkb,geo,wkb_ids,geo_ids")
    args = parser.parse_args()
    rig = Rig(args)
    name = "run-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = rig.root / "reader/lancebench" / name
    root.mkdir(parents=True)
    backing = json.loads(command("findmnt", "-J", "-T", str(root), "-o", "TARGET,SOURCE,FSTYPE", capture=True))["filesystems"][0]
    if backing["fstype"] in ("tmpfs", "ramfs", "overlay"):
        raise RuntimeError(f"expected disk backing: {backing}")
    catalog = ROOT / "fixtures/nw-europe.ducklake"
    selection = "true" if args.full else "hash(id)%128=0 OR (xmax>=4.85 AND xmin<=4.95 AND ymax>=52.3 AND ymin<=52.4)"
    sql = f"""LOAD spatial; LOAD ducklake; SET threads=4; SET memory_limit='4GB';
SET temp_directory='{root}/prepare-spill';
ATTACH 'ducklake:{catalog}' AS lake (READ_ONLY, DATA_PATH '{ROOT}/fixtures/nw-europe.files/', OVERRIDE_DATA_PATH true);
COPY (SELECT id,layer,source_id,ST_AsWKB(geom) AS geom,properties::VARCHAR AS properties,
sortkey,xmin,ymin,xmax,ymax,cx,cy,name FROM lake.features AT (VERSION => {args.snapshot})
WHERE {selection}
ORDER BY sortkey,id) TO '{root}/source.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 65536);
"""
    (root / "prepare.sql").write_text(sql)
    command(str(ROOT / ".deps/duckdb/duckdb"), "-batch", "-bail", ":memory:", "-c", sql)
    provenance = {"catalog_sha256": hashlib.sha256(catalog.read_bytes()).hexdigest(),
                  "snapshot": args.snapshot, "backing": backing,
                  "source_sha256": hashlib.sha256((root / "source.parquet").read_bytes()).hexdigest(),
                  "source_engine": command(str(ROOT / ".deps/duckdb/duckdb"), "--version", capture=True),
                  "selection": selection, "order": "sortkey,id",
                  "pod_cpu": "4", "pod_memory": "6Gi", "cache_bytes": 32 * 1024**2,
                  "external_file_cache": args.external_file_cache,
                  "s3_delay": json.loads(rig.kube("-n", "lake-bench", "get", "pod", "s3-delay", "-o", "json", capture=True))["spec"]["containers"][0]["env"],
                  "notes": "SDK gate; query surfaces, not production Go HTTP endpoints; no OS cache eviction"}
    (root / "provenance.json").write_text(json.dumps(provenance, indent=2))
    command("docker", "build", "-f", str(ROOT / "scripts/lancebench/Dockerfile"), "-t", IMAGE, str(ROOT))
    command("kind", "load", "docker-image", IMAGE, "--name", args.cluster)
    volume = [{"name": "work", "hostPath": {"path": f"/nvme/lancebench/{name}"}}]
    mounts = [{"name": "work", "mountPath": "/work"}]
    for phase in ("local", "s3"):
        pod_name = "lance-" + phase
        argv = ["--root", "/work", "--label", phase, "--repeats", str(args.repeats),
                "--queries", args.queries, "--backends", args.backends,
                "--otlp", "http://lgtm.monitoring.svc.cluster.local:4318/v1/traces"]
        if not args.external_file_cache:
            argv.append("--no-external-file-cache")
        if phase == "local":
            argv += ["--build", "--source", "/work/source.parquet"]
        else:
            seed = rig.pod("lance-seed", {"name": "seed", "image": "lakewing-cachebench:dev",
                "command": ["sh", "-ec", "\n".join([
                    f"rclone copy /work/{d} lake:lake/lancebench/{name}/{d}" for d in ("parquet.files", "wkb.lance", "geo.lance")
                ] + [f"rclone copyto /work/parquet.ducklake lake:lake/lancebench/{name}/parquet.ducklake"])],
                "env": rig.rclone_env("http://seaweed:8333"), "envFrom": [{"secretRef": {"name": "s3"}}],
                "volumeMounts": mounts}, volume)
            rig.kube("-n", "lake-bench", "delete", "pod", "lance-seed", "--ignore-not-found", "--wait=true")
            rig.apply(seed)
            rig.wait_finished("lance-seed")
            proxy = rig.pod("lance-s3cache", {"name": "s3cache", "image": "lakewing-s3cache:dev",
                "env": [{"name": "UPSTREAM", "value": "http://httpcache-s3.lake-bench.svc.cluster.local:8080"},
                        {"name": "LISTEN", "value": ":8080"}, {"name": "CACHE_DIR", "value": "/cache"},
                        {"name": "CACHE_BYTES", "value": str(32 * 1024**2)},
                        {"name": "SLICE_BYTES", "value": str(1024**2)},
                        {"name": "S3_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "s3", "key": "AWS_ACCESS_KEY_ID"}}},
                        {"name": "S3_SECRET", "valueFrom": {"secretKeyRef": {"name": "s3", "key": "AWS_SECRET_ACCESS_KEY"}}},
                        {"name": "S3_REGION", "value": "us-east-1"}],
                "volumeMounts": [{"name": "cache", "mountPath": "/cache"}],
                "resources": {"limits": {"cpu": "2", "memory": "512Mi"}}},
                [{"name": "cache", "hostPath": {"path": f"/nvme/lancebench/{name}/cache", "type": "DirectoryOrCreate"}}])
            proxy["metadata"]["labels"] = {"app": "lance-s3cache"}
            rig.kube("-n", "lake-bench", "delete", "pod", "lance-s3cache", "--ignore-not-found", "--wait=true")
            rig.apply(proxy, rig.service("lance-s3cache", "lance-s3cache"))
            rig.ready("lance-s3cache")
            argv += ["--uri", f"s3://lake/lancebench/{name}", "--endpoint", "http://lance-s3cache:8080",
                     "--meter", "http://s3-meter:8080"]
        pod = rig.pod(pod_name, {"name": "bench", "image": IMAGE, "args": argv,
            "resources": {"requests": {"cpu": "4", "memory": "6Gi"}, "limits": {"cpu": "4", "memory": "6Gi"}},
            "volumeMounts": mounts}, volume)
        (root / (phase + "-pod.json")).write_text(json.dumps(pod, indent=2))
        rig.kube("-n", "lake-bench", "delete", "pod", pod_name, "--ignore-not-found", "--wait=true")
        rig.apply(pod)
        try:
            rig.wait_finished(pod_name)
        finally:
            (root / (phase + ".log")).write_text(rig.kube("-n", "lake-bench", "logs", pod_name, capture=True))
        rows = [json.loads(line) for line in (root / (phase + ".jsonl")).read_text().splitlines()]
        if rows[-1].get("operation") != "complete":
            raise RuntimeError(f"incomplete phase {phase}")
    if json.loads((root / "local-expected.json").read_text()) != json.loads((root / "s3-expected.json").read_text()):
        raise AssertionError("local/S3 reference results differ")
    print(json.dumps({"complete": True, "root": str(root)}))


if __name__ == "__main__":
    main()
