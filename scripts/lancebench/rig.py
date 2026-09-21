#!/usr/bin/env python3
"""Run the Rust serve against the kind-lake-cache rig's metered origin.

Phase 1 (done ad hoc): point --catalog-uri at the S3 root with
--endpoint through the s3-meter. This harness automates it: deploys the
seaweed+meter pods if missing, uploads the built dataset, starts the
serve locally against the metered origin, and runs the equality battery
plus the concurrent load test with origin-delta attribution.
"""

import argparse
import datetime
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
NS = "lake-bench"
CTX = "kind-lake-cache"


def kube(*args, capture=False):
    cmd = ["mise", "exec", "--", "kubectl", "--context", CTX, "-n", NS, *args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"kubectl {args}: {result.stderr}")
    return result.stdout if capture else None


def pod_ready(name):
    kube("wait", "--for=condition=Ready", f"pod/{name}", "--timeout=120s")
    print(f"{name} ready")


def seaweed_up():
    pods = kube("get", "pods", "-o", "json", capture=True)
    names = {p["metadata"]["name"] for p in json.loads(pods)["items"]}
    if "seaweed" not in names:
        raise RuntimeError("run the cachebench rig setup first (seaweed configmap expected)")
    pod_ready("seaweed")
    for svc in ("seaweed",):
        try:
            kube("get", "service", svc, capture=True)
        except RuntimeError:
            kube("expose", "pod", "seaweed", "--port=8333")
    pod_ready("s3-meter")
    pod_ready("s3-delay")


def port_forward(resource, local, remote):
    proc = subprocess.Popen(
        ["mise", "exec", "--", "kubectl", "--context", CTX, "-n", NS,
         "port-forward", resource, f"{local}:{remote}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{local}/", timeout=1)
        except Exception:
            time.sleep(0.2)
            continue
        break
    return proc


def upload_dataset(local_dir, remote_prefix):
    # rclone copy via a temporary pod; the rig image has rclone.
    pod = {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "lance-upload", "namespace": NS, "labels": {"app": "seaweed"}},
        "spec": {
            "restartPolicy": "Never",
            "nodeSelector": {"kubernetes.io/hostname": "lake-cache-control-plane"},
            "tolerations": [{"operator": "Exists"}],
            "containers": [{
                "name": "rclone",
                "image": "rclone/rclone:1.74.3",
                "command": ["sh", "-ec",
                            f"rclone copy /work/data lake:lake/{remote_prefix}/ --transfers 8"],
                "env": [
                    {"name": "RCLONE_CONFIG_LAKE_TYPE", "value": "s3"},
                    {"name": "RCLONE_CONFIG_LAKE_PROVIDER", "value": "Other"},
                    {"name": "RCLONE_CONFIG_LAKE_ENDPOINT", "value": "http://127.0.0.1:8333"},
                    {"name": "RCLONE_CONFIG_LAKE_ACCESS_KEY_ID", "value": "cachebench"},
                    {"name": "RCLONE_CONFIG_LAKE_SECRET_ACCESS_KEY", "value": "cachebench-local-only"},
                    {"name": "RCLONE_CONFIG_LAKE_REGION", "value": "us-east-1"},
                    {"name": "RCLONE_CONFIG_LAKE_FORCE_PATH_STYLE", "value": "true"},
                ],
                "volumeMounts": [{"name": "work", "mountPath": "/work"}],
            }],
            "volumes": [{"name": "work", "hostPath": {"path": "/nvme/lancebench-upload"}}],
        },
    }
    kube("delete", "pod", "lance-upload", "--ignore-not-found", "--wait=true")
    kube("apply", "-f", "-", data=json.dumps(pod))
    pod_ready("lance-upload")
    kube("delete", "pod", "lance-upload", "--wait=true")


def wait_http(url, timeout_s):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f"{url} never came up")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serve-bin", default=str(ROOT / "target/release/lakewing"))
    parser.add_argument("--dataset", required=True, help="local .lance directory")
    parser.add_argument("--remote-prefix", default="lancebench/rust-v5")
    parser.add_argument("--listen", default="127.0.0.1:3150")
    parser.add_argument("--meter", default="http://127.0.0.1:8335")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()

    seaweed_up()
    pf_meter = port_forward("svc/seaweed", 8333, 8333)
    pf_stats = port_forward("svc/s3-meter", 8335, 8080)
    pf_origin = port_forward("svc/httpcache-s3", 8334, 8080)

    # Stage the dataset onto the node's NVMe for the upload pod.
    work = Path("/var/home/adonm/dev/lakewing/.tmp/cache-bench/reader/lancebench-upload")
    subprocess.run(["sudo", "-n", "rm", "-rf", str(work)], check=False)
    work.mkdir(parents=True, exist_ok=True)
    data = work / "data"
    subprocess.run(["sudo", "-n", "cp", "-r", args.dataset, str(data)], check=True)
    subprocess.run(["sudo", "-n", "chmod", "-R", "a+rX", str(work)], check=True)
    upload_dataset(str(data), args.remote_prefix)

    s3_root = f"s3://lake/{args.remote_prefix.rsplit('/', 1)[0]}"
    # The catalog root is the parent of the table directory.
    catalog_root = s3_root
    table = args.remote_prefix.rsplit("/", 1)[-1]

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log = ROOT / ".tmp" / f"rust-rig-{stamp}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        args.serve_bin,
        "--catalog-uri", catalog_root,
        "--table", table,
        "--tag", "prod",
        # The serve runs on the host: reach the metered origin through the
        # local port-forward, not the in-cluster DNS name.
        "--endpoint", "http://127.0.0.1:8334",
        "--s3-key", "cachebench",
        "--s3-secret", "cachebench-local-only",
        "--listen", args.listen,
        "--cache-dir", str(ROOT / ".tmp" / "rust-rig-cache"),
        "--cache-bytes", str(512 * 1024 * 1024),
    ]
    print("serve:", " ".join(cmd))
    with log.open("w") as f:
        serve = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
    base = f"http://{args.listen}"
    wait_http(base + "/healthz", 120)
    print("serve up; log:", log)

    rc = subprocess.run([
        sys.executable, str(ROOT / "scripts/lancebench/battery.py"), base,
    ]).returncode
    rc |= subprocess.run([
        sys.executable, str(ROOT / "scripts/lancebench/load.py"),
        base, args.meter, str(args.threads), str(args.rounds),
    ]).returncode
    serve.terminate()
    serve.wait(timeout=10)
    for proc in (pf_meter, pf_stats, pf_origin):
        proc.terminate()
    sys.exit(rc)


if __name__ == "__main__":
    main()
