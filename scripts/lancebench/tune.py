#!/usr/bin/env python3
"""Equal-budget index/cache sweep over distinct overlapping queries.

Example (run with mise exec -- python):
  tune.py --uri .tmp/tuning/geo.lance --out .tmp/tuning/results \
    --case baseline:prod:262144:65536 --case indexes:tuned:262144:65536
  tune.py --uri s3://lake/path/geo.lance --endpoint http://127.0.0.1:8336 \
    --out .tmp/tuning/s3 --case exact:prod:0:0 --case blocks:prod:262144:65536

Every response is compared with the first case, including geometry, properties,
order and pagination links (only snapshot versions are normalized). MVT bytes
must match exactly. Errors/mismatches exit nonzero. Fresh foyer directories are
under --out; OS/origin caches are not flushed. This is a host-process benchmark.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request


def canonical(value):
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in value.items()}
    if isinstance(value, list):
        return [canonical(item) for item in value]
    if isinstance(value, str) and value.startswith("/collections/"):
        url = urllib.parse.urlsplit(value)
        query = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        if "snapshot" in query:
            query["snapshot"] = ["0"]
        if "cursor" in query:
            cursor = json.loads(query["cursor"][0])
            cursor["version"] = 0
            query["cursor"] = [json.dumps(cursor, sort_keys=True)]
        return urllib.parse.urlunsplit(url._replace(query=urllib.parse.urlencode(sorted(query.items()), doseq=True)))
    return value


def request(base, path):
    start = time.perf_counter()
    with urllib.request.urlopen(base + path, timeout=90) as response:
        body = response.read()
        status = response.status
        content_type = response.headers.get("Content-Type", "")
    ms = (time.perf_counter() - start) * 1000
    if "json" in content_type:
        body = json.dumps(canonical(json.loads(body)), sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(str(status).encode() + body).hexdigest()
    return {"ms": ms, "status": status, "bytes": len(body), "digest": digest}


def metrics(base):
    with urllib.request.urlopen(base + "/metrics", timeout=10) as response:
        return {line.split()[0]: float(line.split()[1]) for line in response.read().decode().splitlines()
                if line.startswith(("lakewing_cache_", "lakewing_response_cache_"))}


def workload():
    queries = {
        "ITEM": "/collections/buildings/items/relation:1003279?sources=1",
        "FULL": "/collections/buildings/items?sources=1&limit=101",
        "DEEP": "/collections/buildings/items?sources=1&limit=101&offset=50000",
        "WIDE": "/collections/buildings/items?sources=1&limit=101&bbox=3,47,8,54",
    }
    for i in range(8):
        w, s = 4.895 + i * .0003, 52.365 + i * .0002
        queries[f"CITY{i}"] = f"/collections/buildings/items?sources=1&limit=101&bbox={w:.6f},{s:.6f},{w+.01:.6f},{s+.01:.6f}"
    for z, x, y in [(6, 32, 21), (7, 64, 42), (8, 129, 85), (10, 525, 336),
                    *[(12, x, y) for x in range(2102, 2105) for y in range(1345, 1348)]]:
        queries[f"T{z}/{x}/{y}"] = f"/collections/buildings/tiles/{z}/{x}/{y}?sources=1"
    return queries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/release/lakewing")
    parser.add_argument("--uri", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--case", action="append", required=True, help="NAME:TAG:DATA_BLOCK_BYTES:INDEX_BLOCK_BYTES")
    parser.add_argument("--endpoint")
    parser.add_argument("--s3-key")
    parser.add_argument("--s3-secret")
    parser.add_argument("--port", type=int, default=3195)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--cache-bytes", type=int, default=512 * 1024**2)
    parser.add_argument("--cache-memory-bytes", type=int, default=64 * 1024**2)
    parser.add_argument("--lance-index-cache-bytes", type=int, default=256 * 1024**2)
    parser.add_argument("--cache-fetch-concurrency", type=int, default=16)
    parser.add_argument("--cache-max-range-bytes", type=int, default=8 * 1024**2)
    parser.add_argument("--selective-only", action="store_true", help="Skip wide/full scans for cache-probe sweeps")
    args = parser.parse_args()
    if args.rounds < 1 or args.concurrency < 1:
        parser.error("rounds and concurrency must be positive")
    args.out.mkdir(parents=True, exist_ok=True)
    base = f"http://127.0.0.1:{args.port}"
    queries = workload()
    if args.selective_only:
        queries = {name: path for name, path in queries.items() if name == "ITEM" or name.startswith(("CITY", "T12"))}
    expected, results = {}, {}
    for case in args.case:
        name, tag, data, index = case.split(":")
        directory = Path(tempfile.mkdtemp(prefix=name + "-", dir=args.out))
        command = [args.binary, "--uri", args.uri, "--tag", tag, "--listen", base.split("//")[1],
                   "--cache-dir", str(directory / "cache"), "--cache-bytes", str(args.cache_bytes),
                   "--cache-memory-bytes", str(args.cache_memory_bytes), "--cache-block-bytes", data,
                   "--cache-index-block-bytes", index, "--lance-index-cache-bytes", str(args.lance_index_cache_bytes),
                   "--lance-metadata-cache-bytes", str(64 * 1024**2), "--cache-fetch-concurrency", str(args.cache_fetch_concurrency),
                   "--cache-max-range-bytes", str(args.cache_max_range_bytes), "--response-cache-bytes", "0",
                   "--concurrency", str(args.concurrency), "--duck-threads", "1", "--duck-memory-mb", "512"]
        for key in ["endpoint", "s3_key", "s3_secret"]:
            value = getattr(args, key)
            if value:
                command.extend(["--" + key.replace("_", "-"), value])
        with (directory / "serve.log").open("w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 120
                while True:
                    if process.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError(f"server failed: {directory / 'serve.log'}")
                    try:
                        urllib.request.urlopen(base + "/healthz", timeout=1).close()
                        break
                    except OSError:
                        time.sleep(.2)
                initial = metrics(base)
                samples = {query: [] for query in queries}

                def checked(query):
                    sample = request(base, queries[query])
                    if query in expected and sample["digest"] != expected[query]:
                        raise AssertionError(f"result mismatch: {name} {query}")
                    return sample

                phases = []
                for iteration in range(args.rounds + 1):
                    before = metrics(base)
                    for query in queries:
                        sample = checked(query)
                        expected.setdefault(query, sample["digest"])
                        samples[query].append(sample)
                    after = metrics(base)
                    phases.append({key: after.get(key, 0) - before.get(key, 0) for key in after})
                # Shared-data herd: different overlapping bboxes, warm data, no body cache.
                before = metrics(base)
                with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                    herd = list(pool.map(checked, [f"CITY{i % 8}" for i in range(32)]))
                after = metrics(base)
                assert after.get("lakewing_response_cache_hits_total", 0) == 0
                results[name] = {"case": case, "initial_metrics": initial, "phases": phases, "samples": samples,
                                 "herd_ms": [s["ms"] for s in herd],
                                 "herd_metrics": {key: after.get(key, 0) - before.get(key, 0) for key in after}}
                for query, rows in samples.items():
                    print(f"{name:12} {query:16} first={rows[0]['ms']:9.1f} ms warm={statistics.median(s['ms'] for s in rows[1:]):9.1f} ms", flush=True)
                print(name, "origin deltas:", json.dumps(phases), flush=True)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        output = {"uri": args.uri, "budgets": {k: v for k, v in vars(args).items() if "bytes" in k or k == "concurrency"},
                  "results": results, "expected": expected}
        (args.out / "results.json").write_text(json.dumps(output, indent=2))
    print("PASS: complete normalized GeoJSON and byte-identical MVT across all cases/rounds/herds")


if __name__ == "__main__":
    main()
