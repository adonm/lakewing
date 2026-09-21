#!/usr/bin/env python3
"""Concurrent load battery: N threads x the OGC mix, reporting latency
percentiles plus origin (meter) deltas captured around the run."""

import concurrent.futures as cf
import hashlib
import json
import statistics
import sys
import threading
import time
import urllib.request

QUERIES = {
    "ITEM": "/collections/buildings/items/relation:1003279?sources=1",
    "FULL": "/collections/buildings/items?sources=1&limit=101",
    "CITY": "/collections/buildings/items?sources=1&limit=101&bbox=4.895,52.365,4.905,52.375",
    "DEEP": "/collections/buildings/items?sources=1&limit=101&offset=50000",
}


def fetch(base, path, timeout=300):
    t0 = time.perf_counter()
    body = urllib.request.urlopen(base + path, timeout=timeout).read()
    return time.perf_counter() - t0, hashlib.sha256(body).hexdigest()[:16]


def meter_stats(url):
    with urllib.request.urlopen(url + "/stats", timeout=10) as r:
        return json.load(r)


def main():
    base = sys.argv[1]
    stats = sys.argv[2]
    threads = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    rounds = int(sys.argv[4]) if len(sys.argv) > 4 else 3
    # `none` runs without the metered rig: local warm-load measurement
    # without origin-delta attribution (origin fields report as null).
    metered = stats.lower() != "none"

    # Warm the response + storage caches once so the load measures the
    # warm-path stability, not first-touch.
    for path in QUERIES.values():
        fetch(base, path)

    before = meter_stats(stats) if metered else None
    latencies = []
    digests = {}
    lock = threading.Lock()

    def worker(round_id):
        out = []
        for name, path in QUERIES.items():
            ms, digest = fetch(base, path)
            out.append(ms * 1000)
            with lock:
                digests.setdefault((round_id % threads, name), set()).add(digest)
        return out

    t0 = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=threads) as pool:
        for lat in pool.map(worker, range(threads * rounds)):
            latencies.extend(lat)
    wall = time.perf_counter() - t0
    after = meter_stats(stats) if metered else None

    if metered:
        gets = sum(
            v["requests"] - before["counts"].get(k, {"requests": 0})["requests"]
            for k, v in after["counts"].items()
            if k not in before["counts"] or v["requests"] > before["counts"][k]["requests"]
        )
        origin_bytes = sum(
            v["bytes"] - before["counts"].get(k, {"bytes": 0})["bytes"]
            for k, v in after["counts"].items()
            if v["bytes"] > before["counts"].get(k, {"bytes": 0})["bytes"]
        )
    else:
        gets, origin_bytes = None, None

    latencies.sort()
    p = lambda q: latencies[min(int(q * len(latencies)), len(latencies) - 1)]
    mismatches = {k: v for k, v in digests.items() if len(v) > 1}
    print(json.dumps({
        "threads": threads, "rounds": rounds, "queries": len(latencies),
        "wall_s": round(wall, 2), "rps": round(len(latencies) / wall, 2),
        "p50_ms": round(statistics.median(latencies), 1),
        "p95_ms": round(p(0.95), 1), "max_ms": round(latencies[-1], 1),
        "origin_gets_during_load": gets, "origin_bytes_during_load": origin_bytes,
        "digest_mismatches": len(mismatches),
    }))


if __name__ == "__main__":
    main()
