#!/usr/bin/env python3
"""OGC equality gate: canonical-JSON digests across serve bases.

Exits non-zero when any query mismatches between bases, so CI or a rig
run fails loudly instead of printing a table. Timings are informational:
the gate is result equality, not speed.

Usage: battery.py BASE [BASE ...]
"""

import hashlib
import json
import sys
import time
import urllib.error
import urllib.request

QUERIES = {
    "ITEM": "/collections/buildings/items/relation:1003279?sources=1",
    "FULL": "/collections/buildings/items?sources=1&limit=101",
    "CITY": "/collections/buildings/items?sources=1&limit=101&bbox=4.895,52.365,4.905,52.375",
    "DEEP": "/collections/buildings/items?sources=1&limit=101&offset=50000",
}


def canonical(body: bytes) -> str:
    return hashlib.sha256(
        json.dumps(json.loads(body), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]


def fetch(url: str, timeout: int = 300) -> bytes:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise SystemExit(f"{url}: HTTP {error.code} {error.read()[:200]!r}")


def measure(base: str, path: str, runs: int = 3) -> tuple[str, float]:
    fetch(base + path)  # warm
    times, digest = [], None
    for _ in range(runs):
        started = time.perf_counter()
        digest = canonical(fetch(base + path))
        times.append((time.perf_counter() - started) * 1000)
    return digest, sorted(times)[len(times) // 2]


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    bases = sys.argv[1:]
    results: dict[str, dict[str, tuple[str, float]]] = {}
    for base in bases:
        results[base] = {name: measure(base, path) for name, path in QUERIES.items()}

    port = lambda base: base.split("//")[1].rsplit(":", 1)[-1]
    print(f"{'query':6}" + "".join(f"{port(base):>16}" for base in bases))
    mismatches: list[str] = []
    for name in QUERIES:
        row = f"{name:6}"
        digests = {results[base][name][0] for base in bases}
        for base in bases:
            digest, ms = results[base][name]
            row += f"{ms:>12.1f} ms{'' if digest in digests and len(digests) == 1 else ' !MISMATCH':<4}"
        if len(digests) > 1:
            mismatches.append(name)
        print(row)

    if len(bases) > 1:
        print("equality:", "FAIL " + ",".join(mismatches) if mismatches else "all match")
    if mismatches:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
