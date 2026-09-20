#!/usr/bin/env python3
"""Heavy-query battery: times the most expensive OGC shapes against a
running server and reports ms + bytes + rows. Usage:
bench_heavy.py --base http://127.0.0.1:3126 [--repeat 3]
"""
import argparse
import http.client
import time
from urllib.parse import urlsplit

QUERIES = [
    ("full-region page", "/collections/buildings/items?bbox=2,48,6,54&sources=1&limit=1000"),
    ("broad quarter", "/collections/buildings/items?bbox=2,48,4,51&sources=1&limit=100"),
    ("city window", "/collections/buildings/items?bbox=4.895,52.365,4.905,52.375&sources=1&limit=100"),
    ("deep offset", "/collections/buildings/items?sources=1&limit=100&offset=50000"),
    ("single item", "/collections/buildings/items/relation:10000948?sources=1"),
    ("empty bbox", "/collections/buildings/items?bbox=0,0,0.1,0.1&sources=1&limit=10"),
    ("tile z5 land", "/collections/buildings/tiles/5/16/10?sources=1"),
    ("tile z12 city", "/collections/buildings/tiles/12/2102/1383?sources=1"),
    ("datetime page", "/collections/buildings/items?bbox=4.3,51.9,4.5,52.1&sources=1&limit=100&datetime=2024-01-01T00:00:00Z"),
    ("gzip page", "/collections/buildings/items?bbox=4.3,51.9,4.5,52.1&sources=1&limit=100"),
]


def fetch(base, path, gzip=False):
    u = urlsplit(base)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=120)
    headers = {}
    if gzip:
        headers["Accept-Encoding"] = "gzip"
    conn.request("GET", path, headers=headers)
    resp = conn.getresponse()
    body = resp.read()
    return resp.status, len(body), dict(resp.getheaders())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:3126")
    ap.add_argument("--repeat", type=int, default=3)
    args = ap.parse_args()
    print(f"{'query':<18} {'status':>6} {'ms':>10} {'bytes':>10} rows/detail")
    for name, path in QUERIES:
        best, results = None, []
        for _ in range(args.repeat):
            t0 = time.perf_counter()
            status, nbytes, _ = fetch(args.base, path, gzip=(name == "gzip page"))
            ms = (time.perf_counter() - t0) * 1000
            results.append((status, ms, nbytes))
            if best is None or ms < best[1]:
                best = (status, ms, nbytes)
        detail = ""
        if "tile" in name:
            detail = "mvt" if best[2] > 0 else "empty"
        print(f"{name:<18} {best[0]:>6} {best[1]:>10.0f} {best[2]:>10} {detail}")


if __name__ == "__main__":
    main()
