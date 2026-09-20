#!/usr/bin/env python3
"""OGC battery: md5 equality + timings for a list of serve bases."""
import hashlib, json, sys, time, urllib.request

def canonical(body):
    return hashlib.sha256(json.dumps(json.loads(body), sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]

QUERIES = {
    "ITEM":  "/collections/buildings/items/relation:1003279?sources=1",
    "FULL":  "/collections/buildings/items?sources=1&limit=101",
    "CITY":  "/collections/buildings/items?sources=1&limit=101&bbox=4.895,52.365,4.905,52.375",
    "DEEP":  "/collections/buildings/items?sources=1&limit=101&offset=50000",
}

def fetch(base, path, runs=3):
    url = base + path
    urllib.request.urlopen(url).read()  # warm
    best, times = None, []
    for _ in range(runs):
        t0 = time.perf_counter()
        body = urllib.request.urlopen(url).read()
        times.append((time.perf_counter() - t0) * 1000)
        best = canonical(body)
    return best, sorted(times)[len(times)//2]

def main():
    bases = sys.argv[1:]
    results = {}
    for base in bases:
        results[base] = {}
        for name, path in QUERIES.items():
            digest, ms = fetch(base, path)
            results[base][name] = (digest, round(ms, 1))
    names = list(QUERIES)
    header = f"{'query':6}" + "".join(f"{b.split('//')[1].split(':')[1]:>16}" for b in bases)
    print(header)
    for q in names:
        row = f"{q:6}"
        first = results[bases[0]][q][0]
        for b in bases:
            digest, ms = results[b][q]
            mark = "" if digest == first else " !MISMATCH"
            row += f"{ms:>12.1f} ms{mark if mark else '':<4}"
        print(row)
    if len(bases) > 1:
        mism = [q for q in names if len({results[b][q][0] for b in bases}) > 1]
        print("equality:", "FAIL " + ",".join(mism) if mism else "all match")

if __name__ == "__main__":
    main()
