#!/usr/bin/env python3
"""Bounded-cache DuckLake comparison. All storage and queries run in kind."""

import argparse
import contextlib
import datetime
import hashlib
import json
from pathlib import Path
import socket
import shutil
import statistics
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
NS = "lake-bench"
IMAGE = "lakewing-cachebench:dev"
PROXY_IMAGE = "lakewing-s3cache:dev"


def command(*args, capture=False, data=None):
    result = subprocess.run(args, input=data, text=True, check=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else None


class Rig:
    def __init__(self, args):
        self.args = args
        self.context = "kind-" + args.cluster
        self.root = Path(args.directory).resolve()
        self.worker = args.cluster + "-worker"
        self.control = args.cluster + "-control-plane"
        self.run_name = "setup"

    def kube(self, *args, **kwargs):
        return command("kubectl", "--context", self.context, *args, **kwargs)

    def apply(self, *objects):
        self.kube("apply", "-f", "-", data=json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}))

    def obj(self, kind, name, spec=None, **extra):
        obj = {"apiVersion": "v1", "kind": kind, "metadata": {"name": name, "namespace": NS}, **extra}
        if spec is not None:
            obj["spec"] = spec
        return obj

    def pod(self, name, container, volumes=(), node=None):
        return self.obj("Pod", name, {
            "restartPolicy": "Never", "nodeSelector": {"kubernetes.io/hostname": node or self.worker},
            "tolerations": [{"operator": "Exists"}],
            "containers": [container], "volumes": list(volumes),
        })

    def service(self, name, app, port=8080, target=None):
        return self.obj("Service", name, {"selector": {"app": app}, "ports": [{"port": port, "targetPort": target or port}]})

    def ready(self, name):
        try:
            self.kube("-n", NS, "wait", "--for=condition=Ready", "pod/" + name, "--timeout=300s")
        except subprocess.CalledProcessError:
            self.kube("-n", NS, "describe", "pod", name)
            raise

    @contextlib.contextmanager
    def forward(self, resource, remote=8080, namespace=NS):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        proc = subprocess.Popen(["kubectl", "--context", self.context, "-n", namespace,
                                 "port-forward", resource, f"{port}:{remote}"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            for _ in range(100):
                if proc.poll() is not None:
                    raise RuntimeError(proc.stderr.read().decode())
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("port-forward did not become ready")
            yield f"http://127.0.0.1:{port}"
        finally:
            proc.terminate()
            proc.wait(timeout=10)
            proc.stderr.close()

    def up(self):
        self.root.mkdir(parents=True, exist_ok=True)
        backing = json.loads(command("findmnt", "-J", "-T", str(self.root), "-o", "TARGET,SOURCE,FSTYPE", capture=True))["filesystems"][0]
        if backing["fstype"] in ("tmpfs", "ramfs", "overlay"):
            raise RuntimeError(f"cache root must be on a real disk filesystem: {backing}")
        (self.root / "backing.json").write_text(json.dumps(backing, indent=2))
        for name in ("storage", "reader"):
            (self.root / name).mkdir(exist_ok=True)
        clusters = command("kind", "get", "clusters", capture=True).splitlines()
        if self.args.cluster not in clusters:
            config = {"kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4", "name": self.args.cluster, "nodes": []}
            for role, directory in (("control-plane", "storage"), ("worker", "reader")):
                config["nodes"].append({"role": role, "extraMounts": [
                    {"hostPath": str(self.root / directory), "containerPath": "/nvme"},
                    {"hostPath": str(ROOT / "fixtures"), "containerPath": "/fixtures", "readOnly": True},
                ]})
            config_path = self.root / "kind.json"
            config_path.write_text(json.dumps(config))
            command("kind", "create", "cluster", "--image", "kindest/node:v1.36.1", "--config", str(config_path))
        # Verify an existing cluster has the same real-disk bind, too.
        mounts = json.loads(command("docker", "inspect", self.worker, "--format", "{{json .Mounts}}", capture=True))
        if not any(m["Destination"] == "/nvme" and m["Source"] == str(self.root / "reader") for m in mounts):
            raise RuntimeError("existing cluster has a different /nvme backing; use another --cluster")
        self.apply(self.obj("Namespace", NS))
        credentials = {"AWS_ACCESS_KEY_ID": "cachebench", "AWS_SECRET_ACCESS_KEY": "cachebench-local-only"}
        self.apply(self.obj("Secret", "s3", type="Opaque", stringData=credentials))
        self.kube("apply", "-f", str(ROOT / "k8s/lgtm.yaml"))
        patch = {"spec": {"template": {"spec": {"nodeSelector": {"kubernetes.io/hostname": self.control}, "tolerations": [{"operator": "Exists"}]}}}}
        self.kube("-n", "monitoring", "patch", "deployment", "lgtm", "--type=merge", "-p", json.dumps(patch))
        self.kube("apply", "-f", str(ROOT / "k8s/cachebench-alloy.yaml"))
        self.kube("-n", "monitoring", "rollout", "status", "deployment/lgtm", "--timeout=300s")
        self.kube("-n", "monitoring", "rollout", "status", "daemonset/cachebench-alloy", "--timeout=300s")
        if not self.args.skip_build:
            command("docker", "build", "-f", str(ROOT / "scripts/cachebench/Dockerfile"), "-t", IMAGE, str(ROOT))
            command("docker", "build", "-f", str(ROOT / "scripts/s3cache/Dockerfile"), "-t", PROXY_IMAGE, str(ROOT))
        command("kind", "load", "docker-image", IMAGE, "--name", self.args.cluster)
        command("kind", "load", "docker-image", PROXY_IMAGE, "--name", self.args.cluster)
        self.storage(credentials)
        self.node_tools()

    def node_tools(self):
        self.kube("-n", NS, "delete", "pod", "node-tools", "--ignore-not-found", "--wait=true")
        pod = self.pod("node-tools", {"name": "node-tools", "image": IMAGE,
            "command": ["sleep", "infinity"], "securityContext": {"privileged": True},
            "volumeMounts": [{"name": "cgroups", "mountPath": "/cgroups"}, {"name": "nvme", "mountPath": "/nvme"}]},
            [{"name": "cgroups", "hostPath": {"path": "/sys/fs/cgroup"}}, {"name": "nvme", "hostPath": {"path": "/nvme"}}])
        pod['spec']['hostPID'] = True
        self.apply(pod)
        self.ready("node-tools")

    def node(self, *args):
        return json.loads(self.kube("-n", NS, "exec", "node-tools", "--", "python3", "/usr/local/bin/bench-node", *args, capture=True))

    def storage(self, credentials):
        identity = {"identities": [{"name": "bench", "actions": ["Admin"], "credentials": [{"accessKey": credentials["AWS_ACCESS_KEY_ID"], "secretKey": credentials["AWS_SECRET_ACCESS_KEY"]}]}]}
        self.apply(self.obj("ConfigMap", "seaweed-config", data={"s3.json": json.dumps(identity)}))
        pod = self.pod("seaweed", {"name": "seaweed", "image": "chrislusf/seaweedfs:4.13", "args": ["server", "-dir=/data", "-s3", "-s3.port=8333", "-s3.config=/config/s3.json"], "volumeMounts": [{"name": "data", "mountPath": "/data"}, {"name": "config", "mountPath": "/config"}], "readinessProbe": {"tcpSocket": {"port": 8333}, "periodSeconds": 2}}, [{"name": "data", "hostPath": {"path": "/nvme/seaweed", "type": "DirectoryOrCreate"}}, {"name": "config", "configMap": {"name": "seaweed-config"}}], self.control)
        pod["metadata"]["labels"] = {"app": "seaweed"}
        self.apply(pod, self.service("seaweed", "seaweed", 8333))
        self.ready("seaweed")
        self.metering()
        self.seed()

    def metering(self):
        meter = self.pod("s3-meter", {"name": "s3-meter", "image": IMAGE, "args": ["meter"], "env": [{"name": "UPSTREAM", "value": "http://s3-delay.lake-bench.svc.cluster.local:8080"}], "ports": [{"name": "metrics", "containerPort": 8080}], "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}}, node=self.control)
        meter["metadata"]["labels"] = {"app": "s3-meter"}
        self.kube("-n", NS, "delete", "pod", "s3-meter", "--ignore-not-found", "--wait=true")
        self.apply(meter, self.service("s3-meter", "s3-meter"), *(self.service(b + "-s3", "s3-meter") for b in ("direct", "httpcache")))
        self.ready("s3-meter")
        # Origin-latency injector: every metered S3 byte passes through
        # here on its way to SeaweedFS, so all backends share one RTT.
        # Seed traffic addresses seaweed directly and stays un delayed.
        delay = self.pod("s3-delay", {"name": "s3-delay", "image": IMAGE, "args": ["delay"], "env": [
            {"name": "UPSTREAM", "value": "http://seaweed:8333"},
            {"name": "LATENCY_MS", "value": str(self.args.s3_latency_ms)},
            {"name": "JITTER_MS", "value": str(self.args.s3_latency_jitter_ms)},
        ], "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}}, node=self.control)
        delay["metadata"]["labels"] = {"app": "s3-delay"}
        self.kube("-n", NS, "delete", "pod", "s3-delay", "--ignore-not-found", "--wait=true")
        self.apply(delay, self.service("s3-delay", "s3-delay"))
        self.ready("s3-delay")

    def seed(self):
        self.kube("-n", NS, "delete", "pod", "seed", "--ignore-not-found", "--wait=true")
        seed = self.pod("seed", {"name": "seed", "image": IMAGE, "command": ["sh", "-ec", "rclone copy /fixtures/nw-europe.files lake:lake/nw/data\nrclone copyto /fixtures/nw-europe.ducklake lake:lake/nw/catalogs/nw-europe.ducklake"], "env": self.rclone_env("http://seaweed:8333"), "envFrom": [{"secretRef": {"name": "s3"}}], "volumeMounts": [{"name": "fixtures", "mountPath": "/fixtures", "readOnly": True}]}, [{"name": "fixtures", "hostPath": {"path": "/fixtures"}}], self.control)
        self.apply(seed)
        self.wait_finished("seed")

    def rclone_env(self, endpoint):
        return [{"name": "RCLONE_CONFIG_LAKE_" + k, "value": v} for k, v in {"TYPE": "s3", "PROVIDER": "Other", "ENDPOINT": endpoint, "ENV_AUTH": "true", "REGION": "us-east-1", "FORCE_PATH_STYLE": "true"}.items()]

    def wait_finished(self, name):
        for _ in range(600):
            pod = json.loads(self.kube("-n", NS, "get", "pod", name, "-o", "json", capture=True))
            phase = pod["status"]["phase"]
            if phase in ("Succeeded", "Failed"):
                self.kube("-n", NS, "logs", name)
                if phase == "Failed":
                    raise RuntimeError(name + " failed")
                return
            time.sleep(1)
        raise RuntimeError(name + " timed out")

    def mounts(self, backend):
        if backend == "local":
            return [{"name": "fixtures", "hostPath": {"path": "/fixtures"}}], [{"name": "fixtures", "mountPath": "/fixtures", "readOnly": True}]
        return [], []

    def cleanup(self):
        self.kube("-n", NS, "delete", "pod", "reader-a", "reader-b", "--ignore-not-found", "--wait=true")
        self.kube("-n", NS, "delete", "pod", "s3cache", "--ignore-not-found", "--wait=true")

    def proxy(self):
        """(Re)create the node-local s3cache pod. Its origin traffic flows
        through the meter as httpcache-s3, so S3 deltas stay comparable;
        the /nvme/s3cache dir survives pod recreation (restart phase)."""
        self.kube("-n", NS, "delete", "pod", "s3cache", "--ignore-not-found", "--wait=true")
        pod = self.pod("s3cache", {"name": "s3cache", "image": PROXY_IMAGE, "env": [
            {"name": "UPSTREAM", "value": "http://httpcache-s3.lake-bench.svc.cluster.local:8080"},
            {"name": "LISTEN", "value": ":8080"},
            {"name": "CACHE_DIR", "value": "/cache"},
            {"name": "CACHE_BYTES", "value": str(self.args.cache_mib * 1024**2)},
            {"name": "SLICE_BYTES", "value": str(self.args.slice_bytes)},
            {"name": "READAHEAD", "value": "4"},
            {"name": "S3_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "s3", "key": "AWS_ACCESS_KEY_ID"}}},
            {"name": "S3_SECRET", "valueFrom": {"secretKeyRef": {"name": "s3", "key": "AWS_SECRET_ACCESS_KEY"}}},
            {"name": "S3_REGION", "value": "us-east-1"},
        ], "ports": [{"name": "metrics", "containerPort": 8080}], "volumeMounts": [{"name": "cache", "mountPath": "/cache"}],
            "readinessProbe": {"httpGet": {"port": 8080, "path": "/healthz"}, "periodSeconds": 2}},
            [{"name": "cache", "hostPath": {"path": "/nvme/s3cache", "type": "DirectoryOrCreate"}}])
        pod["metadata"]["labels"] = {"app": "s3cache"}
        self.apply(pod, self.service("httpcache-proxy", "s3cache"))
        self.ready("s3cache")

    def readers(self, backend, reset=False):
        if reset:
            self.cleanup()
            # Only a stopped proxy may have its disk cache removed.
            self.kube('-n', NS, 'exec', 'node-tools', '--', 'python3', '-c',
                      'import shutil, pathlib; p=pathlib.Path("/nvme/s3cache"); shutil.rmtree(p, ignore_errors=True); p.mkdir(parents=True)')
        if backend == "proxy":
            self.proxy()
        for slot in ("a", "b"):
            self.reader(backend, slot)

    def reader(self, backend, slot):
            name = "reader-" + slot
            self.kube("-n", NS, "delete", "pod", name, "--ignore-not-found", "--wait=true")
            volumes, mounts = self.mounts(backend)
            catalog, data = "/lake/catalogs/nw-europe.ducklake", "/lake/data/"
            if backend in ("direct", "proxy"):
                catalog, data = "s3://lake/nw/catalogs/nw-europe.ducklake", "s3://lake/nw/data/"
            elif backend == "local":
                catalog, data = "/fixtures/nw-europe.ducklake", "/fixtures/nw-europe.files/"
            envs = {"BACKEND": backend, "CATALOG": catalog, "DATA_ROOT": data, "THREADS": str(self.args.threads), "MEMORY_LIMIT": self.args.memory, "OTLP_ENDPOINT": "http://lgtm.monitoring:4318"}
            if backend == "direct":
                envs["S3_ENDPOINT"] = "direct-s3.lake-bench.svc.cluster.local:8080"
            elif backend == "proxy":
                envs["S3_ENDPOINT"] = "httpcache-proxy.lake-bench.svc.cluster.local:8080"
            volumes += [{"name": "temp", "emptyDir": {"sizeLimit": "4Gi"}}, {"name": "results", "hostPath": {"path": f"/nvme/results/{self.run_name}/{backend}/{slot}", "type": "DirectoryOrCreate"}}]
            mounts += [{"name": "temp", "mountPath": "/duckdb-temp"}, {"name": "results", "mountPath": "/results"}]
            # Direct readers sign origin requests themselves and need real
            # credentials. Proxy readers stay credential-free (anonymous to
            # the proxy, which is the sole signer): DuckDB then skips
            # per-request SigV4 entirely.
            creds = [{"secretRef": {"name": "s3"}}] if backend == "direct" else []
            pod = self.pod(name, {"name": "duckdb-" + backend, "image": IMAGE, "env": [{"name": k, "value": v} for k, v in envs.items()], "envFrom": creds, "ports": [{"name": "metrics", "containerPort": 8080}], "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": str(self.args.threads), "memory": "6Gi"}}, "volumeMounts": mounts, "readinessProbe": {"httpGet": {"port": 8080, "path": "/healthz"}, "periodSeconds": 2}}, volumes)
            self.apply(pod)
            self.ready(name)

    def run(self):
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output = self.root / ("run-" + stamp)
        output.mkdir(parents=True)
        self.run_name = output.name
        config = {"cluster": self.args.cluster, "cache_mib": self.args.cache_mib, "threads": self.args.threads, "memory": self.args.memory, "repeats": self.args.repeats, "queries": self.args.queries, "s3_latency_ms": self.args.s3_latency_ms, "s3_latency_jitter_ms": self.args.s3_latency_jitter_ms, "backing": json.loads((self.root / "backing.json").read_text()), "dataset_bytes": sum(p.stat().st_size for p in (ROOT / "fixtures/nw-europe.files").rglob("*.parquet")), "catalog_sha256": hashlib.sha256((ROOT / "fixtures/nw-europe.ducklake").read_bytes()).hexdigest(), "git_revision": command('git', '-C', str(ROOT), 'rev-parse', 'HEAD', capture=True).strip(), "image": json.loads(command('docker', 'inspect', IMAGE, capture=True))[0]['Id'], "start": time.time()}
        if config["dataset_bytes"] <= self.args.cache_mib * 1024**2:
            raise RuntimeError("dataset must exceed the cache budget")
        (output / "config.json").write_text(json.dumps(config, indent=2))
        self.node_tools()
        self.metering()
        fingerprints, samples = {}, []
        with self.forward("svc/s3-meter") as meter:
            for backend in self.args.backends.split(","):
                print("BACKEND", backend, flush=True)
                self.readers(backend, reset=True)
                self.kube("-n", NS, "exec", "reader-a", "--", "cachebench.test", "-test.v")
                with self.forward("pod/reader-a") as a, self.forward("pod/reader-b") as b:
                    def observed(base, path, label):
                        before = settled_stats(meter)
                        before_node = self.node('stats')
                        result = request(base + path, method="POST")
                        after = settled_stats(meter)
                        result['node_before'] = before_node
                        result['node_after'] = self.node('stats')
                        result["s3"] = delta(before["counts"], after["counts"], backend)
                        result["s3_active"] = {"before": before["active"], "after": after["active"]}
                        if not before.get("quiesced", True) or not after.get("quiesced", True):
                            result["s3_quiesced"] = False
                        result["label"] = label
                        if "query" in result:
                            key = result["query"]
                            fingerprint = (result["rows"], result["sha256"])
                            if fingerprints.setdefault(key, fingerprint) != fingerprint:
                                raise RuntimeError(f"result mismatch: {backend} {key}")
                            expected = {"CITY": 101, "BROAD": 101, "FULL": 1001, "DEEP": 101, "SCAN": 1}[key]
                            if result['rows'] != expected:
                                raise RuntimeError(f"unexpected row count: {backend} {key}")
                        elif result['metadata'] != [["v2.0.0-alpha42069", "6"]] or result['snapshot'] != 6:
                            raise RuntimeError('engine or catalog snapshot mismatch')
                        samples.append(result)
                        with (output / "samples.jsonl").open("a") as f:
                            f.write(json.dumps(result) + "\n")
                        print(json.dumps({k: v for k, v in result.items() if not k.startswith('node_')}), flush=True)
                        return result

                    observed(a, "/open", "cold-attach")
                    perf_dir = f'/nvme/results/{self.run_name}/{backend}/perf'
                    self.node('start', perf_dir)
                    for query in self.args.queries.split(","):
                        observed(a, f"/query?name={query}&phase=first", "first")
                        for _ in range(self.args.repeats):
                            observed(a, f"/query?name={query}&phase=warm", "warm")
                    # Rewarm CITY immediately before a different pod uses it.
                    observed(a, "/query?name=CITY&phase=prime-peer", "prime-peer")
                    observed(b, "/open", "peer-attach")
                    observed(b, "/query?name=CITY&phase=peer-first", "peer-first")
                    for _ in range(self.args.repeats):
                        observed(b, "/query?name=CITY&phase=peer-warm", "peer-warm")
                    # Remove process-local DuckDB state and reclaim only benchmark
                    # cgroups' file pages. Never drop the machine's global caches.
                    request(a + '/close', method='POST')
                    request(b + '/close', method='POST')
                    (output / f'{backend}-reclaim.json').write_text(json.dumps(self.node('reclaim'), indent=2))
                    observed(b, '/open', 'reclaimed-attach')
                    observed(b, '/query?name=CITY&phase=reclaimed-first', 'reclaimed-first')
                    observed(b, "/query?name=SCAN&phase=pollution", "pollution")
                    request(b + '/close', method='POST')
                    time.sleep(2)
                    observed(b, '/open', 'after-scan-attach')
                    observed(b, '/query?name=CITY&phase=after-scan-first', 'after-scan-first')
                    for _ in range(self.args.repeats):
                        observed(b, "/query?name=CITY&phase=after-scan-warm", "after-scan-warm")
                    (output / f'{backend}-native.json').write_text(json.dumps(self.node('stop', perf_dir), indent=2))
                    (output / f"{backend}-metrics.txt").write_text(request(b + "/metrics", raw=True).decode())
                    if backend == "proxy":
                        # The proxy pod is deleted at cleanup; persist its
                        # counters (fetch latency, evictions) with the run.
                        with self.forward("pod/s3cache") as proxy:
                            (output / f"{backend}-proxy-metrics.txt").write_text(request(proxy + "/metrics", raw=True).decode())
                    # Keep completed observations available for two Alloy scrapes.
                    time.sleep(10)
                self.cleanup()
                # Restart the proxy with its disk directory intact, then open a
                # new reader. This is a distinct cache-reuse condition.
                self.readers(backend)
                with self.forward('pod/reader-a') as a:
                    observed(a, '/open', 'restart-attach')
                    observed(a, '/query?name=CITY&phase=restart-first', 'restart-first')
                    time.sleep(10)
                self.cleanup()
        (output / "summary.md").write_text(summary(samples, config))
        (output / 'pods.json').write_text(self.kube("get", "pods", "-A", "-o", "json", capture=True))
        config['end'] = time.time()
        (output / 'config.json').write_text(json.dumps(config, indent=2))
        print("RESULTS", output, flush=True)
        print((output / "summary.md").read_text())


def request(url, method="GET", raw=False):
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=900) as response:
            data = response.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"{url}: {error.code}: {error.read().decode()}") from error
    return data if raw else json.loads(data)


def settled_stats(base, timeout=60):
    # Readers cancel speculative ranges; require stable counts+active for
    # 1s rather than absolute zero. Best-effort: never fail the run on
    # quiescence.
    last, stable = None, 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = request(base + "/stats")
        if last is not None and current["active"] == last["active"] and current["counts"] == last["counts"]:
            stable += 1
            if stable >= 5:
                current["quiesced"] = True
                return current
        else:
            stable = 0
        last = current
        time.sleep(0.2)
    last["quiesced"] = False
    return last


def delta(before, after, backend):
    # The proxy's origin traffic is metered under httpcache-s3.
    prefix = {"proxy": "httpcache-s3"}.get(backend, backend + "-s3")
    result = {}
    for key, count in after.items():
        if key.split("/")[0] != prefix:
            continue
        old = before.get(key, {"requests": 0, "bytes": 0})
        difference = {field: count[field] - old[field] for field in ("requests", "bytes")}
        if difference["requests"]:
            result[key] = difference
    return result


def summary(samples, config):
    lines = ["# Bounded-cache DuckLake benchmark", "", f"Dataset: {config['dataset_bytes']:,} bytes; disk cache: {config['cache_mib']} MiB; DuckDB: {config['threads']} threads / {config['memory']}; S3 latency: {config.get('s3_latency_ms', 0)}ms + {config.get('s3_latency_jitter_ms', 0)}ms jitter.", "", "All query rows are consumed and SHA-256 compared across backends and repetitions.", "First touches are ordered within a session, not independently cold queries. Peer uses a second pod. SCAN reads every geometry/property payload.", "", "| Backend | Query | Phase | n | Median ms | S3 GETs (total) | S3 body MiB (total) |", "|---|---|---|---:|---:|---:|---:|"]
    groups = {}
    for sample in samples:
        if "query" in sample:
            groups.setdefault((sample["backend"], sample["query"], sample["phase"]), []).append(sample)
    for (backend, query, phase), rows in groups.items():
        gets = sum(v["requests"] for row in rows for k, v in row["s3"].items() if k.split("/")[1] == "GET")
        size = sum(v["bytes"] for row in rows for v in row["s3"].values()) / 1024**2
        lines.append(f"| {backend} | {query} | {phase} | {len(rows)} | {statistics.median(r['ms'] for r in rows):.2f} | {gets} | {size:.2f} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("up", "run", "all"), default="all", nargs="?")
    parser.add_argument("--cluster", default="lake-cache")
    parser.add_argument("--directory", default=str(ROOT / ".tmp/cache-bench"))
    parser.add_argument("--cache-mib", type=int, default=512)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="1GB")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--backends", default="direct,proxy")
    parser.add_argument("--queries", default="CITY,BROAD,FULL,DEEP",
                        help="subset of DuckLake query shapes (SCAN pollution phase always runs)")
    parser.add_argument("--s3-latency-ms", type=int, default=25)
    parser.add_argument("--s3-latency-jitter-ms", type=int, default=5)
    parser.add_argument("--slice-bytes", type=int, default=1048576)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if args.repeats < 1 or not 1 <= args.cache_mib <= 1024 or args.s3_latency_ms < 0 or args.s3_latency_jitter_ms < 0 or args.slice_bytes < 65536 or any(b not in {"direct", "local", "proxy"} for b in args.backends.split(",")) or any(q not in {"CITY", "BROAD", "FULL", "DEEP"} for q in args.queries.split(",")):
        parser.error("positive repeats, 1..1024 MiB cache, non-negative latency, slice >= 64KiB, known backends and queries required")
    rig = Rig(args)
    if args.action in ("up", "all"):
        rig.up()
    if args.action in ("run", "all"):
        rig.run()


if __name__ == "__main__":
    main()
