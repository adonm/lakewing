import json
import tempfile
import unittest
from pathlib import Path

from osb_run import TS_BASE, bulk_workload, corpus, search_workload


class WorkloadTest(unittest.TestCase):
    def test_corpus_is_deterministic_and_create_only(self):
        with tempfile.TemporaryDirectory() as temp:
            a, b = Path(temp) / "a.json", Path(temp) / "b.json"
            self.assertEqual(corpus(a, "otel-logs-v0_9", 100),
                             corpus(b, "otel-logs-v0_9", 100))
            lines = a.read_text().splitlines()
            self.assertEqual(len(lines), 200)
            for i in range(100):
                self.assertEqual(json.loads(lines[i * 2]),
                                 {"create": {"_index": "otel-logs-v0_9"}})
                self.assertEqual(json.loads(lines[i * 2 + 1])["timestamp_nanos"],
                                 TS_BASE + i)

    def test_workloads_share_index_and_explicit_client_counts(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            bulk_workload(path, "otel-logs-v0_9", 1000, 202058)
            ingest = json.loads((path / "workload.json").read_text())
            self.assertEqual(ingest["corpora"][0]["documents"][0]["document-count"], 1000)
            self.assertEqual(ingest["schedule"][0]["clients"], 1)
            search_workload(path, "otel-logs-v0_9", 1000, 400, 0.1)
            search = json.loads((path / "workload.json").read_text())
            self.assertEqual([task["clients"] for task in search["schedule"]], [1, 8])
            self.assertEqual(search["schedule"][0]["operation"]["body"]["query"]["bool"]
                             ["filter"][0]["range"]["timestamp_nanos"],
                             {"gte": TS_BASE, "lt": TS_BASE + 100})
            self.assertTrue((path / "workload.py").exists())


if __name__ == "__main__":
    unittest.main()
