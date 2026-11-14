#!/usr/bin/env python3
"""Synthetic gate self-test control; never a baseline measurement harness."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--describe", action="store_true")
    parser.add_argument("--run", type=Path)
    parser.add_argument("--round", type=int)
    args = parser.parse_args()
    label = os.environ.get("CODEC_FAKE_SIDE", "old")
    mode = os.environ.get("CODEC_FAKE_MODE", "pass")
    if args.describe:
        new_revision = label == "new" and mode != "baseline-revision"
        print(json.dumps({"schema_version": 1, "revision": "c" * 40 if new_revision else gate.BASELINE_REVISION,
                          "source_checksum": ("1" if label == "new" else "0") * 64, "compiler": "synthetic-selftest",
                          "target": "synthetic", "profile": "release", "features": [],
                          "build_flags": [], "config": {"synthetic": True},
                          "storage_directory": os.environ["CODEC_FAKE_STORAGE"]}))
        return 0
    data = json.loads(args.run.read_text())
    checksum = hashlib.sha256(args.run.read_bytes()).hexdigest()
    if mode == "missing" and label == "new":
        return 0
    for workload in data["workloads"]:
        metrics = {}
        for name, (unit, direction, tolerance) in gate.METRICS.items():
            value = 100
            if label == "new" and tolerance == 5:
                value = 95 if direction == "higher" else 105
            if name in {"pool_reserved", "pool_used", "rss_shmem", "mapped_resident", "metadata"}:
                value = 0 if label == "old" else 8
            # An outlier must not turn the median gate into a mean gate.
            if args.round == 3 and tolerance == 5 and direction == "lower":
                value = 10000 if label == "old" else 20000
            if label == "new" and mode == "regress":
                if name == "cpu_ns_per_event":
                    value = 106
                if name == "throughput":
                    value = 94
                if name == "heap_retained":
                    value = 101
            # A harness that emits nothing has structural zero latencies and output.
            if label == "new" and mode == "silent":
                if name in {"encoded_bytes_per_event", "output_chunks"} or name.startswith(
                        ("first_deliverable_", "inter_event_")):
                    value = 0
                if name == "no_output_completions":
                    value = 200
            metrics[name] = {"unit": unit, "value": value}
        print(json.dumps({"schema_version": 1, "workload_id": workload["id"],
                          "manifest_sha256": checksum, "round": args.round,
                          "metrics": metrics}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
