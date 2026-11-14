#!/usr/bin/env python3
"""Render a codec gate or partial summary as Markdown: every metric of every workload.

usage: report.py OUT   (OUT holds summary.json and workloads.json from codec-gate.sh or partial.py)
"""
from decimal import Decimal
import json
from pathlib import Path
import sys


def percent(old, new):
    old, new = Decimal(old), Decimal(new)
    if old == 0:
        return "n/a" if new == 0 else "new>0"
    return f"{(new - old) * 100 / old:+.1f}%"


def failures(out, workload, label="new"):
    """New-side refusals and codec failures across warm rounds, from the harness evidence."""
    totals = {}
    for evidence in sorted(out.glob(f"{label}-[2-9]-evidence/{workload}.json")):
        detail = json.loads(evidence.read_text())
        for key in ("completions", "refused_operations", "ingress_failures", "egress_failures"):
            totals[key] = totals.get(key, 0) + detail.get(key, 0)
    return totals


def main():
    if len(sys.argv) != 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    out = Path(sys.argv[1])
    summary = json.loads(out.joinpath("summary.json").read_text())
    rows = {row["id"]: row for row in json.loads(out.joinpath("workloads.json").read_text())["workloads"]}
    by_workload = {}
    for comparison in summary.get("comparisons", []):
        by_workload.setdefault(comparison["workload_id"], []).append(comparison)
    print(f"Status: {summary['status']}; failed metrics: {summary.get('failed_metrics', 'n/a')}\n")
    order = sorted(by_workload, key=lambda key: (rows[key]["protocol"], rows[key]["direction"],
                                                  rows[key]["mode"], rows[key]["event_kinds"][0]))
    for key in order:
        row, comparisons = rows[key], by_workload[key]
        failed = [c["metric"] for c in comparisons if not c["pass"]]
        print(f"### {row['protocol']} {row['direction']} {row['mode']} `{row['event_kinds'][0]}`\n")
        print(f"Workload `{key[:12]}`, {len(row['cases'])} cases, role {row['role']}. "
              f"New-side warm rounds: {json.dumps(failures(out, key))}. "
              f"Failed: {', '.join(failed) or 'none'}.\n")
        print("| Metric | Unit | Old median | New median | Change | Rule | Pass |")
        print("| --- | --- | ---: | ---: | ---: | --- | --- |")
        for c in comparisons:
            rule = c["direction"] if c["regression_percent"] is None else f"{c['direction']} {c['regression_percent']}%"
            if "noise_band" in c:
                rule = f"band {c['noise_band']} B"
            print(f"| {c['metric']} | {c['unit']} | {c['old_median']} | {c['new_median']} | "
                  f"{percent(c['old_median'], c['new_median'])} | {rule} | {'yes' if c['pass'] else '**no**'} |")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
