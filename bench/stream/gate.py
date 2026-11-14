#!/usr/bin/env python3
"""Run every production storage row; preserve failures and reject missing evidence."""
import argparse
import hashlib
import json
import os
import math
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time

ROWS = [(f"small-{n}", 7) for n in (1, 64)]
ROWS += [(f"concurrent-{n}", 7) for n in (256, 1024)]
ROWS += [(f"throughput-{r}-{c}-{n}", 101 if (r, c, n) == (4, 64, 1) else 7)
         for r in (1, 4) for c in (64, 49152) for n in (1, 32, 250)]
ROWS += [(f"latency-{r}", 7) for r in (1, 4)]
ROWS += [("memory-100-1", 7), ("memory-10-64", 7)]
ROWS += [("totalcap", 1), ("enospc", 1)]
HIGHER = {"mb_s", "req_s"}
METRICS = {"small": ("req_s", "p50_ns", "p99_ns"),
           "concurrent": ("wall_ns", "ttfb_p99_ns"),
           "throughput": ("mb_s",),
           "latency": ("p50_ns", "p99_ns", "ttfb_p50_ns", "ttfb_p99_ns")}


def quantile(values, fraction):
    ordered = sorted(values)
    index = (len(ordered)-1)*fraction
    lo = int(index)
    hi = min(lo+1, len(ordered)-1)
    return ordered[lo]+(ordered[hi]-ordered[lo])*(index-lo)


def distribution(values):
    return {"min": min(values), "p25": quantile(values, .25),
            "median": statistics.median(values), "p75": quantile(values, .75),
            "max": max(values)}


def evaluate(row, runs, expected):
    result = {"row": row, "status": "pass", "failures": [], "metrics": {}}
    def fail(reason):
        result["status"] = "fail"
        result["failures"].append(reason)
    kinds = ("pool",) if row in ("enospc", "totalcap") else ("pool", "heap")
    samples = {}
    for kind in kinds:
        found = [run for run in runs if run.get("backend") == kind]
        if len(found) != expected:
            fail(f"{kind}: expected {expected} runs, found {len(found)}")
        good = [run for run in found if run.get("status") == "ok"]
        if len(good) != expected:
            fail(f"{kind}: {len(good)}/{expected} successful runs")
        samples[kind] = good
    try:
        group = row.split("-")[0]
        if group in METRICS and any(run.get("warmup") is not True for kind in kinds for run in samples[kind]):
            fail("sample without the untimed same-process warmup")
        for metric in METRICS.get(group, ()):
            if any(not isinstance(run["metrics"][metric], (int, float)) or not math.isfinite(run["metrics"][metric]) or run["metrics"][metric] <= 0
                   for kind in kinds for run in samples[kind]):
                raise ValueError(f"nonpositive or nonfinite {metric}")
            values = {kind: distribution([run["metrics"][metric] for run in samples[kind]])
                      for kind in kinds if samples[kind]}
            result["metrics"][metric] = values
            if any(not samples[kind] for kind in kinds):
                continue
            pool, heap = values["pool"], values["heap"]
            worse = pool["median"] < heap["min"] if metric in HIGHER else pool["median"] > heap["max"]
            if worse:
                fail(f"{metric}: pool median outside heap min/max in regression direction")
            if row == "throughput-4-64-1":
                if pool["median"] < heap["p25"]:
                    fail("pool median below heap P25")
                values["pool_vs_heap_median_pct"] = (pool["median"]/heap["median"]-1)*100
            result["metrics"][metric] = values
        if group in METRICS and all(samples[kind] for kind in kinds):
            # Diagnostic only: first-touch cost left inside each measured pass.
            result["metrics"]["minor_faults"] = {kind: distribution([run["metrics"]["minor_faults"] for run in samples[kind]])
                                                 for kind in kinds}
        if any(not samples[kind] for kind in kinds):
            return result
        if group == "memory":
            for kind in kinds:
                for run in samples[kind]:
                    metrics = run["metrics"]
                    if metrics["workers"] != 8 or not 0 < metrics["release_sample_ms"] <= 2000:
                        fail(f"{kind}: invalid worker count or decay timing")
                    if kind == "pool":
                        for metric in ("VmRSS", "tmpfs_used"):
                            if metrics["after"][metric] > metrics["baseline"][metric]+4*1024*1024+3_200_000:
                                fail(f"pool {metric}: post-decay baseline allowance exceeded")
            for phase in ("baseline", "held", "after"):
                result["metrics"][phase] = {
                    metric: {kind: distribution([run["metrics"][phase][metric] for run in samples[kind]]) for kind in kinds}
                    for metric in ("VmRSS", "RssAnon", "RssShmem", "RssFile", "tmpfs_used", "cgroup_current", "cgroup_anon", "cgroup_shmem")}
            if result["metrics"]["after"]["VmRSS"]["pool"]["median"] > result["metrics"]["after"]["VmRSS"]["heap"]["median"]:
                fail("post-decay pool VmRSS median exceeds heap")
        if row == "totalcap":
            metrics = samples["pool"][0]["metrics"]
            counts = metrics["stream_frames"]
            if len(counts) != 17 or counts != [5368]*16+[11] or not metrics["survived"] or len(metrics["errors"]) != 17:
                fail("invalid total-cap evidence")
            result["metrics"] = metrics
        if row == "enospc":
            metrics = samples["pool"][0]["metrics"]
            if metrics["errno"] != 28 or not metrics["survived"] or "No space left" not in metrics["error"]:
                fail("missing real ENOSPC and surviving-process evidence")
            result["metrics"] = metrics
    except (KeyError, TypeError, ValueError, ZeroDivisionError) as error:
        fail(f"missing or invalid metric: {error}")
    return result


def command_output(command):
    try:
        run = subprocess.run(command, text=True, capture_output=True, timeout=30)
        return {"command": command, "exit": run.returncode, "stdout": run.stdout, "stderr": run.stderr}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"command": command, "error": str(error)}


def provenance(root, tmpfs, binary):
    paths = list((root/"bench/stream").rglob("*.rs"))+list((root/"crates/ck-stream").rglob("*.rs"))
    paths += [root/"bench/stream/Cargo.toml", root/"bench/stream/Cargo.lock", root/"bench/stream/gate.py", root/"bench/stream-gate.sh", binary]
    return {"timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "host": platform.node(), "kernel": platform.platform(), "architecture": platform.machine(),
            "page_size": os.sysconf("SC_PAGE_SIZE"), "affinity": sorted(os.sched_getaffinity(0)),
            "load": os.getloadavg(), "workers": 8, "frame_bytes": 50000,
            "rustflags": os.environ.get("RUSTFLAGS", ""), "cargo_target_dir": os.environ.get("CARGO_TARGET_DIR"),
            "build": "cargo build --release --locked --offline --manifest-path bench/stream/Cargo.toml; thin LTO; codegen-units=1; default features",
            "revision": command_output(["git", "-C", str(root), "rev-parse", "HEAD"]),
            "rustc": command_output(["rustc", "-Vv"]), "cargo": command_output(["cargo", "-V"]),
            "filesystem": command_output(["findmnt", "-T", str(tmpfs), "-o", "TARGET,FSTYPE,OPTIONS,SIZE,AVAIL"]),
            "cgroup": Path("/proc/self/cgroup").read_text(),
            "hashes": {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths if path.is_file()},
            "notes": ["Fresh process per sample; alternating backend order on successive pairs.",
                      "Small requests expire in untimed batches of at most 2048; response delivery never releases bytes.",
                      "49152-byte records are frame-aligned in both backends; other appends are packed.",
                      "Heap: S0 shape; uninitialised 50000-byte frames from the system allocator, lock-free committed reads (Release/Acquire record count), locks only for frame-table growth, run-index updates and pointer lookup; Notify wakeups; payloads retained until expiry.",
                      "Comparison rows run one identical untimed warmup pass and expiry in the same process for both backends; minor_faults counts the measured pass.",
                      "Payload bytes follow a per-stream, per-offset pattern; every reader compares every byte.",
                      "No production event, metadata, stream, header or pool quota is increased.",
                      "cgroup and tmpfs measurements are shared-host charges and may include neighbours."]}


def check_out(root, out):
    """Refuse repository paths and old evidence; the wrapper may already have written build.log."""
    if out == root or root in out.parents:
        raise ValueError("OUT must be outside the repository")
    if out.exists() and any(path.name != "build.log" for path in out.iterdir()):
        raise ValueError("OUT must be empty; raw evidence is never overwritten")


def self_test():
    row = "throughput-4-64-1"
    good = [{"backend": k, "status": "ok", "warmup": True, "metrics": {"mb_s": v, "minor_faults": 0}}
            for k, v in (("heap", 100), ("pool", 100))]
    assert evaluate(row, good, 1)["status"] == "pass"
    assert evaluate(row, good[:1], 1)["status"] == "fail"
    assert evaluate(row, good[:1], 1)["metrics"]["mb_s"]["heap"]["median"] == 100
    assert evaluate(row, [dict(run, warmup=False) for run in good], 1)["status"] == "fail"
    assert evaluate(row, [{k: v for k, v in run.items() if k != "warmup"} for run in good], 1)["status"] == "fail"
    bad = [dict(run, metrics={"mb_s": 99, "minor_faults": 0}) if run["backend"] == "pool" else run for run in good]
    assert evaluate(row, bad, 1)["status"] == "fail"
    assert evaluate(row, [dict(run, status="error") for run in good], 1)["status"] == "fail"
    assert evaluate(row, [dict(run, metrics={}) for run in good], 1)["status"] == "fail"
    good_latency = [{"backend": k, "status": "ok", "warmup": True, "metrics": {"wall_ns": v, "ttfb_p99_ns": v, "minor_faults": 0}}
                    for k, v in (("heap", 100), ("pool", 101))]
    assert evaluate("concurrent-256",good_latency,1)["status"] == "fail"
    spread = [{"backend": k, "status": "ok", "warmup": True, "metrics": {"mb_s": v, "minor_faults": 0}}
              for k, values in (("heap", [50, 100, 110, 120]), ("pool", [80]*4)) for v in values]
    strict = evaluate(row, spread, 4)
    assert "pool median below heap P25" in strict["failures"]
    cap = {"stream_frames": [5368]*16+[11], "survived": True, "errors": ["cap"]*17}
    assert evaluate("totalcap", [{"backend": "pool", "status": "ok", "metrics": cap}], 1)["status"] == "pass"
    for short in (0, 10):
        leaked = dict(cap, stream_frames=[5368]*16+[short])
        assert evaluate("totalcap", [{"backend": "pool", "status": "ok", "metrics": leaked}], 1)["status"] == "fail"
    root = Path(__file__).resolve().parents[2]
    for bad_out in (root, root/"bench/out"):
        try:
            check_out(root, bad_out)
            raise AssertionError("repository OUT accepted")
        except ValueError:
            pass
    with tempfile.TemporaryDirectory() as directory:
        out = Path(directory)
        (out/"build.log").write_text("")
        check_out(root, out)
        (out/"raw.jsonl").write_text("")
        try:
            check_out(root, out)
            raise AssertionError("nonempty OUT accepted")
        except ValueError:
            pass
    print("gate self-test: planted regressions, failed runs and missing evidence rejected")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tmpfs", nargs="?")
    parser.add_argument("out", nargs="?")
    parser.add_argument("--binary")
    parser.add_argument("--build-failed", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.tmpfs or not args.out or not args.binary:
        parser.error("TMPFS OUT --binary BINARY are required")
    tmpfs, out, binary = Path(args.tmpfs).resolve(), Path(args.out).resolve(), Path(args.binary).resolve()
    root = Path(__file__).resolve().parents[2]
    try:
        check_out(root, out)
    except ValueError as error:
        parser.error(str(error))
    out.mkdir(parents=True, exist_ok=True)
    info = provenance(root, tmpfs, binary)
    (out/"provenance.json").write_text(json.dumps(info, indent=2)+"\n")
    filesystem = command_output(["stat", "-f", "-c", "%T", str(tmpfs)])
    preflight = None
    if filesystem.get("exit") != 0 or filesystem.get("stdout", "").strip() != "tmpfs":
        preflight = "TMPFS must be an existing tmpfs directory"
    if args.build_failed or not binary.is_file():
        preflight = "benchmark build failed or binary missing; see build.log"
    summaries = []
    with (out/"raw.jsonl").open("w") as raw:
        for row, repetitions in ROWS:
            samples = []
            if preflight:
                raw.write(json.dumps({"row": row, "status": "error", "error": preflight, "measured": False})+"\n")
                summaries.append({"row":row,"status":"fail","failures":[preflight],"metrics":{}})
                continue
            kinds = ("pool",) if row in ("enospc", "totalcap") else ("pool", "heap")
            for repetition in range(repetitions):
                order = kinds if repetition % 2 == 0 else tuple(reversed(kinds))
                for kind in order:
                    with tempfile.TemporaryDirectory(prefix="ck-stream-gate-",dir=tmpfs) as directory:
                        command = [str(binary), kind, row, directory]
                        if row == "enospc":
                            command = ["unshare", "-Urm", "sh", "-ec", 'mount -t tmpfs -o size=64m,mode=0700 tmpfs "$1"; exec "$2" pool enospc "$1"', "sh", directory, str(binary)]
                        started = time.time()
                        try:
                            run = subprocess.run(command, text=True, capture_output=True, timeout=180)
                            (out/f"{row}-{repetition:03d}-{kind}.stderr").write_text(run.stderr)
                            lines = run.stdout.splitlines()
                            sample = json.loads(lines[-1]) if lines else {"status":"error","error":"missing JSON output"}
                            if run.returncode != 0:
                                sample["status"] = "error"
                                sample.setdefault("error", f"exit {run.returncode}")
                            sample.update(exit=run.returncode, stdout=run.stdout)
                        except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError) as error:
                            sample = {"status":"error","error":str(error)}
                        sample.update(backend=kind,row=row,pair=repetition,order=list(order),started=started,load=os.getloadavg(),command=command)
                        raw.write(json.dumps(sample)+"\n")
                        raw.flush()
                        samples.append(sample)
            summary = evaluate(row,samples,repetitions)
            summaries.append(summary)
            print(f"{row}: {summary['status']}",flush=True)
    report = {"status":"pass" if summaries and all(s["status"]=="pass" for s in summaries) and len(summaries)==len(ROWS) else "fail", "rows":summaries,"required_rows":len(ROWS)}
    (out/"medians.json").write_text(json.dumps(report,indent=2)+"\n")
    lines = ["# Storage gate results", "", f"Status: {report['status']}", "", "| Row | Result | Failure |", "| --- | --- | --- |"]
    lines += [f"| {s['row']} | {s['status']} | {'; '.join(s['failures'])} |" for s in summaries]
    lines += ["", "Raw samples: raw.jsonl. Distributions, medians and IQR: medians.json. Host, source and build provenance: provenance.json.", ""]
    (out/"RESULTS.md").write_text("\n".join(lines))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
