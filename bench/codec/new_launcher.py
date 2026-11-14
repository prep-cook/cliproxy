#!/usr/bin/env python3
"""Launch the new codec harness: one disposable process per workload in a private cgroup.

Cgroup handling is baseline_launcher.worker, unchanged, so both sides share one accounting path.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("baseline_launcher", HERE / "baseline_launcher.py")
baseline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(baseline)


def pool_directory(provenance):
    """The pool directory is the reported storage directory; recreate it private after a reboot."""
    path = Path(json.loads(provenance.read_text())["backend_config"]["pool_directory"])
    path.mkdir(mode=0o700, exist_ok=True)
    if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o7777 != 0o700:
        raise ValueError(f"pool directory {path} must be owned by this user with mode 0700")
    return path


def main():
    provenance = Path(os.environ["CODEC_NEW_PROVENANCE"])
    binary = provenance.parent / "new-codec-harness"
    pool = pool_directory(provenance)
    if sys.argv[1:] == ["--describe"]:
        os.execv(binary, [str(binary), "--describe"])
    if len(sys.argv) != 5 or sys.argv[1] != "--run" or sys.argv[3] != "--round":
        raise ValueError("usage: new-harness --describe | --run MANIFEST --round N")
    raw = Path(sys.argv[2]).read_bytes()
    manifest = json.loads(raw)
    evidence = Path(os.environ.get("CODEC_EVIDENCE_DIR", provenance.parent / f"evidence-{os.getpid()}"))
    evidence.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "CODEC_EVIDENCE_DIR": str(evidence), "CODEC_POOL_DIRECTORY": str(pool),
           "CODEC_MANIFEST_SHA256": hashlib.sha256(raw).hexdigest()}

    def interrupt(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGALRM, interrupt)
    signal.alarm(550)
    with tempfile.TemporaryDirectory(prefix="workload-", dir=evidence) as temporary:
        path = Path(temporary) / "workload.json"
        for index, row in enumerate(manifest["workloads"]):
            path.write_text(json.dumps({**manifest, "workloads": [row]}))
            code = baseline.worker(binary, ["--run", str(path), "--round", sys.argv[4]], env, index)
            if code:
                return code
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError) as error:
        print(f"new codec accounting: {error}", file=sys.stderr)
        sys.exit(2)
