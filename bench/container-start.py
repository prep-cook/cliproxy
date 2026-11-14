#!/usr/bin/env python3
"""Actual-image startup gate; fake Docker is confined to container-test.py."""

import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid


RUNTIME = "/run/cooker-runtime"
# Logged by cooker's shutdown_signal() when the stop signal arrives (crates/cooker/src/main.rs).
SIGNAL_PREFIX = "shutdown signal received: "
SIGNAL_LINE = SIGNAL_PREFIX + "SIGTERM"
TMPFS = f"{RUNTIME}:rw,noexec,nosuid,size=512m,mode=0700,uid=10001,gid=10001"
POOL_BYTES = 6_400_000
POOL_FRAMES = POOL_BYTES // 50_000
SCENARIOS = ("default", "missing-xdg", "unsafe-directory", "explicit-directory",
             "small-derived-pool", "invalid-max-frames")

# Inspect the server's descriptors, not a filename that O_TMPFILE need not create.
PROBE = r'''
set -eu
printf 'IDENTITY\n'
id -u; id -g
printf 'STATUS\n'
cat /proc/1/status
printf 'DIRECTORY\n'
stat -c '%a %u %g %F' "$1" || true
printf 'DATA\n'
stat -c '%a %u %g %F' /data
printf 'FDS\n'
for fd in /proc/1/fd/*; do
    target=$(readlink "$fd") || continue
    printf 'FD\t%s\t%s\n' "$fd" "$target"
    stat -L -c 'STAT %d %i %a %u %g %s %F' "$fd" || true
 done
printf 'MAPS\n'
cat /proc/1/maps
printf 'MOUNTS\n'
cat /proc/1/mountinfo
'''


class GateError(RuntimeError):
    pass


class Interrupted(BaseException):
    pass


def dump(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


class Docker:
    proof = "actual runtime image"

    def __init__(self, out):
        self.out = out
        self.sequence = 0
        self.context = None

    def run(self, *args, **kwargs):
        command = ["docker"]
        if self.context:
            command += ["--context", self.context]
        return self.execute(command + list(args), **kwargs)

    def namespace(self, pid, *args):
        # The host's /run/xtables.lock belongs to real root, not rootless uid 0.
        return self.execute(["env", f"XTABLES_LOCKFILE={self.out / 'xtables.lock'}",
                             "nsenter", "--target", str(pid), "--user", "--net", "--", *args])

    def execute(self, command, input_text=None, timeout=35, check=True, private=False):
        self.sequence += 1
        started = time.monotonic()
        try:
            result = subprocess.run(command, input=input_text, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            self.record(command, None, "", str(exc), started, private)
            raise GateError(f"command failed: {command[0]}: {exc}") from exc
        self.record(command, result.returncode, result.stdout, result.stderr,
                    started, private)
        if check and result.returncode:
            raise GateError(f"{command[0]} exited {result.returncode}: {result.stderr.strip()}")
        return result

    def record(self, command, code, stdout, stderr, started, private):
        record = {"command": command, "exit_code": code,
                  "seconds": time.monotonic() - started,
                  "stdout": "[image environment omitted]" if private else stdout,
                  "stderr": stderr}
        dump(self.out / f"docker-{self.sequence:04d}.json", record)

    def json(self, *args, **kwargs):
        return json.loads(self.run(*args, **kwargs).stdout)


def check_pool(probe, directory, cap=None):
    """Require a live regular FD and shared writable mapping of that same inode."""
    errors = []
    sections = {}
    section = None
    for line in probe.splitlines():
        if line in ("IDENTITY", "STATUS", "DIRECTORY", "DATA", "FDS", "MAPS", "MOUNTS"):
            section = line
            sections[section] = []
        elif section:
            sections[section].append(line)
    if sections.get("IDENTITY") != ["10001", "10001"]:
        errors.append("pool probe did not execute as uid/gid 10001")
    for field in ("Uid", "Gid"):
        values = re.search(rf"^{field}:\s+(.*)$", "\n".join(sections.get("STATUS", [])), re.M)
        if not values or values.group(1).split() != ["10001"] * 4:
            errors.append(f"server {field} is not exclusively 10001")
    for name in ("DIRECTORY", "DATA"):
        if sections.get(name) != ["700 10001 10001 directory"]:
            errors.append(f"{name.lower()} must be a real 0700 directory owned by 10001:10001")
    fds = []
    seen_inodes = set()
    pending = None
    for line in sections.get("FDS", []):
        if line.startswith("FD\t"):
            _, fd, target = line.split("\t", 2)
            pending = (fd, target)
        elif line.startswith("STAT ") and pending:
            fields = line.split(maxsplit=7)
            fd, target = pending
            pending = None
            if not target.startswith(directory.rstrip("/") + "/"):
                continue
            if len(fields) != 8 or fields[7] != "regular file":
                continue
            _, device, inode, mode, uid, gid, size, _ = fields
            dev = int(device)
            mapped = []
            for mapping in sections.get("MAPS", []):
                parts = mapping.split(maxsplit=5)
                if len(parts) < 6:
                    continue
                major, minor = (int(part, 16) for part in parts[3].split(":"))
                if (major, minor, parts[4]) == (os.major(dev), os.minor(dev), inode):
                    if parts[1] == "rw-s" and parts[5].startswith(directory.rstrip("/") + "/"):
                        start, end = (int(part, 16) for part in parts[0].split("-"))
                        mapped.append(end - start)
            if not mapped:
                continue
            if (dev, int(inode)) in seen_inodes:
                continue
            seen_inodes.add((dev, int(inode)))
            entry = {"fd": fd, "target": target, "device": dev, "inode": int(inode),
                     "mode": mode, "uid": int(uid), "gid": int(gid),
                     "bytes": int(size), "mapped_bytes": sum(mapped)}
            fds.append(entry)
            if (mode, uid, gid) != ("600", "10001", "10001"):
                errors.append(f"mapped pool {fd} must be 0600 owned by 10001:10001")
            if int(size) <= 0:
                errors.append("mapped pool is empty")
            if cap is not None and int(size) != cap:
                errors.append(f"small capped pool mapping must equal pool_bytes {cap}; found {size}")
            if cap is not None:
                page = os.sysconf("SC_PAGE_SIZE")
                expected_mapping = ((cap + page - 1) // page) * page
                if sum(mapped) != expected_mapping:
                    errors.append(f"small capped pool virtual mapping must cover exactly {expected_mapping} page-rounded bytes; found {sum(mapped)}")
    if len(fds) != 1:
        errors.append(f"expected one actual mapped pool inode under {directory}; found {len(fds)}")
    mounts = "\n".join(sections.get("MOUNTS", []))
    root = directory if directory != RUNTIME + "/cooker" else RUNTIME
    mount = next((line for line in mounts.splitlines() if line.split()[4] == root), "")
    if " - tmpfs " not in mount:
        errors.append(f"pool mount {root} is not tmpfs")
    elif not all(option in mount.replace(",", " ").split() for option in ("rw", "noexec", "nosuid")):
        errors.append("pool tmpfs is missing rw/noexec/nosuid")
    if not re.search(r"(?:,| )size=524288k(?:,| |$)", mount):
        errors.append("pool tmpfs is not exactly 512 MiB")
    return errors, fds


def refusal_errors(name, state, logs):
    if state.get("OOMKilled") or state.get("ExitCode", 0) == 0:
        return ["refusal requires a nonzero, non-OOM exit"]
    if state.get("ExitCode", 0) >= 128:
        return ["process died by signal rather than refusing configuration"]
    text = logs.lower()
    if name == "missing-xdg":
        valid = ("xdg_runtime_dir" in text and
                 re.search(r"pool[._ -]?directory", text) and
                 re.search(r"set|configur|specify|provide", text))
    elif name == "unsafe-directory":
        valid = ("/run/cooker-unsafe" in text and
                 re.search(r"unsafe|private|permission|0700|owner|symlink", text))
    else:
        valid = (re.search(r"max[._ -]?frames", text) and
                 re.search(r"pool|128|exceed|greater|limit", text))
    return [] if valid else ["exit lacks the required actionable configuration diagnostic"]


class Gate:
    def __init__(self, image, out, docker=None, health=None, startup_timeout=30):
        self.image = image
        self.out = out
        self.docker = docker or Docker(out)
        self.health = health or self.fetch_health
        self.startup_timeout = startup_timeout
        self.token = "cooker-container-bench-" + uuid.uuid4().hex
        self.resources = []
        self.rows = []
        self.metadata = {}
        self.image_config = {}

    @staticmethod
    def fetch_health(port):
        # Ignore host HTTP_PROXY settings: the only allowed request is loopback healthz.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f"http://127.0.0.1:{port}/healthz", timeout=1) as response:
            return response.status, response.read(4096).decode("utf-8")

    def preflight(self):
        context = self.docker.run("context", "show").stdout.strip()
        if not context:
            raise GateError("Docker has no selected context")
        self.docker.context = context
        details = self.docker.json("context", "inspect", context)
        endpoint = details[0]["Endpoints"]["docker"]["Host"]
        # A remote daemon cannot provide the host-loopback endpoint inspected below.
        if not endpoint.startswith("unix://"):
            raise GateError(f"local rootless Unix Docker endpoint required; got {endpoint}")
        if os.environ.get("DOCKER_HOST") and os.environ["DOCKER_HOST"] != endpoint:
            raise GateError("DOCKER_HOST differs from selected context; select the rootless context explicitly")
        info = self.docker.json("info", "--format", "{{json .}}")
        if "name=rootless" not in info.get("SecurityOptions", []):
            raise GateError("refusing rootful Docker: daemon SecurityOptions lacks name=rootless")
        image = self.docker.json("image", "inspect", self.image, private=True)[0]
        self.image_config = image["Config"]
        env = self.image_config.get("Env") or []
        sensitive = [item.split("=", 1)[0] for item in env
                     if re.search(r"token|secret|password|credential|api.?key", item.split("=", 1)[0], re.I)
                     and item.partition("=")[2]]
        if sensitive:
            raise GateError("image contains credential environment variables: " + ", ".join(sensitive))
        labels = self.image_config.get("Labels") or {}
        self.metadata = {
            "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "proof": self.docker.proof, "image": self.image, "image_id": image["Id"],
            "repo_digests": image.get("RepoDigests", []), "image_created": image.get("Created"),
            "revision": labels.get("org.opencontainers.image.revision"),
            "compiler": labels.get("cooker.compiler"), "profile": labels.get("cooker.profile"),
            "features": labels.get("cooker.features"), "build_flags": labels.get("cooker.build-flags"),
            "unavailable_build_metadata": "null fields were not supplied by the image; no build provenance inferred",
            "entrypoint": self.image_config.get("Entrypoint"), "cmd": self.image_config.get("Cmd"),
            "image_user": self.image_config.get("User"), "image_xdg": next(
                (item.partition("=")[2] for item in env if item.startswith("XDG_RUNTIME_DIR=")), None),
            "context": context, "endpoint": endpoint, "security_options": info["SecurityOptions"],
            "docker_version": info.get("ServerVersion"), "storage_driver": info.get("Driver"),
            "host": platform.node(), "architecture": platform.machine(), "kernel": platform.release(),
            "page_size": os.sysconf("SC_PAGE_SIZE"), "affinity": sorted(os.sched_getaffinity(0)),
            "load": os.getloadavg(), "tmpfs": TMPFS, "scenarios": list(SCENARIOS),
            "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "units": {"time": "seconds", "pool_size": "bytes", "mode": "octal"},
        }
        self.image = image["Id"]
        dump(self.out / "metadata.json", self.metadata)

    def create(self, kind, name, *args):
        # Reserve the unique name before creation, so a client timeout still gets cleanup.
        self.resources.append((kind, name))
        return self.docker.run(kind, "create", *args, name)

    def state(self, container):
        return self.docker.json("container", "inspect", "--format", "{{json .State}}", container)

    def expose_port(self, container, network):
        # Docker 29 does not publish internal-bridge ports. Keep the original
        # entrypoint: stop PID 1 on that bridge, install an outbound firewall in
        # only its network namespace, then attach the disposable port bridge.
        if not self.state(container)["Running"]:
            return None
        stopped = self.docker.run("container", "kill", "--signal", "STOP", container, check=False)
        if stopped.returncode:
            if not self.state(container)["Running"]:
                return None
            raise GateError("cannot stop own container before exposing the health port")
        try:
            pid = int(self.state(container)["Pid"])
            if pid <= 0:
                raise GateError("own container has no live namespace PID")
            self.docker.namespace(pid, "iptables", "--wait", "5", "-P", "OUTPUT", "DROP")
            self.docker.namespace(pid, "iptables", "--wait", "5", "-A", "OUTPUT", "-m", "conntrack",
                                  "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT")
            rules = self.docker.namespace(pid, "iptables", "--wait", "5", "-S", "OUTPUT").stdout
            if "-P OUTPUT DROP" not in rules or "--ctstate RELATED,ESTABLISHED -j ACCEPT" not in rules:
                raise GateError("outbound namespace firewall was not installed")
            self.docker.run("network", "connect", network, container)
            return rules
        finally:
            self.docker.run("container", "kill", "--signal", "CONT", container, check=False)

    def cleanup(self):
        errors = []
        remaining = []
        for kind, name in reversed(self.resources):
            args = (kind, "rm", "--force", name) if kind == "container" else (kind, "rm", name)
            try:
                result = self.docker.run(*args, check=False)
                if result.returncode and "No such" not in result.stderr:
                    errors.append(f"cannot remove own {kind} {name}: {result.stderr.strip()}")
                    remaining.append((kind, name))
            except GateError as exc:
                errors.append(str(exc))
                remaining.append((kind, name))
        self.resources = list(reversed(remaining))
        return errors

    def prepare(self, volume, config):
        helper = self.token + "-prepare-" + volume.rsplit("-", 1)[-1]
        self.resources.append(("container", helper))
        script = "set -eu; test -z \"$(ls -A /data)\"; chown 10001:10001 /data; chmod 0700 /data"
        if config:
            script += "; umask 077; cat > /data/config.yaml; chown 10001:10001 /data/config.yaml"
        self.docker.run("run", "--name", helper, "--rm", "-i", "--pull=never",
                        "--network", "none", "--user", "0:0", "--label", self.token,
                        "--mount", f"type=volume,src={volume},dst=/data,volume-nocopy",
                        "--entrypoint", "/bin/sh", self.image, "-c", script, input_text=config)
        self.resources.remove(("container", helper))

    def scenario(self, name, network, port_network):
        row = {"scenario": name, "round": 1, "errors": [], "checks": {}, "config": None}
        container = self.token + "-" + name
        volume = container + "-data"
        negative = name in ("missing-xdg", "unsafe-directory", "invalid-max-frames")
        directory = RUNTIME + "/cooker"
        config = ""
        extras = []
        if name in ("unsafe-directory", "explicit-directory"):
            directory = "/run/cooker-unsafe" if negative else "/run/cooker-private"
            mode = "0777" if negative else "0700"
            extras = ["--tmpfs", f"{directory}:rw,noexec,nosuid,size=512m,mode={mode},uid=10001,gid=10001"]
            config = f"stream:\n  pool_directory: {directory}\n"
        if name in ("small-derived-pool", "invalid-max-frames"):
            config = f"stream:\n  pool_bytes: {POOL_BYTES}\n"
            if negative:
                config += f"  max_frames: {POOL_FRAMES + 1}\n"
        row["config"] = config or None
        row["expected"] = "configuration refusal" if negative else "healthy mapped pool and clean shutdown"
        started = time.monotonic()
        try:
            self.create("volume", volume, "--label", self.token)
            self.prepare(volume, config)
            args = ["container", "create", "--name", container, "--pull=never", "--label", self.token,
                    "--network", network, "--publish", "127.0.0.1::8317",
                    "--mount", f"type=volume,src={volume},dst=/data,volume-nocopy", "--tmpfs", TMPFS]
            args += extras
            if name in ("missing-xdg", "explicit-directory"):
                entry = self.image_config.get("Entrypoint") or []
                cmd = self.image_config.get("Cmd") or []
                if not entry:
                    raise GateError("image has no executable entrypoint for absent-XDG scenario")
                args += ["--entrypoint", "/bin/sh", self.image, "-c",
                         'unset XDG_RUNTIME_DIR; exec "$@"', "container-start", *entry, *cmd]
            else:
                args += [self.image]
            self.resources.append(("container", container))
            self.docker.run(*args)
            self.docker.run("container", "start", container)
            row["outbound_firewall"] = self.expose_port(container, port_network)
            ports = self.docker.json("container", "inspect", "--format", "{{json .NetworkSettings.Ports}}", container)
            binding = ports.get("8317/tcp") or []
            # An expected refusal may exit before it can acquire a host binding.
            port = None
            if binding:
                if len(binding) != 1 or binding[0]["HostIp"] != "127.0.0.1":
                    raise GateError("runtime port is not exclusively mapped to host loopback")
                port = int(binding[0]["HostPort"])
            elif self.state(container)["Running"]:
                raise GateError("running runtime has no host-loopback port mapping")
            row["port"] = port
            deadline = time.monotonic() + self.startup_timeout
            health_attempts = []
            exited = False
            healthy = False
            while time.monotonic() < deadline:
                state = self.state(container)
                if not state["Running"]:
                    exited = True
                    row["exit_state"] = state
                    break
                try:
                    status, body = self.health(port)
                    health_attempts.append({"status": status, "body": body})
                    if status == 200 and json.loads(body) == {"status": "ok"}:
                        healthy = True
                        break
                except (OSError, ValueError, urllib.error.URLError) as exc:
                    health_attempts.append({"error": str(exc)})
                time.sleep(0.1)
            row["health_attempts"] = health_attempts
            # Docker may put application logs on either stream.
            log_result = self.docker.run("logs", container)
            logs = log_result.stdout + log_result.stderr
            (self.out / f"{name}.log").write_text(logs)
            if negative:
                if not exited:
                    row["errors"].append("configuration was accepted" if healthy else "configuration refusal timed out")
                else:
                    row["errors"] += refusal_errors(name, row["exit_state"], logs)
                row["checks"]["refusal"] = exited and not row["errors"]
            else:
                row["checks"]["health"] = healthy
                if not healthy:
                    row["errors"].append("server exited before health" if exited else "health timed out")
                else:
                    if name == "default":
                        row["checks"]["image_xdg"] = self.metadata.get("image_xdg") == RUNTIME
                        if not row["checks"]["image_xdg"]:
                            row["errors"].append(f"image must set XDG_RUNTIME_DIR={RUNTIME}; driver did not override it")
                    probe = self.docker.run("exec", "--user", "10001:10001", container,
                                            "/bin/sh", "-c", PROBE, "pool-probe", directory).stdout
                    (self.out / f"{name}.proc.txt").write_text(probe)
                    errors, pools = check_pool(probe, directory, POOL_BYTES if name == "small-derived-pool" else None)
                    row["checks"]["pool"] = not errors
                    row["pools"] = pools
                    row["errors"] += errors
                    checksum = self.docker.run("exec", "--user", "10001:10001", container,
                                               "sha256sum", "/proc/1/exe").stdout.split()[0]
                    if not re.fullmatch(r"[0-9a-f]{64}", checksum):
                        raise GateError("could not checksum actual server executable")
                    row["binary_sha256"] = checksum
        except Interrupted as exc:
            row["errors"].append(str(exc))
            raise
        except (GateError, KeyError, ValueError, IndexError) as exc:
            row["errors"].append(str(exc))
        finally:
            stopped = False
            prior_signals = 0
            try:
                state = self.state(container)
                stopped = state["Running"]
                if stopped:
                    before = self.docker.run("logs", container, check=False)
                    prior_signals = (before.stdout + before.stderr).count(SIGNAL_PREFIX)
                    self.docker.run("container", "stop", "--time", "10", container, timeout=15)
                    state = self.state(container)
                    row["shutdown_state"] = state
                    row["checks"]["clean_shutdown"] = (
                        not state["Running"] and state["ExitCode"] == 0 and not state.get("OOMKilled"))
                    if state["Running"] or state["ExitCode"] != 0 or state.get("OOMKilled"):
                        row["errors"].append("shutdown was not clean (exit 0 without OOM/forced kill required)")
                elif not negative:
                    # A server must still be running when the gate stops it; exiting on its own is a crash.
                    row["shutdown_state"] = state
                    row["checks"]["clean_shutdown"] = False
                    row["errors"].append(f"server exited before shutdown, ExitCode {state['ExitCode']}")
            except (GateError, KeyError, ValueError, IndexError) as exc:
                row["errors"].append(f"shutdown: {exc}")
            try:
                logs = self.docker.run("logs", container, check=False)
                (self.out / f"{name}.log").write_text(logs.stdout + logs.stderr)
                # Graceful SIGTERM shutdown and a server that exits 0 on its own between
                # the last inspect and the stop look alike; only the server's own log line
                # shows that the stop signal arrived before the exit. A line logged before
                # the stop belongs to another signal that was already draining the server,
                # so the gate requires none before the stop and exactly one SIGTERM line after.
                text = logs.stdout + logs.stderr
                if stopped and row["checks"].get("clean_shutdown") and prior_signals:
                    row["checks"]["clean_shutdown"] = False
                    row["errors"].append(f"server logged {SIGNAL_PREFIX.strip()!r} before the gate's stop; "
                                         "it was already shutting down")
                elif stopped and row["checks"].get("clean_shutdown") and not (
                        text.count(SIGNAL_PREFIX) == text.count(SIGNAL_LINE) == 1):
                    row["checks"]["clean_shutdown"] = False
                    row["errors"].append(f"server exited without logging {SIGNAL_LINE!r} exactly once; "
                                         "it did not stop on the gate's SIGTERM")
            except GateError as exc:
                row["errors"].append(f"logs: {exc}")
            row["seconds"] = time.monotonic() - started
            row["passed"] = not row["errors"]
            self.rows.append(row)
            dump(self.out / f"{name}.json", row)
            print(f"{'PASS' if row['passed'] else 'FAIL'} {name}: " + "; ".join(row["errors"]), flush=True)

    def run(self):
        fatal = []
        dump(self.out / "metadata.json", {"image": self.image, "proof": self.docker.proof,
                                          "phase": "preflight", "scenarios": list(SCENARIOS)})
        try:
            self.preflight()
            network = self.token + "-network"
            self.create("network", network, "--internal", "--label", self.token)
            details = self.docker.json("network", "inspect", network)[0]
            if not details.get("Internal") or details.get("Driver") != "bridge":
                raise GateError("Docker network is not an isolated internal bridge")
            port_network = self.token + "-ports"
            self.create("network", port_network, "--label", self.token)
            port_details = self.docker.json("network", "inspect", port_network)[0]
            if port_details.get("Driver") != "bridge" or port_details.get("EnableIPv6"):
                raise GateError("port network must be an IPv4-only disposable bridge")
            for name in SCENARIOS:
                self.scenario(name, network, port_network)
        except (Interrupted, GateError, KeyError, ValueError, IndexError) as exc:
            fatal.append(str(exc))
        finally:
            fatal += self.cleanup()
            for name in SCENARIOS:
                if not any(row["scenario"] == name for row in self.rows):
                    row = {"scenario": name, "passed": False, "errors": ["not run: " + "; ".join(fatal)]}
                    self.rows.append(row)
                    dump(self.out / f"{name}.json", row)
            summary = {"passed": not fatal and all(row["passed"] for row in self.rows),
                       "fatal_errors": fatal, "scenarios": self.rows,
                       "resources_remaining": self.resources}
            dump(self.out / "summary.json", summary)
            with (self.out / "raw.jsonl").open("w") as stream:
                for row in self.rows:
                    stream.write(json.dumps(row, sort_keys=True) + "\n")
            if fatal:
                print("FAIL: " + "; ".join(fatal), file=sys.stderr)
        return 0 if summary["passed"] else 1


def main():
    if len(sys.argv) != 3:
        print("usage: bench/container-start.sh IMAGE OUT", file=sys.stderr)
        return 2
    out = Path(sys.argv[2]).resolve()
    repo = Path(__file__).resolve().parent.parent
    if out == repo or repo in out.parents:
        print("OUT must be outside the repository", file=sys.stderr)
        return 2
    os.umask(0o077)
    try:
        out.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        print(f"cannot create fresh OUT directory: {exc}", file=sys.stderr)
        return 2
    gate = Gate(sys.argv[1], out)

    def interrupted(signum, _frame):
        raise Interrupted(f"interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    return gate.run()


if __name__ == "__main__":
    sys.exit(main())
