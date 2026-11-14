#!/usr/bin/env python3
"""Explicit fake-Docker controls, plus real local unlinked mmap parser coverage.

Usage: python3 -I bench/container-test.py EXTERNAL_ARTIFACT_DIRECTORY
These tests are not evidence that a runtime image implements the pool.
"""

import importlib.util
import json
import mmap
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("container_start", Path(__file__).with_name("container-start.py"))
gate = importlib.util.module_from_spec(spec)
sys.dont_write_bytecode = True
spec.loader.exec_module(gate)
ARTIFACTS = None


def probe(directory, mode="600", uid="10001", mapped=True, target=None, inode="55", size=6_400_000):
    target = target or directory + "/#55 (deleted)"
    page = os.sysconf("SC_PAGE_SIZE")
    end = 0x1000 + ((size + page - 1) // page) * page
    mapping = f"1000-{end:x} rw-s 00000000 00:2a {inode} {target}\n" if mapped else ""
    root = gate.RUNTIME if directory == gate.RUNTIME + "/cooker" else directory
    return ("IDENTITY\n10001\n10001\nSTATUS\nUid:\t10001\t10001\t10001\t10001\n"
            "Gid:\t10001\t10001\t10001\t10001\nDIRECTORY\n700 10001 10001 directory\n"
            f"DATA\n700 10001 10001 directory\nFDS\nFD\t/proc/1/fd/9\t{target}\n"
            f"STAT 42 55 {mode} {uid} 10001 {size} regular file\nMAPS\n{mapping}"
            f"MOUNTS\n100 99 0:42 / {root} rw,nosuid,noexec - tmpfs tmpfs rw,size=524288k,uid=10001,gid=10001\n")


class FakeDocker:
    """Only constructed by this explicit test executable; never exposed by the driver CLI."""

    proof = "fake Docker control; NOT actual image proof"

    def __init__(self, defect=None):
        self.defect = defect
        self.context = None
        self.commands = []
        self.containers = {}
        self.scenarios = []

    def namespace(self, pid, *args):
        self.commands.append(("nsenter", str(pid), *args))
        rules = "-P OUTPUT DROP\n-A OUTPUT -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT\n"
        return subprocess.CompletedProcess(args, 0, rules, "")

    def run(self, *args, **kwargs):
        self.commands.append(args)
        output = ""
        if args[:2] == ("context", "show"):
            output = "rootless\n"
        elif args[:2] == ("container", "create"):
            name = args[args.index("--name") + 1]
            scenario = next(name_ for name_ in gate.SCENARIOS if name.endswith("-" + name_))
            self.containers[name] = {"name": scenario, "running": False}
            self.scenarios.append(scenario)
        elif args[:2] == ("container", "start"):
            container = self.containers[args[-1]]
            container["running"] = container["name"] not in ("missing-xdg", "unsafe-directory", "invalid-max-frames")
            if self.defect == "early-exit" and container["name"] == "default":
                container["running"] = False
            # prior-signal: an earlier SIGTERM is already draining the server, which then
            # exits 0 on its own before the gate's stop reaches it.
            container["prior_signal"] = self.defect == "prior-signal" and container["name"] == "default"
        elif args[:2] == ("container", "stop"):
            container = self.containers[args[-1]]
            # exit-before-stop: the server exited 0 after the last inspect, so the stop
            # signal never reached it; stopping a stopped container still succeeds.
            signalled = not (self.defect in ("exit-before-stop", "prior-signal") and container["name"] == "default")
            container.update(running=False, stopped=True, signalled=signalled)
        elif args[0] == "logs":
            scenario = self.containers[args[-1]]["name"]
            output = {"missing-xdg": "XDG_RUNTIME_DIR missing: configure stream.pool_directory",
                      "unsafe-directory": "/run/cooker-unsafe must be private mode 0700",
                      "invalid-max-frames": "stream.max_frames exceeds pool_frames 128"}.get(scenario, "started")
            for flag in ("prior_signal", "signalled"):
                if self.containers[args[-1]].get(flag):
                    output += "\n[info] " + gate.SIGNAL_LINE
        elif args[0] == "exec":
            if "sha256sum" in args:
                output = "a" * 64 + "  /proc/1/exe\n"
                container = self.containers[args[3]]
                if self.defect == "crash" and container["name"] == "default":
                    container.update(running=False, crashed=True)
            else:
                directory = args[-1]
                scenario = self.containers[args[3]]["name"]
                output = probe(directory, mapped=not (self.defect == "missing-pool" and scenario == "default"))
        return subprocess.CompletedProcess(args, 0, output, "")

    def json(self, *args, **kwargs):
        self.commands.append(args)
        if args[:2] == ("context", "inspect"):
            return [{"Endpoints": {"docker": {"Host": "unix:///run/user/1000/docker.sock"}}}]
        if args[0] == "info":
            return {"SecurityOptions": [] if self.defect == "rootful" else ["name=rootless"]}
        if args[:2] == ("image", "inspect"):
            return [{"Id": "sha256:" + "b" * 64, "Config": {
                "Env": ["XDG_RUNTIME_DIR=" + gate.RUNTIME], "User": "cooker",
                "Entrypoint": ["cooker"], "Cmd": None}}]
        if args[:2] == ("network", "inspect"):
            return [{"Internal": True, "Driver": "bridge"}]
        if "{{json .NetworkSettings.Ports}}" in args:
            if not self.containers[args[-1]]["running"]:
                return {"8317/tcp": None}
            return {"8317/tcp": [{"HostIp": "127.0.0.1", "HostPort": "12345"}]}
        if "{{json .State}}" in args:
            container = self.containers[args[-1]]
            return {"Running": container["running"], "OOMKilled": False, "Pid": 1234,
                    "ExitCode": 139 if container.get("crashed")
                    else 137 if self.defect == "forced-kill" and container.get("stopped")
                    else 0 if container.get("stopped") or container["running"] else 2}
        raise AssertionError(args)


class DriverControls(unittest.TestCase):
    def run_control(self, defect=None):
        out = Path(tempfile.mkdtemp(prefix=(defect or "pass") + "-", dir=ARTIFACTS))
        fake = FakeDocker(defect)
        health = (lambda _port: (200, '{"status":"ok"}'))
        if defect == "timeout":
            health = lambda _port: (200, '{"status":"wrong"}')
        instance = gate.Gate("explicit-fake-image", out, docker=fake, health=health, startup_timeout=0.01)
        with patch.dict(os.environ, {}, clear=True), patch.object(gate.time, "sleep"):
            result = instance.run()
        summary = json.loads((out / "summary.json").read_text())
        return result, summary, fake, out

    def test_pass_control_exercises_all_scenarios_and_own_cleanup(self):
        result, summary, fake, out = self.run_control()
        self.assertEqual(result, 0)
        self.assertTrue(summary["passed"])
        self.assertEqual(fake.scenarios, list(gate.SCENARIOS))
        self.assertFalse(summary["resources_remaining"])
        self.assertEqual(json.loads((out / "metadata.json").read_text())["proof"], fake.proof)
        creates = [command for command in fake.commands if command[:2] == ("container", "create")]
        default = creates[0]
        self.assertNotIn("--entrypoint", default)
        self.assertNotIn("--env", default)
        self.assertEqual(default[-1], "sha256:" + "b" * 64)
        self.assertIn(gate.TMPFS, default)
        for command in fake.commands:
            self.assertNotIn("prune", command)
            if len(command) > 1 and command[1] == "rm":
                self.assertTrue(command[-1].startswith("cooker-container-bench-"))

    def test_missing_pool_fails_but_keeps_later_scenarios(self):
        result, summary, fake, _ = self.run_control("missing-pool")
        self.assertEqual(result, 1)
        self.assertIn("actual mapped pool", " ".join(summary["scenarios"][0]["errors"]))
        self.assertEqual(fake.scenarios, list(gate.SCENARIOS))
        self.assertTrue(summary["scenarios"][0]["checks"]["clean_shutdown"])

    def test_early_exit_is_failure_not_startup_proof(self):
        result, summary, fake, _ = self.run_control("early-exit")
        self.assertEqual(result, 1)
        self.assertIn("server exited before health", summary["scenarios"][0]["errors"])
        self.assertEqual(fake.scenarios, list(gate.SCENARIOS))

    def test_timeout_does_not_accept_wrong_health_json(self):
        result, summary, fake, _ = self.run_control("timeout")
        self.assertEqual(result, 1)
        self.assertIn("health timed out", summary["scenarios"][0]["errors"])
        self.assertEqual(fake.scenarios, list(gate.SCENARIOS))

    def test_rootful_daemon_is_refused_before_creating_resources(self):
        result, summary, fake, _ = self.run_control("rootful")
        self.assertEqual(result, 1)
        self.assertIn("rootful", " ".join(summary["fatal_errors"]))
        self.assertFalse(any(len(command) > 1 and command[1] in ("create", "run") for command in fake.commands))
        self.assertEqual({row["scenario"] for row in summary["scenarios"]}, set(gate.SCENARIOS))

    def test_forced_shutdown_fails(self):
        result, summary, _, _ = self.run_control("forced-kill")
        self.assertEqual(result, 1)
        self.assertFalse(summary["scenarios"][0]["checks"]["clean_shutdown"])

    def test_server_exit_after_health_fails(self):
        result, summary, fake, _ = self.run_control("crash")
        self.assertEqual(result, 1)
        default = summary["scenarios"][0]
        self.assertIn("server exited before shutdown, ExitCode 139", default["errors"])
        self.assertFalse(default["checks"]["clean_shutdown"])
        self.assertEqual(fake.scenarios, list(gate.SCENARIOS))

    def test_server_exit_between_inspect_and_stop_fails(self):
        result, summary, fake, _ = self.run_control("exit-before-stop")
        self.assertEqual(result, 1)
        default = summary["scenarios"][0]
        self.assertIn(f"server exited without logging {gate.SIGNAL_LINE!r} exactly once; "
                      "it did not stop on the gate's SIGTERM", default["errors"])
        self.assertFalse(default["checks"]["clean_shutdown"])
        self.assertEqual(default["shutdown_state"]["ExitCode"], 0)
        self.assertTrue(all(row["passed"] for row in summary["scenarios"][1:]))

    def test_signal_logged_before_the_gate_stop_fails(self):
        # Review repro: an earlier SIGTERM line, no gate signal delivered, exit 0.
        result, summary, fake, _ = self.run_control("prior-signal")
        self.assertEqual(result, 1)
        default = summary["scenarios"][0]
        self.assertEqual(default["errors"], ["server logged 'shutdown signal received:' before the gate's stop; "
                                             "it was already shutting down"])
        self.assertFalse(default["checks"]["clean_shutdown"])
        self.assertEqual(default["shutdown_state"]["ExitCode"], 0)
        name = next(name for name, value in fake.containers.items() if value["name"] == "default")
        stop = fake.commands.index(("container", "stop", "--time", "10", name))
        self.assertIn(("logs", name), fake.commands[:stop])
        self.assertTrue(all(row["passed"] for row in summary["scenarios"][1:]))

    def test_interruption_stops_new_scenarios_and_cleans_own_resources(self):
        out = Path(tempfile.mkdtemp(prefix="interrupt-", dir=ARTIFACTS))
        fake = FakeDocker()
        instance = gate.Gate("explicit-fake-image", out, docker=fake,
                             health=lambda _port: (_ for _ in ()).throw(gate.Interrupted("test signal")))
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(instance.run(), 1)
        summary = json.loads((out / "summary.json").read_text())
        self.assertIn("test signal", summary["fatal_errors"])
        self.assertFalse(summary["resources_remaining"])
        self.assertEqual(fake.scenarios, ["default"])
        self.assertTrue(summary["scenarios"][0]["checks"]["clean_shutdown"])


class DockerDeadlineControls(unittest.TestCase):
    def test_default_deadline_rejects_a_stalled_docker_command(self):
        out = Path(tempfile.mkdtemp(prefix="docker-deadline-", dir=ARTIFACTS))
        docker = gate.Docker(out)
        def stalled(command, **kwargs):
            timeout = kwargs["timeout"]
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, 35)
            raise subprocess.TimeoutExpired(command, timeout)
        with patch.object(gate.subprocess, "run", side_effect=stalled):
            with self.assertRaisesRegex(gate.GateError, "timed out"):
                docker.run("info")
            with self.assertRaisesRegex(gate.GateError, "timed out"):
                docker.namespace(1234, "iptables", "-S", "OUTPUT")
        self.assertIsNone(json.loads((out / "docker-0001.json").read_text())["exit_code"])


class PoolControls(unittest.TestCase):
    directory = gate.RUNTIME + "/cooker"

    def test_unlinked_pool_is_found_by_descriptor_and_mapping(self):
        errors, pools = gate.check_pool(probe(self.directory), self.directory)
        self.assertEqual(errors, [])
        self.assertEqual(pools[0]["target"], self.directory + "/#55 (deleted)")

    def test_duplicate_descriptors_of_same_inode_are_one_pool(self):
        text = probe(self.directory)
        descriptor = (f"FD\t/proc/1/fd/10\t{self.directory}/#55 (deleted)\n"
                      "STAT 42 55 600 10001 10001 6400000 regular file\n")
        errors, pools = gate.check_pool(text.replace("MAPS\n", descriptor + "MAPS\n"), self.directory)
        self.assertEqual(errors, [])
        self.assertEqual(len(pools), 1)

    def test_filename_without_mapping_is_not_pool_proof(self):
        errors, pools = gate.check_pool(probe(self.directory, mapped=False), self.directory)
        self.assertTrue(errors)
        self.assertFalse(pools)

    def test_wrong_inode_mapping_is_not_pool_proof(self):
        errors, pools = gate.check_pool(probe(self.directory, inode="56"), self.directory)
        self.assertTrue(errors)
        self.assertFalse(pools)

    def test_wrong_permissions_and_ownership_fail(self):
        errors, pools = gate.check_pool(probe(self.directory, mode="644", uid="0"), self.directory)
        self.assertTrue(pools)
        self.assertIn("0600 owned by 10001:10001", " ".join(errors))

    def test_small_pool_requires_exact_configured_backing_size(self):
        for size in (gate.POOL_BYTES, gate.POOL_BYTES - 50_000, gate.POOL_BYTES + 50_000):
            with self.subTest(size=size):
                errors, _ = gate.check_pool(probe(self.directory, size=size), self.directory, gate.POOL_BYTES)
                self.assertEqual(not errors, size == gate.POOL_BYTES)
                if size != gate.POOL_BYTES:
                    self.assertIn("must equal pool_bytes", " ".join(errors))

    def test_small_pool_rejects_partial_or_oversized_virtual_mapping(self):
        text = probe(self.directory)
        for end in (0x2000, 0x1000 + gate.POOL_BYTES + 2 * os.sysconf("SC_PAGE_SIZE")):
            mapped = text.split("MAPS\n")[1].splitlines()[0]
            wrong = mapped.replace(mapped.split()[0], f"1000-{end:x}", 1)
            errors, _ = gate.check_pool(text.replace(mapped, wrong), self.directory, gate.POOL_BYTES)
            self.assertIn("page-rounded bytes", " ".join(errors))

    def test_refusal_requires_actionable_error_not_crash(self):
        state = {"ExitCode": 2, "OOMKilled": False}
        self.assertTrue(gate.refusal_errors("missing-xdg", state, "database failed"))
        self.assertFalse(gate.refusal_errors("missing-xdg", state,
                                            "XDG_RUNTIME_DIR unset; configure stream.pool_directory"))
        self.assertTrue(gate.refusal_errors("missing-xdg", {"ExitCode": 137},
                                           "XDG_RUNTIME_DIR unset; configure stream.pool_directory"))

    def test_real_otmpfile_mapping_is_discovered_without_pathname(self):
        with tempfile.TemporaryDirectory(prefix="real-otmpfile-", dir=ARTIFACTS) as directory:
            fd = os.open(directory, os.O_TMPFILE | os.O_RDWR, 0o600)
            try:
                os.ftruncate(fd, 50_000)
                with mmap.mmap(fd, 50_000, access=mmap.ACCESS_WRITE) as mapping:
                    mapping[0] = 42
                    stat = os.fstat(fd)
                    target = os.readlink(f"/proc/self/fd/{fd}")
                    text = probe(directory, target=target)
                    # Preserve the actual host ownership: this is parser proof, not uid-10001 image proof.
                    text = text.replace("STAT 42 55 600 10001 10001 6400000 regular file",
                                        f"STAT {stat.st_dev} {stat.st_ino} 600 {stat.st_uid} {stat.st_gid} {stat.st_size} regular file")
                    maps = Path("/proc/self/maps").read_text()
                    text = text.split("MAPS\n")[0] + "MAPS\n" + maps + "MOUNTS\n"
                    errors, pools = gate.check_pool(text, directory)
                    self.assertEqual(pools[0]["inode"], stat.st_ino)
                    self.assertEqual(pools[0]["mode"], "600")
                    self.assertIn("(deleted)", pools[0]["target"])
                    self.assertTrue(errors)  # Host mmap is deliberately not container startup proof.
                    self.assertFalse(Path(target.removesuffix(" (deleted)")).exists())
            finally:
                os.close(fd)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python3 -I bench/container-test.py EXTERNAL_ARTIFACT_DIRECTORY")
    ARTIFACTS = Path(sys.argv.pop()).resolve()
    repo = Path(__file__).resolve().parent.parent
    if ARTIFACTS == repo or repo in ARTIFACTS.parents:
        raise SystemExit("test artifacts must be outside repository")
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    unittest.main(verbosity=2)
