"""Isolated runtime/source selection; the benchmark child is always mocked."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("profile_log_paths_test", Path(__file__).resolve().parents[1] / "profile_log.py")
PROFILE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROFILE)


class ProfilePathsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gq-profile-paths-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / "source worktree"
        self.runtime = self.root / "runtime root"
        (self.source / "bin").mkdir(parents=True)
        self.binary = self.source / "bin/framebench"
        self.binary.write_bytes(b"fixture only: never executed")
        self.binary.chmod(0o700)
        self.lab = self.runtime / ".local/kafka-lab"
        self.lab.mkdir(parents=True)
        (self.lab / ".gigaquizz-kafka-lab").write_text("gigaquizz native Kafka lab v1\n")
        (self.lab / "metadata.json").write_text(json.dumps({"lab_dir": str(self.lab), "version": "fixture", "java_version": "fixture", "nodes": {str(i): {"pid": 1000+i} for i in range(1,4)}}))
        ledgers = self.runtime / ".local/framebench"
        ledgers.mkdir()
        self.previous = ledgers / "retained.fixture"
        self.previous.write_bytes(b"preserve previous data")

    def test_default_stays_with_source_root(self):
        with patch.object(PROFILE, "SOURCE_ROOT", self.source):
            _, args = PROFILE.parse_arguments(["--label", "default", "--benchmark", "framebench"])
        self.assertEqual(args.runtime_root, self.source)

    def test_external_runtime_uses_source_binary_and_preserves_previous_artifacts(self):
        argv = ["profile_log.py", "--runtime-root", str(self.runtime), "--benchmark", "framebench", "--label", "fixture", "--", "-rate", "1000"]
        calls = []

        def child(command, **kwargs):
            calls.append((command, kwargs["cwd"]))
            json.dump({"errors": [], "workload": {"counts": {"planned_attempts": 0}}, "reconciliation": {"correct": True}}, kwargs["stdout"])
            kwargs["stdout"].flush()
            return types.SimpleNamespace(pid=9999, returncode=0, poll=lambda: 0)

        with patch.object(PROFILE, "SOURCE_ROOT", self.source), patch.object(PROFILE.sys, "argv", argv), \
             patch.object(PROFILE.subprocess, "Popen", side_effect=child), \
             patch.object(PROFILE.shutil, "disk_usage", return_value=types.SimpleNamespace(free=20<<30)), \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(PROFILE.main(), 0)
            before = (self.lab / "profiles/fixture.json").read_bytes()
            with self.assertRaises(SystemExit):
                PROFILE.main()
            self.assertEqual((self.lab / "profiles/fixture.json").read_bytes(), before)
        self.assertEqual(calls, [([str(self.binary), "-rate", "1000"], self.runtime)])
        self.assertEqual(self.previous.read_bytes(), b"preserve previous data")
        telemetry = json.loads((self.lab / "profiles/fixture_telemetry.json").read_text())
        self.assertEqual(telemetry["paths"]["binary"], str(self.binary))
        self.assertEqual(telemetry["paths"]["runtime_root"], str(self.runtime))
        self.assertEqual(telemetry["paths"]["lab"], str(self.lab))
        self.assertEqual(telemetry["inventory_before"]["client_ledgers"]["files"], 1)
        self.assertEqual(len(telemetry["binary_sha256"]), 64)

    def test_external_runtime_cannot_inject_faults(self):
        with patch.object(PROFILE, "SOURCE_ROOT", self.source), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                PROFILE.parse_arguments(["--runtime-root", str(self.runtime), "--label", "fault", "--fault-node", "1"])

    def test_wrong_marker_cannot_launch_child(self):
        (self.lab / ".gigaquizz-kafka-lab").write_text("not owned\n")
        argv = ["profile_log.py", "--runtime-root", str(self.runtime), "--benchmark", "framebench", "--label", "bad"]
        with patch.object(PROFILE, "SOURCE_ROOT", self.source), patch.object(PROFILE.sys, "argv", argv), \
             patch.object(PROFILE.subprocess, "Popen") as child, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                PROFILE.main()
            child.assert_not_called()

    def test_free_space_floor_cannot_launch_child(self):
        argv = ["profile_log.py", "--runtime-root", str(self.runtime), "--benchmark", "framebench", "--label", "lowdisk"]
        with patch.object(PROFILE, "SOURCE_ROOT", self.source), patch.object(PROFILE.sys, "argv", argv), \
             patch.object(PROFILE.shutil, "disk_usage", return_value=types.SimpleNamespace(free=(8<<30)-1)), \
             patch.object(PROFILE.subprocess, "Popen") as child, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                PROFILE.main()
            child.assert_not_called()


if __name__ == "__main__":
    unittest.main()
