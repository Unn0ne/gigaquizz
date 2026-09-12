"""Metadata-only fixtures; never use a running Kafka lab or a network client."""
import contextlib
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("kafka_http_disk_test_target",
                                            Path(__file__).resolve().parents[1] / "kafka_http_disk.py")
DISK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DISK)


class KafkaHTTPDiskTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gq-kafka-disk-fixture-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.lab = self.root / ".local/kafka-lab"
        self.lab.mkdir(parents=True)
        (self.lab / ".gigaquizz-kafka-lab").write_bytes(DISK.MARKER)
        self.brokers = [f"127.0.0.1:{19091+i}" for i in range(1, 4)]
        metadata = {"lab_dir": str(self.lab), "replication_factor": 3,
                    "bootstrap_servers": ",".join(self.brokers),
                    "nodes": {str(i): {"node_id": i, "port": 19091+i} for i in range(1, 4)}}
        (self.lab / "metadata.json").write_text(json.dumps(metadata))
        self.topic = "gqlog_app_" + "01" * 16
        ends = datetime.now(timezone.utc) - timedelta(minutes=1)
        self.config = {"Brokers": self.brokers, "Topic": self.topic, "PollID": [1] * 16,
                       "Partitions": 2, "StartsAt": (ends-timedelta(minutes=1)).isoformat(),
                       "EndsAt": ends.isoformat(), "AllowRemoteBrokers": False}
        self.config_path = self.root / "journal-config.json"
        self.save_config()
        self.files = []
        for node in range(1, 4):
            for partition in range(2):
                selected = self.lab / f"node{node}/data/{self.topic}-{partition}"
                selected.mkdir(parents=True)
                for suffix, length in {".log": 101, ".index": 4096, ".timeindex": 8192,
                                       ".txnindex": 7, ".snapshot": 13}.items():
                    path = selected / ("00000000000000000000" + suffix)
                    with path.open("wb") as output:
                        output.truncate(length)
                    self.files.append(path)

    def save_config(self):
        self.config_path.write_text(json.dumps(self.config))
        self.config_path.chmod(0o600)

    def test_exact_three_replica_sums_and_no_identifiers_in_output(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = DISK.main(["--runtime-root", str(self.root),
                              "--kafka-config", str(self.config_path)])
        self.assertEqual(code, 0)
        report = json.loads(output.getvalue())
        self.assertEqual(report["replica_directories_present"], 6)
        self.assertEqual(report["files"], 30)
        self.assertEqual(report["logical_file_bytes"], 6 * (101+4096+8192+7+13))
        self.assertEqual(report["allocated_file_bytes"], sum(path.stat().st_blocks*512 for path in self.files))
        self.assertEqual(report["by_extension"][".log"]["logical_bytes"], 606)
        self.assertEqual(report["by_extension"][".timeindex"]["logical_bytes"], 49152)
        self.assertEqual(report["by_extension"]["other"]["logical_bytes"], 78)
        self.assertFalse(report["closed_barriers_verified"])
        self.assertNotIn(self.topic, output.getvalue())
        self.assertNotIn(str(self.root), output.getvalue())

    def test_unrelated_topic_is_not_enumerated(self):
        unrelated = self.lab / "node1/data/unrelated-topic-0"
        unrelated.symlink_to(self.root / "does-not-exist", target_is_directory=True)
        report = DISK.inspect(self.root, self.config_path)
        self.assertEqual(report["files"], 30)
        self.assertTrue(unrelated.is_symlink())

    def test_private_config_and_owned_marker_are_required(self):
        self.config_path.chmod(0o644)
        with self.assertRaises(DISK.InspectionError):
            DISK.inspect(self.root, self.config_path)
        self.config_path.chmod(0o600)
        (self.lab / ".gigaquizz-kafka-lab").write_text("unowned")
        with self.assertRaises(DISK.InspectionError):
            DISK.inspect(self.root, self.config_path)

    def test_wrong_topic_poll_remote_brokers_and_open_window_fail(self):
        original = dict(self.config)
        for field, value in [("Topic", "../unrelated"), ("PollID", [2] * 16),
                             ("Brokers", ["192.0.2.1:19092", *self.brokers[1:]]),
                             ("Partitions", 33), ("AllowRemoteBrokers", True)]:
            with self.subTest(field=field):
                self.config = {**original, field: value}
                self.save_config()
                with self.assertRaises(DISK.InspectionError):
                    DISK.inspect(self.root, self.config_path)
        starts = datetime.now(timezone.utc)
        self.config = {**original, "StartsAt": starts.isoformat(),
                       "EndsAt": (starts+timedelta(minutes=1)).isoformat()}
        self.save_config()
        with self.assertRaises(DISK.InspectionError):
            DISK.inspect(self.root, self.config_path)

    def test_symlinked_config_parent_and_selected_entry_are_rejected(self):
        alias = self.root / "root-alias"
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            DISK.inspect(self.root, alias / self.config_path.name)
        self.files[0].unlink()
        self.files[0].symlink_to(self.config_path)
        with self.assertRaises(DISK.InspectionError):
            DISK.inspect(self.root, self.config_path)

    def test_missing_replica_or_file_bound_produces_no_partial_report(self):
        with self.assertRaises(DISK.InspectionError):
            DISK.inspect(self.root, self.config_path, max_files=1)
        missing = self.lab / f"node3/data/{self.topic}-1"
        missing.rename(missing.with_name("preserved-but-not-selected"))
        output, errors = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = DISK.main(["--runtime-root", str(self.root),
                              "--kafka-config", str(self.config_path)])
        self.assertEqual(code, 1)
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn(self.topic, errors.getvalue())
        self.assertNotIn(str(self.root), errors.getvalue())


if __name__ == "__main__":
    unittest.main()
