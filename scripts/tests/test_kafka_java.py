"""Java discovery checks with tiny fake executables, without launching Kafka/JVMs."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("kafka_lab_java_test", Path(__file__).resolve().parents[1] / "kafka_lab.py")
LAB = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LAB)


class JavaDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gq-java-discovery-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def java(self, name, version):
        home = self.root / name
        (home / "bin").mkdir(parents=True)
        executable = home / "bin/java"
        executable.write_text('#!/bin/sh\nprintf "%s\\n" \'openjdk version "' + version + '"\' >&2\n')
        executable.chmod(0o700)
        return home

    def test_explicit_home_is_used_before_any_discovery(self):
        home = self.java("explicit", "21.0.10")
        with patch.dict(os.environ, {"GIGAQUIZZ_KAFKA_JAVA_HOME": str(home)}, clear=True), \
             patch.object(LAB.sys, "platform", "linux"), \
             patch.object(LAB.shutil, "which", side_effect=AssertionError("unexpected PATH fallback")):
            found, version = LAB.java_home()
        self.assertEqual(found, str(home.resolve()))
        self.assertIn("21.0.10", version)

    def test_linux_path_symlink_finds_real_home_without_mac_helper(self):
        home = self.java("jdk", "25.0.1")
        link = self.root / "java"
        link.symlink_to(home / "bin/java")
        with patch.dict(os.environ, {}, clear=True), patch.object(LAB.sys, "platform", "linux"), \
             patch.object(LAB.shutil, "which", return_value=str(link)):
            found, _ = LAB.java_home()
        self.assertEqual(found, str(home.resolve()))

    def test_explicit_old_java_fails_without_silent_fallback(self):
        home = self.java("old", "17.0.1")
        with patch.dict(os.environ, {"GIGAQUIZZ_KAFKA_JAVA_HOME": str(home)}, clear=True), \
             patch.object(LAB.shutil, "which", side_effect=AssertionError("unexpected fallback")):
            with self.assertRaisesRegex(RuntimeError, "Java 21"):
                LAB.java_home()

    def test_linux_path_old_java_is_rejected(self):
        home = self.java("old-path", "17.0.1")
        with patch.dict(os.environ, {}, clear=True), patch.object(LAB.sys, "platform", "linux"), \
             patch.object(LAB.shutil, "which", return_value=str(home / "bin/java")):
            with self.assertRaisesRegex(RuntimeError, "JDK 21"):
                LAB.java_home()

    def test_no_java_returns_actionable_error_without_missing_helper_exception(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(LAB.sys, "platform", "linux"), \
             patch.object(LAB.shutil, "which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "GIGAQUIZZ_KAFKA_JAVA_HOME"):
                LAB.java_home()


if __name__ == "__main__":
    unittest.main()
