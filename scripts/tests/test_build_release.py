"""Release assembly fixtures; no Go build, service, or workload is executed."""
import argparse
from contextlib import ExitStack, redirect_stdout
from copy import deepcopy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('build_release', ROOT / 'scripts/build_release.py')
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class Fixture:
    def __init__(self, directory, dirty=False, fail_at=None, omit_at=None, changed=False, backend='simple-files'):
        self.root = Path(directory) / 'source repo'
        self.root.mkdir()
        (self.root / 'scripts').mkdir()
        (self.root / 'scripts/run_http_generators.py').write_text('# fixture runner\n')
        (self.root / '.env.example').write_text('ADMIN_PASSWORD=CHANGE_ME\n' +
                                              ('KAFKA_BROKERS=127.0.0.1:19092\n' if backend == 'postgres-kafka' else 'DATA_DIR=.local/files\n'))
        (self.root / '.env').write_text('DO_NOT_COPY_PRIVATE_SECRET')
        (self.root / '.local').mkdir()
        (self.root / '.local/private-data').write_text('DO_NOT_COPY_PRIVATE_DATA')
        self.output = Path(directory) / 'release with spaces'
        self.snapshot = dict(commit='a' * 40, branch=backend, dirty=dirty, status_sha256='b' * 64,
                             source_tree_sha256='c' * 64, source_files=3)
        self.fail_at, self.omit_at, self.changed = fail_at, omit_at, changed
        self.commands = []
        self.builds = 0

    def command(self, args, **kwargs):
        self.commands.append((args, kwargs))
        if args == ['go', 'version']:
            return argparse.Namespace(returncode=0, stdout=b'go version go1.26.1 fixture/fixture\n')
        self.builds += 1
        if self.builds == self.fail_at:
            return argparse.Namespace(returncode=1)
        if self.builds != self.omit_at:
            binary = Path(args[args.index('-o') + 1])
            binary.write_bytes(f'fixture executable {self.builds}'.encode())
            binary.chmod(0o700)
        return argparse.Namespace(returncode=0)

    def run(self, allow_dirty=False, targets=None):
        args = argparse.Namespace(output=str(self.output), target=targets, allow_dirty=allow_dirty)
        before = self.snapshot
        after = deepcopy(before)
        if self.changed:
            after['source_tree_sha256'] = 'd' * 64
        stdout = io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(patch.object(release, 'source_snapshot', side_effect=[before, after]))
            stack.enter_context(patch.object(release.subprocess, 'run', side_effect=self.command))
            stack.enter_context(redirect_stdout(stdout))
            code = release.build(args, self.root)
        return code, json.loads((self.output / 'release-report.json').read_text()), json.loads(stdout.getvalue())


class ReleaseTests(unittest.TestCase):
    def test_complete_default_release_has_all_targets_checked_inventory_and_no_private_data(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory)
            code, report, summary = fixture.run()
            self.assertEqual(code, 0)
            self.assertTrue(report['complete'])
            self.assertTrue(summary['complete'])
            self.assertEqual(fixture.builds, 4)
            expected = {'linux-amd64/gigaquizz', 'linux-amd64/httpbench', 'linux-arm64/gigaquizz',
                        'linux-arm64/httpbench', 'env.example', 'RUNNING.md', 'run_http_generators.py', 'source.json'}
            self.assertTrue(expected <= set(report['files']))
            self.assertFalse((fixture.output / '.env').exists())
            self.assertFalse((fixture.output / '.local').exists())
            self.assertNotIn('DO_NOT_COPY', ''.join(p.read_text() for p in fixture.output.rglob('*') if p.is_file()))
            for line in (fixture.output / 'SHA256SUMS').read_text().splitlines():
                digest, relative = line.split('  ', 1)
                self.assertEqual(digest, hashlib.sha256((fixture.output / relative).read_bytes()).hexdigest())
            self.assertEqual(fixture.output.stat().st_mode & 0o777, 0o700)
            self.assertEqual((fixture.output / 'env.example').stat().st_mode & 0o777, 0o600)

    def test_build_arguments_are_separate_and_override_ambient_go_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory)
            code, _, _ = fixture.run(targets=['darwin/arm64'])
            self.assertEqual(code, 0)
            for args, kwargs in fixture.commands[1:]:
                self.assertEqual(args[:4], ['go', 'build', '-trimpath', '-mod=readonly'])
                self.assertEqual(kwargs['env']['CGO_ENABLED'], '0')
                self.assertEqual(kwargs['env']['GOOS'], 'darwin')
                self.assertEqual(kwargs['env']['GOARCH'], 'arm64')
                self.assertEqual(kwargs['env']['GOFLAGS'], '')
                self.assertEqual(kwargs['env']['GOWORK'], 'off')
                self.assertEqual(kwargs['env']['GOAMD64'], 'v1')
                self.assertEqual(kwargs['env']['GOARM64'], 'v8.0')
                self.assertEqual(kwargs['cwd'], fixture.root.resolve())
                self.assertNotIn('shell', kwargs)
                self.assertIn('release with spaces', args[args.index('-o') + 1])

    def test_dirty_default_rejects_before_build_and_explicit_override_is_honest(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory, dirty=True)
            code, report, summary = fixture.run()
            self.assertEqual(code, 1)
            self.assertFalse(report['complete'])
            self.assertFalse(summary['complete'])
            self.assertEqual(fixture.commands, [])
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory, dirty=True)
            code, report, _ = fixture.run(allow_dirty=True)
            self.assertEqual(code, 0)
            source = json.loads((fixture.output / 'source.json').read_text())
            self.assertTrue(source['dirty'])
            self.assertTrue(source['allow_dirty'])
            self.assertEqual(source['source_tree_sha256'], fixture.snapshot['source_tree_sha256'])

    def test_failed_or_missing_build_output_stops_immediately_and_preserves_partial(self):
        for fault in ('fail_at', 'omit_at'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                fixture = Fixture(directory, **{fault: 2})
                code, report, summary = fixture.run()
                self.assertEqual(code, 1)
                self.assertFalse(report['complete'])
                self.assertFalse(summary['complete'])
                self.assertEqual(fixture.builds, 2)
                self.assertTrue((fixture.output / 'linux-amd64/gigaquizz').exists())
                self.assertFalse((fixture.output / 'SHA256SUMS').exists())
                self.assertFalse((fixture.output / 'linux-arm64').exists())

    def test_source_change_during_build_cannot_publish_success(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory, changed=True)
            code, report, _ = fixture.run()
            self.assertEqual(code, 1)
            self.assertFalse(report['complete'])
            self.assertFalse((fixture.output / 'SHA256SUMS').exists())

    def test_existing_output_and_symlink_parent_are_not_modified(self):
        for symlink in (False, True):
            with self.subTest(symlink=symlink), tempfile.TemporaryDirectory() as directory:
                fixture = Fixture(directory)
                if symlink:
                    parent = Path(directory) / 'link'
                    parent.symlink_to(fixture.root, target_is_directory=True)
                    fixture.output = parent / 'new-release'
                else:
                    fixture.output.mkdir()
                    (fixture.output / 'sentinel').write_text('preserve')
                with self.assertRaises((RuntimeError, FileExistsError)):
                    fixture.run()
                self.assertEqual(fixture.commands, [])
                if not symlink:
                    self.assertEqual((fixture.output / 'sentinel').read_text(), 'preserve')

    def test_pg_runtime_document_is_self_contained_and_does_not_promise_bundled_services(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory, backend='postgres-kafka')
            code, _, _ = fixture.run()
            self.assertEqual(code, 0)
            text = (fixture.output / 'RUNNING.md').read_text()
            for required in ('DATABASE_URL', 'KAFKA_BROKERS', 'GIGAQUIZZ_SCHEMA', '-kafka-config',
                             'Python >=3.8', 'Linux procps', '-env /absolute/private/service.env'):
                self.assertIn(required, text)
            self.assertNotIn('](launch.md)', text)
            self.assertIn('Комплект не устанавливает и не запускает PostgreSQL/Kafka', text)
            self.assertIn('Transaction/statement pooling несовместим', text)

    def test_git_snapshot_disables_hooks_and_hashes_untracked_source_without_copying(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'a.go').write_text('old content')
            (root / 'new.py').write_text('new source')
            commands = []
            def git_result(args, **kwargs):
                commands.append(args)
                value = (b'' if '--ignored' in args else b'?? new.py\0' if 'status' in args else b'a.go\0new.py\0' if 'ls-files' in args else
                         b'a' * 40 + b'\n' if 'rev-parse' in args else b'simple-files\n')
                return argparse.Namespace(returncode=0, stdout=value)
            with patch.object(release.subprocess, 'run', side_effect=git_result):
                first = release.source_snapshot(root, root / 'release')
                (root / 'new.py').write_text('changed source')
                second = release.source_snapshot(root, root / 'release')
            self.assertTrue(first['dirty'])
            self.assertNotEqual(first['source_tree_sha256'], second['source_tree_sha256'])
            for command in commands:
                self.assertEqual(command[:5], ['git', '-c', 'core.fsmonitor=false', '-c', 'core.untrackedCache=false'])
            self.assertIn(':(exclude,literal)release', next(command for command in commands if 'status' in command))

    def test_ignored_private_asset_rejected_before_build_but_local_data_excluded(self):
        # Real Git determines ignore scope; only Go tool execution is blocked.
        # This catches a broad go:embed input omitted by normal git ls-files.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / '.gitignore').write_text('.env\n.local/\n')
            (root / 'internal/web/static').mkdir(parents=True)
            (root / 'internal/web/assets.go').write_text('package web\n')
            (root / '.local').mkdir()
            user = root / '.local/user.env'
            user.write_text('fixture-private-user-data')
            env = dict(os.environ, GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull)
            for command in (['git', 'init', '-q'], ['git', 'add', '.gitignore', 'internal/web/assets.go'],
                            ['git', '-c', 'user.name=Release Fixture', '-c', 'user.email=fixture@example.invalid',
                             'commit', '-qm', 'fixture source']):
                subprocess.run(command, cwd=root, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            before = release.source_snapshot(root, root / '.local/new-release')
            self.assertFalse(before['dirty'])
            user.write_text('changed fixture-private-user-data')
            self.assertEqual(before, release.source_snapshot(root, root / '.local/new-release'))
            secret = root / 'internal/web/static/.env'
            secret.write_text('fixture-secret-must-not-be-read-or-embedded')
            original_run, original_open = subprocess.run, Path.open
            def only_git(command, **kwargs):
                self.assertEqual(command[0], 'git', 'ignored asset must fail before even go version')
                return original_run(command, **kwargs)
            def protect_private(path, *args, **kwargs):
                self.assertNotEqual(path, secret, 'builder must reject by inventory without reading private contents')
                self.assertNotEqual(path, user, 'ignored local data must not enter source hashing')
                return original_open(path, *args, **kwargs)
            output = root / '.local/new-release'
            args = argparse.Namespace(output=str(output), target=['linux/amd64'], allow_dirty=True)
            public = io.StringIO()
            with patch.object(release.subprocess, 'run', side_effect=only_git), \
                 patch.object(Path, 'open', protect_private), redirect_stdout(public):
                code = release.build(args, root)
            self.assertEqual(code, 1)
            report = json.loads((output / 'release-report.json').read_text())
            self.assertFalse(report['complete'])
            self.assertNotIn(secret.name, public.getvalue())
            self.assertNotIn('fixture-secret', public.getvalue())
            self.assertFalse((output / 'env.example').exists())
            self.assertFalse((output / 'SHA256SUMS').exists())


if __name__ == '__main__':
    unittest.main()
