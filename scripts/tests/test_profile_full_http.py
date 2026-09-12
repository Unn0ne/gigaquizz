"""Exercise orchestration with in-memory HTTP/process fixtures only.

No application, generator, broker, subprocess or listener is started here.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime, timedelta
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('profile_full_http', ROOT / 'scripts/profile_full_http.py')
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class Response(io.BytesIO):
    def __init__(self, value, status=200):
        super().__init__(json.dumps(value).encode())
        self.status = status


class Process:
    def __init__(self, pid, running=True, stuck=False):
        self.pid = pid
        self.returncode = None if running else 0
        self.stuck = stuck
        self.terminated = False
        self.killed = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True

    def wait(self, timeout=None):
        if self.stuck:
            raise subprocess.TimeoutExpired('fixture-owned-process', timeout)
        self.returncode = 0
        return 0


class HarnessFixture:
    def __init__(self, directory, interrupt=False, stuck=False):
        self.directory = directory
        self.interrupt, self.stuck = interrupt, stuck
        self.commands = []
        self.sql_commands = []
        self.processes = []
        self.poll = None
        self.result = None

    def open(self, request, timeout=None):
        path = urllib.parse.urlsplit(request.full_url).path
        if path == '/readyz':
            return Response({'status': 'ready'})
        if path == '/api/admin/login':
            return Response({'authenticated': True})
        if path == '/api/admin/polls':
            body = json.loads(request.data)
            start = datetime.fromisoformat(body['starts_at'])
            self.poll = {'id': '10000000-0000-0000-0000-000000000001',
                         'starts_at': start.isoformat(), 'ends_at': (start + timedelta(seconds=60)).isoformat()}
            self.result = {'state': 'final', 'total_votes': 1,
                           'options': [{'id': 1, 'votes': 1}, {'id': 2, 'votes': 0}]}
            return Response(self.poll, 201)
        if path.endswith('/results'):
            return Response(self.result)
        if path == '/api/admin/metrics':
            return Response({'recorded_responses': 1})
        if path.endswith('/votes'):
            raise urllib.error.HTTPError(request.full_url, 410, 'closed', {}, io.BytesIO(b'{"status":"closed"}'))
        raise AssertionError('unexpected fixture HTTP request')

    def popen(self, command, **kwargs):
        self.commands.append((command, kwargs))
        workload = '-rate' in command
        audit = '-audit-only' in command
        process = Process(20000 + len(self.processes), running=not (audit or (workload and not self.interrupt)),
                          stuck=workload and self.stuck)
        self.processes.append(process)
        if workload or audit:
            json.dump({'complete': True, 'cancelled': False, 'correct': True, 'planned_attempts': 60,
                       'valid_recorded_ack': 1, 'matched_ack_attempts': 1, 'missing_ack_attempts': 0,
                       'unverified_ack_attempts': 0, 'saved_service_results_matched': True}, kwargs['stdout'])
            kwargs['stdout'].flush()
        return process

    def sleep(self, seconds):
        if self.interrupt:
            raise KeyboardInterrupt
        raise AssertionError('successful fixture should require no real waiting')

    def run(self, command, **kwargs):
        if '-XAt' not in command:
            raise AssertionError('unexpected fixture subprocess request')
        self.sql_commands.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps({'fixture_journal': True}), stderr='')


class ProfileTests(unittest.TestCase):
    def execute_fixture(self, repeat_every=0, interrupt=False, stuck=False, extra_env=None,
                        cpu_profile=False, mode='simple-files'):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            app, generator, psql = directory / 'application', directory / 'generator', directory / 'psql'
            app.write_bytes(b'fixture application identity')
            generator.write_bytes(b'fixture generator identity')
            psql.write_bytes(b'fixture psql identity; never executed')
            if mode == 'postgres-kafka':
                lab = directory / '.local/kafka-lab'
                lab.mkdir(parents=True)
                (lab / 'metadata.json').write_text('{"nodes":{}}')
            args = argparse.Namespace(mode=mode, label='fixture', port=8093, rate=1,
                                      repeat_every=repeat_every, workers=2, queue=8, max_lag_ms=100,
                                      max_inflight=8, partitions=4, batch_votes=None, queue_votes=None,
                                      linger_ms=None, app_memory='3GiB', psql=str(psql), cpu_profile=cpu_profile)
            fixture = HarnessFixture(directory, interrupt, stuck)
            with ExitStack() as stack:
                stack.enter_context(patch.object(profile.urllib.request, 'build_opener', return_value=fixture))
                stack.enter_context(patch.object(profile.subprocess, 'Popen', side_effect=fixture.popen))
                stack.enter_context(patch.object(profile.subprocess, 'run', side_effect=fixture.run))
                stack.enter_context(patch.object(profile.socket, 'create_connection'))
                stack.enter_context(patch.object(profile, 'process_samples', return_value=[]))
                stack.enter_context(patch.object(profile.shutil, 'disk_usage', return_value=argparse.Namespace(free=64 * 1024**3)))
                stack.enter_context(patch.object(profile.time, 'sleep', side_effect=fixture.sleep))
                stack.enter_context(patch.dict(profile.os.environ, extra_env or {}, clear=True))
                stack.enter_context(patch('sys.stdout', new_callable=io.StringIO))
                code, raised = None, None
                attempts, required = profile.plan_bytes(args.rate, repeat_every, args.mode)
                try:
                    code = profile.owned_profile(args, ROOT, directory, app, generator,
                                                 'http://127.0.0.1:8093', directory, attempts, required)
                except (KeyboardInterrupt, subprocess.TimeoutExpired, OSError) as error:
                    raised = type(error).__name__
            path = directory / 'report.json'
            report = json.loads(path.read_text()) if path.exists() else None
            return fixture, code, raised, report

    def test_explicit_zero_repeat_reaches_generator_and_report(self):
        fixture, code, raised, report = self.execute_fixture()
        self.assertIsNone(raised)
        self.assertEqual(code, 0)
        command = next(cmd for cmd, _ in fixture.commands if '-rate' in cmd)
        self.assertIn('-repeat-every', command)
        self.assertEqual(command[command.index('-repeat-every') + 1], '0')
        self.assertEqual(report['configuration']['planned_attempts'], 60)
        self.assertEqual(report['configuration']['repeat_every'], 0)
        self.assertTrue(report['correct'])
        self.assertEqual(sum(p.terminated for p in fixture.processes), 2)

    def test_repeats_are_added_to_same_minute_population_and_disk_guard(self):
        plain, plain_bytes = profile.plan_bytes(1000, 0, 'simple-files')
        repeated, repeated_bytes = profile.plan_bytes(1000, 5, 'simple-files')
        self.assertEqual(plain, 60000)
        self.assertEqual(repeated, 72000)
        self.assertGreater(repeated_bytes, plain_bytes)
        self.assertGreater(plain_bytes, profile.RESERVE)
        _, kafka_bytes = profile.plan_bytes(1000, 5, 'postgres-kafka')
        self.assertGreater(kafka_bytes, repeated_bytes)

    def test_total_attempt_cap_rejects_additional_repeats_before_network(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / 'fixture'
            binary.write_bytes(b'never executed')
            binary.chmod(0o700)
            args = argparse.Namespace(runtime_root=str(root), server_binary=str(binary), load_binary=str(binary),
                                      mode='simple-files', rate=100000, repeat_every=1, port=8093, label='fixture')
            with patch.object(profile.os, 'umask'), \
                    patch.object(profile.socket, 'socket', side_effect=AssertionError('preflight touched network')), \
                    patch.object(profile, 'owned_profile', side_effect=AssertionError('preflight started a profile')):
                with self.assertRaisesRegex(RuntimeError, '10 million'):
                    profile.run(args)
            self.assertFalse((root / '.local').exists())

    def test_existing_kafka_allocation_plus_profile_must_fit_lab_cap(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / 'fixture'
            binary.write_bytes(b'never executed')
            binary.chmod(0o700)
            lab = root / '.local/kafka-lab'
            lab.mkdir(parents=True)
            marker = lab / '.gigaquizz-kafka-lab'
            marker.write_text('gigaquizz native Kafka lab v1\n')
            args = argparse.Namespace(runtime_root=str(root), server_binary=str(binary), load_binary=str(binary),
                                      mode='postgres-kafka', rate=1000, repeat_every=5, port=8093, label='fixture')
            with patch.object(profile.os, 'umask'), \
                    patch.object(profile.shutil, 'disk_usage', return_value=argparse.Namespace(free=128 * 1024**3)), \
                    patch.object(profile, 'allocated_bytes', return_value=profile.MAX_KAFKA_LAB), \
                    patch.object(profile.socket, 'socket', side_effect=AssertionError('cap failure touched network')), \
                    patch.object(profile, 'owned_profile', side_effect=AssertionError('cap failure started a profile')):
                with self.assertRaisesRegex(RuntimeError, '26GiB'):
                    profile.run(args)
            self.assertEqual(marker.read_text(), 'gigaquizz native Kafka lab v1\n')
            self.assertFalse((root / '.local/http-full-profiles').exists())

    def test_interrupt_stops_both_owned_processes_and_saves_failed_report(self):
        fixture, _, _, report = self.execute_fixture(interrupt=True)
        self.assertEqual(len(fixture.processes), 2)
        self.assertTrue(all(p.terminated for p in fixture.processes))
        self.assertIsNotNone(report)
        self.assertFalse(report['correct'])

    def test_unreapable_generator_does_not_prevent_app_cleanup_or_report(self):
        fixture, _, _, report = self.execute_fixture(interrupt=True, stuck=True)
        self.assertTrue(fixture.processes[1].killed)
        self.assertTrue(fixture.processes[0].terminated, 'generator wait timeout must not skip application cleanup')
        self.assertIsNotNone(report, 'a failed cleanup still needs a diagnostic report')
        self.assertFalse(report['correct'])
        self.assertTrue(report['errors'])

    def test_database_guard_accepts_loopback_or_exact_owned_socket(self):
        runtime = Path('/fixture/runtime')
        for url in ('postgresql://user@127.0.0.1/database',
                    'postgresql://user@[::1]/database',
                    'postgresql:///database?host=%2Ffixture%2Fruntime%2F.local'):
            with self.subTest(url=url):
                self.assertTrue(profile.local_database(url, runtime))
        for url in ('postgresql://example.com/database', 'postgresql:///database?host=/tmp',
                    'postgresql:///database?host=127.0.0.1&host=203.0.113.7'):
            with self.subTest(url=url):
                self.assertFalse(profile.local_database(url, runtime))

    def test_database_guard_rejects_remote_hostaddr_despite_loopback_host(self):
        self.assertFalse(profile.local_database('postgresql://127.0.0.1/database?hostaddr=203.0.113.7', Path('/fixture')))

    def test_inherited_routing_and_preparation_cannot_override_fixture(self):
        fixture, code, _, _ = self.execute_fixture(extra_env={'PGHOSTADDR': '203.0.113.7',
                                                              'PGSERVICE': 'foreign',
                                                              'POLL_PREPARATION_SECONDS': '60'})
        self.assertEqual(code, 0)
        for _, kwargs in fixture.commands:
            self.assertNotIn('PGHOSTADDR', kwargs['env'])
            self.assertNotIn('PGSERVICE', kwargs['env'])
            self.assertNotEqual(kwargs['env'].get('POLL_PREPARATION_SECONDS'), '60')

    def test_psql_environment_decodes_credentials_and_database_without_inheriting_routing(self):
        # These are deliberately awkward synthetic credentials, not real ones.
        user, password, database = 'fixture@user', 'fake:p@ss /?#+%word', 'fixture database/part'
        quote = urllib.parse.quote
        url = ('postgresql://' + quote(user, safe='') + ':' + quote(password, safe='') +
               '@127.0.0.1:5544/' + quote(database, safe='') + '?sslmode=disable&connect_timeout=7')
        self.assertTrue(profile.local_database(url, Path('/fixture')))
        inherited = {'PATH': '/fixture/bin', 'LANG': 'C', 'PGHOSTADDR': '203.0.113.7',
                     'PGSERVICE': 'foreign', 'PGSERVICEFILE': '/foreign/service',
                     'PGDATABASE': 'foreign', 'PGPASSWORD': 'old', 'PGOPTIONS': '-c search_path=foreign'}
        with patch.dict(profile.os.environ, inherited, clear=True):
            env = profile.psql_environment(url)
            self.assertEqual(dict(profile.os.environ), inherited, 'helper must not mutate the parent environment')
        self.assertEqual({k: v for k, v in env.items() if k.startswith('PG')},
                         {'PGHOST': '127.0.0.1', 'PGPORT': '5544', 'PGDATABASE': database,
                          'PGUSER': user, 'PGPASSWORD': password, 'PGSSLMODE': 'disable', 'PGCONNECT_TIMEOUT': '7'})
        self.assertEqual(env['PATH'], '/fixture/bin')

    def test_psql_environment_supports_ipv6_and_query_socket_without_a_uri_database_name(self):
        cases = (
            ('postgresql://fixture@[::1]:5545/example',
             {'PGHOST': '::1', 'PGPORT': '5545', 'PGDATABASE': 'example', 'PGUSER': 'fixture',
              'PGSSLMODE': 'prefer', 'PGCONNECT_TIMEOUT': '10'}),
            ('postgresql:///example?host=%2Ffixture%2Fruntime%2F.local&port=5546&sslmode=disable',
             {'PGHOST': '/fixture/runtime/.local', 'PGPORT': '5546', 'PGDATABASE': 'example',
              'PGSSLMODE': 'disable', 'PGCONNECT_TIMEOUT': '10'}),
            ('postgres://127.0.0.1/example',
             {'PGHOST': '127.0.0.1', 'PGPORT': '5432', 'PGDATABASE': 'example',
              'PGSSLMODE': 'prefer', 'PGCONNECT_TIMEOUT': '10'}),
        )
        for url, expected in cases:
            with self.subTest(url=url), patch.dict(profile.os.environ, {'PGUSER': 'foreign', 'PGPASSWORD': 'foreign'}, clear=True):
                self.assertTrue(profile.local_database(url, Path('/fixture/runtime')))
                self.assertEqual(profile.psql_environment(url), expected)

    def test_kafka_reader_command_keeps_decoded_password_only_in_environment(self):
        password = 'fixture-secret:@/?%'
        url = 'postgresql://fixture:' + urllib.parse.quote(password, safe='') + '@127.0.0.1:5544/test_database'
        fixture, code, raised, report = self.execute_fixture(mode='postgres-kafka', extra_env={
            'TEST_DATABASE_URL': url, 'PGHOSTADDR': '203.0.113.7', 'PGSERVICE': 'foreign'})
        self.assertEqual(code, 0)
        self.assertIsNone(raised)
        self.assertTrue(report['correct'])
        self.assertEqual(len(fixture.sql_commands), 1)
        command, options = fixture.sql_commands[0]
        self.assertEqual(options['env']['PGDATABASE'], 'test_database')
        self.assertEqual(options['env']['PGPASSWORD'], password)
        self.assertEqual(options['env']['PGHOST'], '127.0.0.1')
        self.assertNotIn('PGHOSTADDR', options['env'])
        self.assertNotIn('PGSERVICE', options['env'])
        self.assertTrue(options['capture_output'])
        for argument in command:
            self.assertNotIn(password, argument)
            self.assertNotIn(url, argument)
        self.assertNotIn(password, json.dumps(report))
        self.assertNotIn(url, json.dumps(report))

    def test_cpu_diagnostic_uses_distinct_owned_files_for_original_and_restart(self):
        fixture, code, raised, report = self.execute_fixture(cpu_profile=True)
        self.assertEqual(code, 0)
        self.assertIsNone(raised)
        self.assertTrue(report['diagnostic_cpu_profile'])
        applications = [cmd for cmd, _ in fixture.commands if '-env' in cmd]
        self.assertEqual(len(applications), 2)
        paths = [Path(cmd[cmd.index('-cpu-profile') + 1]) for cmd in applications]
        self.assertEqual([path.name for path in paths], ['cpu-1.pprof', 'cpu-2.pprof'])
        self.assertTrue(all(path.parent == fixture.directory for path in paths))
        self.assertNotEqual(paths[0], paths[1], 'restart must not overwrite or reopen the first profile')
        for command, _ in fixture.commands:
            if '-env' not in command:
                self.assertNotIn('-cpu-profile', command, 'only the application is CPU-profiled')

    def test_comparable_default_run_does_not_enable_cpu_profiling(self):
        fixture, code, _, report = self.execute_fixture()
        self.assertEqual(code, 0)
        self.assertFalse(report['diagnostic_cpu_profile'])
        for command, _ in fixture.commands:
            self.assertNotIn('-cpu-profile', command)


if __name__ == '__main__':
    unittest.main()
