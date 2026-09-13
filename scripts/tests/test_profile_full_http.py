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


def fixture_load(keys, offset=0, repeat_every=0, index=0):
    attempts = keys + ((offset + keys) // repeat_every - offset // repeat_every if repeat_every else 0)
    # Deliberately sparse ACK coverage: exact reconciliation must never imply
    # that all planned attempts were accepted. This is an orchestration fixture.
    return {'mode': 'http-post', 'complete': True, 'cancelled': False,
            'planned_attempts': attempts, 'planned_unique_keys': keys, 'http_post_sent': 1,
            'valid_recorded_ack': 1, 'generator_skipped': attempts - 1,
            'repeat_every': repeat_every, 'workers': 2, 'queue': 8, 'max_lag_ms': 100,
            'scheduled_seconds': 60, 'post_latency_scope': 'fixture dispatch through body',
            'latency_mean_ms': 10 * (index + 1), 'latency_max_ms': 10 * (index + 1),
            'latency_p99_upper_bound_ms': 16 * (index + 1),
            'post_sent_per_scheduled_second': [1] + [0] * 59,
            'ack_received_per_scheduled_second': [1] + [0] * 59}


class HarnessFixture:
    def __init__(self, directory, interrupt=False, stuck=False, strict=True, generators=1, repeat_every=0,
                 pending_after_restart=0, changed_after_restart=False, never_final=False, failed_generator=None, audit_generators=None,
                 load_overrides=None, audit_overrides=None, manifest_overrides=None):
        self.directory = directory
        self.interrupt, self.stuck, self.strict = interrupt, stuck, strict
        self.generators, self.repeat_every = generators, repeat_every
        self.pending_after_restart, self.changed_after_restart = pending_after_restart, changed_after_restart
        self.never_final, self.failed_generator = never_final, failed_generator
        self.audit_generators = generators if audit_generators is None else audit_generators
        self.load_overrides, self.audit_overrides = load_overrides or {}, audit_overrides or {}
        self.manifest_overrides = manifest_overrides or {}
        self.app_starts, self.sleeps = 0, 0
        self.interrupt_after_spawn = None
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
            if self.app_starts > 1:
                if self.pending_after_restart > 0 or self.never_final:
                    self.pending_after_restart -= 1
                    return Response({'state': 'processing', 'pending': True, 'total_votes': 0})
                if self.changed_after_restart:
                    return Response({**self.result, 'total_votes': self.result['total_votes'] + 1})
            return Response(self.result)
        if path == '/api/admin/metrics':
            return Response({'recorded_responses': 1})
        if path.endswith('/votes'):
            raise urllib.error.HTTPError(request.full_url, 410, 'closed', {}, io.BytesIO(b'{"status":"closed"}'))
        raise AssertionError('unexpected fixture HTTP request')

    def popen(self, command, **kwargs):
        self.commands.append((command, kwargs))
        preparing = '-prepare-plan' in command
        workload = '-plan' in command or ('-rate' in command and not preparing)
        audit = '-audit-only' in command or '-audit-plan' in command
        if '-env' in command:
            self.app_starts += 1
        index = int(command[command.index('-generator') + 1]) if '-generator' in command else 0
        running = not (audit or preparing or (workload and not self.interrupt and self.failed_generator is None and self.interrupt_after_spawn is None))
        process = Process(20000 + len(self.processes), running=running, stuck=workload and self.stuck)
        if workload and index == self.failed_generator:
            process.returncode = 7
        self.processes.append(process)
        if preparing:
            value = {'complete': True, 'generators': self.generators, 'planned_unique_keys': 60,
                     'planned_attempts': 60 + (60 // self.repeat_every if self.repeat_every else 0), 'http_votes_sent': 0}
        elif workload:
            keys = 60 // self.generators + (1 if index < 60 % self.generators else 0)
            offset = (60 // self.generators) * index + min(index, 60 % self.generators)
            value = fixture_load(keys, offset, self.repeat_every, index)
            value.update(self.load_overrides)
            ledger = Path(command[command.index('-ledger-dir') + 1])
            ledger.mkdir(mode=0o700)
            config = {'poll_id': self.poll['id'], 'url': 'http://127.0.0.1:8093', 'unique_rate': 1,
                      'unique_count': keys if self.generators > 1 else 0, 'key_offset': offset,
                      'duration_ns': 60_000_000_000, 'repeat_every': self.repeat_every,
                      'workers': 2, 'queue': 8, 'max_lag_ns': 100_000_000}
            config.update(self.manifest_overrides)
            profile.private_json(ledger / 'manifest.json', {'version': 1, 'complete': True, 'config': config,
                                                         'poll': {'id': self.poll['id']}})
        elif audit:
            value = {'correct': True, 'planned_attempts': 60 + (60 // self.repeat_every if self.repeat_every else 0),
                     'planned_unique_keys': 60, 'generators': self.audit_generators,
                     'matched_ack_attempts': self.generators, 'missing_ack_attempts': 0,
                     'unverified_ack_attempts': 0, 'saved_service_results_matched': True,
                     'journal_inspection': {'method': 'kafka-stable-snapshot', 'strict_snapshot_complete': self.strict, 'snapshot_partitions': 4}}
            value.update(self.audit_overrides)
        else:
            return process
        json.dump(value, kwargs['stdout'])
        kwargs['stdout'].flush()
        return process

    def sleep(self, seconds):
        self.sleeps += 1
        if self.interrupt:
            raise KeyboardInterrupt
        if self.app_starts > 1:
            return
        raise AssertionError('successful fixture should require no real waiting')

    def run(self, command, **kwargs):
        if '-XAt' not in command:
            raise AssertionError('unexpected fixture subprocess request')
        self.sql_commands.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps({'fixture_journal': True}), stderr='')


class ProfileTests(unittest.TestCase):
    def execute_fixture(self, repeat_every=0, interrupt=False, stuck=False, extra_env=None,
                        cpu_profile=False, mode='simple-files', strict=True, generators=1, pending_after_restart=0,
                        changed_after_restart=False, never_final=False, failed_generator=None, audit_generators=None, interrupt_after_spawn=None,
                        load_overrides=None, audit_overrides=None, manifest_overrides=None):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            app, generator, psql = directory / 'application', directory / 'generator', directory / 'psql'
            reader = directory / 'reader'
            reader.write_bytes(b'fixture current strict reader')
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
                                      linger_ms=None, app_memory='3GiB', psql=str(psql), cpu_profile=cpu_profile, generators=generators)
            fixture = HarnessFixture(directory, interrupt, stuck, strict, generators, repeat_every,
                                     pending_after_restart, changed_after_restart, never_final, failed_generator, audit_generators,
                                     load_overrides, audit_overrides, manifest_overrides)
            fixture.interrupt_after_spawn = interrupt_after_spawn
            with ExitStack() as stack:
                stack.enter_context(patch.object(profile.urllib.request, 'build_opener', return_value=fixture))
                stack.enter_context(patch.object(profile.subprocess, 'Popen', side_effect=fixture.popen))
                if interrupt_after_spawn is not None:
                    def mask(how, values):
                        if how == profile.signal.SIG_SETMASK and len(fixture.processes) == fixture.interrupt_after_spawn:
                            fixture.interrupt_after_spawn = -1
                            raise KeyboardInterrupt
                        return set()
                    stack.enter_context(patch.object(profile.signal, 'pthread_sigmask', side_effect=mask))
                stack.enter_context(patch.object(profile.subprocess, 'run', side_effect=fixture.run))
                stack.enter_context(patch.object(profile.socket, 'create_connection'))
                stack.enter_context(patch.object(profile, 'process_samples', return_value=[]))
                stack.enter_context(patch.object(profile.shutil, 'disk_usage', return_value=argparse.Namespace(free=64 * 1024**3)))
                stack.enter_context(patch.object(profile.time, 'sleep', side_effect=fixture.sleep))
                if never_final:
                    stack.enter_context(patch.object(profile.time, 'monotonic', side_effect=iter(range(0, 10000, 60))))
                stack.enter_context(patch.dict(profile.os.environ, extra_env or {}, clear=True))
                stack.enter_context(patch('sys.stdout', new_callable=io.StringIO))
                code, raised = None, None
                attempts, required = profile.plan_bytes(args.rate, repeat_every, args.mode)
                try:
                    code = profile.owned_profile(args, ROOT, directory, app, generator,
                                                 'http://127.0.0.1:8093', directory, attempts, required, reader)
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

    def test_current_reader_is_independent_of_frozen_generator(self):
        fixture, code, _, report = self.execute_fixture()
        self.assertEqual(code, 0)
        workload = next(cmd for cmd, _ in fixture.commands if '-rate' in cmd)
        reader = next(cmd for cmd, _ in fixture.commands if '-audit-only' in cmd)
        self.assertNotEqual(workload[0], reader[0])
        self.assertNotEqual(report['binary_sha256']['generator'], report['binary_sha256']['reader'])
        self.assertFalse(report['load_coverage']['all_planned_attempts_acknowledged'])
        self.assertEqual(report['load_coverage']['planned_attempts_without_ack'], 59)

    def test_prefix_only_kafka_audit_cannot_certify_profile(self):
        _, code, _, report = self.execute_fixture(mode='postgres-kafka', strict=False,
            extra_env={'TEST_DATABASE_URL': 'postgresql://127.0.0.1/fixture'})
        self.assertNotEqual(code, 0)
        self.assertFalse(report['correct'])
        self.assertTrue(any('stable snapshot' in x['message'] for x in report['errors']))

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
            args = argparse.Namespace(runtime_root=str(root), server_binary=str(binary), load_binary=str(binary), audit_binary=str(binary),
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
            args = argparse.Namespace(runtime_root=str(root), server_binary=str(binary), load_binary=str(binary), audit_binary=str(binary),
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


    def test_multiple_generators_share_one_population_and_one_exact_reader(self):
        fixture, code, raised, report = self.execute_fixture(generators=3, repeat_every=7)
        self.assertIsNone(raised)
        self.assertEqual(code, 0)
        prepare = next(command for command, _ in fixture.commands if '-prepare-plan' in command)
        self.assertEqual(prepare[prepare.index('-total-unique') + 1], '60')
        self.assertEqual(prepare[prepare.index('-generators') + 1], '3')
        self.assertNotIn('-allow-high-load', prepare)
        plan = prepare[prepare.index('-prepare-plan') + 1]
        workloads = [command for command, _ in fixture.commands if '-plan' in command]
        self.assertEqual(len(workloads), 3)
        for i, command in enumerate(workloads):
            self.assertEqual(command[command.index('-plan') + 1], plan)
            self.assertEqual(command[command.index('-generator') + 1], str(i))
            self.assertEqual(Path(command[command.index('-ledger-dir') + 1]).name, f'generator-{i:03d}')
            self.assertNotIn('-rate', command, 'children must use disjoint plan ranges, never duplicate the total rate')
        readers = [command for command, _ in fixture.commands if '-audit-plan' in command]
        self.assertEqual(len(readers), 1)
        self.assertEqual(readers[0][readers[0].index('-audit-plan') + 1], plan)
        self.assertEqual(Path(readers[0][readers[0].index('-ledgers') + 1]).name, 'ledger')
        self.assertEqual(report['load']['planned_attempts'], 68)
        self.assertEqual(report['load']['planned_unique_keys'], 60)
        self.assertEqual(report['load']['valid_recorded_ack'], 3)
        self.assertEqual(report['load']['generator_skipped'], 65)
        self.assertEqual(report['load']['latency_mean_ms'], 20)
        self.assertNotIn('latency_p99_upper_bound_ms', report['load'])
        self.assertEqual(len(report['generator_loads']), 3)
        self.assertFalse(report['load_coverage']['all_planned_attempts_acknowledged'])
        self.assertEqual(report['configuration']['combined_resource_budget']['workers_total'], 6)

    def test_one_failed_generator_stops_its_owned_siblings_and_cannot_certify_a_partial_plan(self):
        fixture, code, raised, report = self.execute_fixture(generators=3, failed_generator=1)
        self.assertIsNone(raised)
        self.assertNotEqual(code, 0)
        self.assertFalse(report['correct'])
        children = [process for (command, _), process in zip(fixture.commands, fixture.processes) if '-plan' in command]
        self.assertEqual(len(children), 3)
        self.assertTrue(children[0].terminated and children[2].terminated)
        self.assertFalse(any('-audit-plan' in command for command, _ in fixture.commands))
        self.assertTrue(fixture.processes[0].terminated)


    def test_interrupt_stops_every_generator_and_application_in_one_plan(self):
        fixture, _, _, report = self.execute_fixture(generators=3, interrupt=True)
        children = [process for (command, _), process in zip(fixture.commands, fixture.processes) if '-plan' in command or '-env' in command]
        self.assertEqual(len(children), 4)
        self.assertTrue(all(process.terminated for process in children))
        self.assertFalse(report['correct'])
        self.assertFalse(any('-audit-plan' in command for command, _ in fixture.commands))


    def test_pending_signal_after_app_spawn_still_cleans_up_registered_pid(self):
        fixture, _, raised, report = self.execute_fixture(interrupt_after_spawn=1)
        self.assertIsNone(raised)
        self.assertEqual(len(fixture.processes), 1)
        self.assertTrue(fixture.processes[0].terminated)
        self.assertFalse(report['correct'])

    def test_pending_signal_between_generator_spawn_and_assignment_cleans_up_all_registered_children(self):
        # app, plan preparer, first generator, second generator: interrupt as
        # the fourth PID is registered, before generator_children.append runs.
        fixture, _, raised, report = self.execute_fixture(generators=3, interrupt_after_spawn=4)
        self.assertIsNone(raised)
        self.assertEqual(len(fixture.processes), 4)
        self.assertTrue(fixture.processes[0].terminated)
        self.assertTrue(fixture.processes[2].terminated and fixture.processes[3].terminated)
        self.assertFalse(report['correct'])

    def test_child_restores_original_signal_mask_and_only_changes_its_own_fd_limit(self):
        original = {profile.signal.SIGHUP}
        with patch.object(profile.resource, 'getrlimit', return_value=(256, 65536)), \
                patch.object(profile.resource, 'setrlimit') as limits, \
                patch.object(profile.signal, 'pthread_sigmask') as mask:
            profile.child_limits(33280, original)
        limits.assert_called_once_with(profile.resource.RLIMIT_NOFILE, (33280, 65536))
        mask.assert_called_once_with(profile.signal.SIG_SETMASK, original)

    def test_foreign_partition_memory_bound_cannot_override_the_owned_fixture(self):
        fixture, code, _, report = self.execute_fixture(extra_env={'MAX_PARTITION_UNIQUE_VOTERS': '1'})
        self.assertEqual(code, 0)
        self.assertEqual(report['configuration']['max_partition_unique_voters'], 160)
        for command, options in fixture.commands:
            if '-env' in command:
                self.assertEqual(options['env']['MAX_PARTITION_UNIQUE_VOTERS'], '160')

    def test_reader_must_certify_every_generator_even_if_ack_sum_matches(self):
        _, code, _, report = self.execute_fixture(generators=2, audit_generators=1)
        self.assertNotEqual(code, 0)
        self.assertFalse(report['correct'])
        self.assertTrue(any('every planned generator' in entry['message'] for entry in report['errors']))

    def test_single_generator_cannot_substitute_population_or_workload_at_equal_attempt_count(self):
        # 30 originals + 30 repeats have the same denominator as the requested
        # 60 originals, but exercise a different unique population and dedup path.
        for changed in ({'planned_unique_keys': 30, 'repeat_every': 1},
                        {'mode': 'http-journey'}, {'scheduled_seconds': 30},
                        {'workers': 1}, {'queue': 16}, {'max_lag_ms': 1000}):
            with self.subTest(changed=changed):
                fixture, code, raised, report = self.execute_fixture(load_overrides=changed)
                self.assertIsNone(raised)
                self.assertNotEqual(code, 0)
                self.assertFalse(report['correct'])
                self.assertTrue(any('requested profile' in entry['message'] for entry in report['errors']))
                self.assertFalse(any('-audit-only' in command for command, _ in fixture.commands))
                self.assertTrue(fixture.processes[0].terminated)

    def test_single_generator_post_count_and_outcomes_must_agree(self):
        for changed in ({'http_post_sent': 2}, {'generator_skipped': 58}, {'unknown': -1}):
            with self.subTest(changed=changed):
                _, code, raised, report = self.execute_fixture(load_overrides=changed)
                self.assertIsNone(raised)
                self.assertNotEqual(code, 0)
                self.assertFalse(report['correct'])
                self.assertTrue(any('requested attempts' in entry['message'] for entry in report['errors']))

    def test_single_generator_audit_must_certify_the_requested_population_not_only_ack_sum(self):
        for changed in ({'planned_unique_keys': 30}, {'planned_attempts': 59}, {'generators': 2}):
            with self.subTest(changed=changed):
                _, code, raised, report = self.execute_fixture(audit_overrides=changed)
                self.assertIsNone(raised)
                self.assertNotEqual(code, 0)
                self.assertFalse(report['correct'])
                self.assertTrue(any('every planned generator' in entry['message'] for entry in report['errors']))

    def test_private_manifest_must_match_even_when_summary_claims_the_requested_population(self):
        cases = ({'unique_count': 30, 'repeat_every': 1}, {'unique_count': -1}, {'unique_rate': 0},
                 {'key_offset': 1}, {'repeat_every': 1}, {'journey': True}, {'duration_ns': 30_000_000_000},
                 {'url': 'http://127.0.0.1:1'}, {'poll_id': '10000000-0000-0000-0000-000000000099'})
        for changed in cases:
            with self.subTest(changed=changed):
                fixture, code, raised, report = self.execute_fixture(manifest_overrides=changed)
                self.assertIsNone(raised)
                self.assertNotEqual(code, 0)
                self.assertFalse(report['correct'])
                self.assertTrue(any('generator manifest' in entry['message'] for entry in report['errors']))
                self.assertFalse(any('-audit-only' in command for command, _ in fixture.commands))

    def test_restart_waits_for_async_finalization_then_compares_exact_saved_result(self):
        fixture, code, _, report = self.execute_fixture(pending_after_restart=2)
        self.assertEqual(code, 0)
        self.assertEqual(fixture.sleeps, 2)
        self.assertTrue(report['correct'])
        _, code, _, report = self.execute_fixture(pending_after_restart=1, changed_after_restart=True)
        self.assertNotEqual(code, 0)
        self.assertFalse(report['correct'])
        self.assertTrue(any('final result changed' in entry['message'] for entry in report['errors']))

    def test_restart_pending_state_has_a_deadline_and_cleans_up(self):
        fixture, code, raised, report = self.execute_fixture(never_final=True)
        self.assertIsNone(raised)
        self.assertNotEqual(code, 0)
        self.assertFalse(report['correct'])
        self.assertTrue(any('finalization exceeded' in entry['message'] for entry in report['errors']))
        self.assertTrue(all(process.terminated for (command, _), process in zip(fixture.commands, fixture.processes) if '-env' in command))

    def test_aggregate_rejects_missing_outcomes_or_duplicate_population(self):
        good = [fixture_load(30), fixture_load(30, offset=30, index=1)]
        self.assertEqual(profile.aggregate_load_reports(good, 60, 60)['valid_recorded_ack'], 2)
        bad = [dict(good[0]), dict(good[1])]
        bad[1]['generator_skipped'] -= 1
        with self.assertRaisesRegex(RuntimeError, 'cover'):
            profile.aggregate_load_reports(bad, 60, 60)
        with self.assertRaisesRegex(RuntimeError, 'population'):
            profile.aggregate_load_reports([fixture_load(60), fixture_load(60)], 60, 60)


    def test_multi_get_bytes_and_failure_categories_are_added_with_weighted_latency(self):
        loads = [fixture_load(30), fixture_load(30, offset=30, index=1)]
        for i, load in enumerate(loads):
            sent = i + 1
            load.update(http_get_sent=sent, http_get_failures=1, http_get_body_bytes=100 * sent,
                        http_get_decoded_body_bytes=500 * sent)
            load['http_get_stages'] = [{'stage': 'question', 'sent': sent, 'failures': 1,
                                       'response_body_bytes': 100 * sent, 'decoded_body_bytes': 500 * sent,
                                       'timeouts': 1 if i == 0 else 0, 'validation_failures': 1 if i == 1 else 0,
                                       'mean_latency_ms': 10 * sent, 'max_latency_ms': 20 * sent}]
        aggregate = profile.aggregate_load_reports(loads, 60, 60)
        self.assertEqual(aggregate['http_get_body_bytes'], 300)
        self.assertEqual(aggregate['http_get_decoded_body_bytes'], 1500)
        stage = aggregate['http_get_stages'][0]
        self.assertEqual(stage['timeouts'], 1)
        self.assertEqual(stage['validation_failures'], 1)
        self.assertAlmostEqual(stage['mean_latency_ms'], 50 / 3)
        self.assertEqual(stage['max_latency_ms'], 40)
        loads[1]['http_get_stages'][0]['stage'] = 'different'
        with self.assertRaisesRegex(RuntimeError, 'GET stages differ'):
            profile.aggregate_load_reports(loads, 60, 60)

    def test_combined_resource_budget_does_not_multiply_rate_or_reserve_by_generators(self):
        args = argparse.Namespace(generators=4, workers=4096, queue=8192, app_memory='3GiB')
        budget = profile.local_budget(args)
        self.assertEqual(budget['workers_total'], 16384)
        self.assertEqual(budget['queue_total'], 32768)
        self.assertEqual(budget['owned_child_open_file_limit'], 2 * 16384 + 512)
        self.assertEqual(budget['generator_GOMEMLIMIT_total_bytes'], 8 * 1024**3)
        one, one_bytes = profile.plan_bytes(1000, 7, 'simple-files', generators=1, workers=4096)
        four, four_bytes = profile.plan_bytes(1000, 7, 'simple-files', generators=4, workers=4096)
        self.assertEqual(one, four)
        self.assertLess(four_bytes - one_bytes, 64 * 1024**2)
        with self.assertRaisesRegex(RuntimeError, '1..4'):
            profile.local_budget(argparse.Namespace(generators=5))

    def test_frozen_load_binary_rejected_before_app_or_network_when_multiple_requested(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / 'fixture'
            binary.write_bytes(b'never executed')
            binary.chmod(0o700)
            args = argparse.Namespace(runtime_root=str(root), server_binary=str(binary), load_binary=str(binary), audit_binary=str(binary),
                                      mode='simple-files', rate=1, repeat_every=0, generators=2, port=8093, label='fixture')
            with patch.object(profile.os, 'umask'), \
                    patch.object(profile.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, stdout='', stderr='  -rate uint\n  -audit-only string\n')), \
                    patch.object(profile.socket, 'socket', side_effect=AssertionError('preflight touched network')), \
                    patch.object(profile, 'owned_profile', side_effect=AssertionError('preflight started a profile')):
                with self.assertRaisesRegex(RuntimeError, 'frozen POST-only'):
                    profile.run(args)
            self.assertFalse((root / '.local').exists())

    def test_plan_capability_probe_requires_exact_flags_and_independent_reader(self):
        help_text = '\n'.join('  -' + flag + ' string' for flag in ('prepare-plan', 'total-unique', 'generators', 'plan', 'generator', 'audit-plan', 'ledgers'))
        with patch.object(profile.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, stdout='', stderr=help_text)):
            profile.require_plan_support(Path('/fixture/binary'))
            profile.require_plan_support(Path('/fixture/binary'), reader=True)
        misleading = help_text.replace('  -plan string', '  -plan-extra string')
        with patch.object(profile.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, stdout='', stderr=misleading)):
            with self.assertRaisesRegex(RuntimeError, 'plan-capable'):
                profile.require_plan_support(Path('/fixture/binary'))

    def test_comparable_default_run_does_not_enable_cpu_profiling(self):
        fixture, code, _, report = self.execute_fixture()
        self.assertEqual(code, 0)
        self.assertFalse(report['diagnostic_cpu_profile'])
        for command, _ in fixture.commands:
            self.assertNotIn('-cpu-profile', command)


if __name__ == '__main__':
    unittest.main()
