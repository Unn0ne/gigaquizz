"""Host runner fixtures; no HTTP, application, broker, or workload is launched.

The cancellation test starts one tiny Python child and checks its termination.
All generator/inspection/ledger verification processes are mocked.
"""
import argparse
from contextlib import ExitStack, redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('run_http_generators', ROOT / 'scripts/run_http_generators.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)
DIGEST = 'a' * 64


def inspection():
    start = datetime.now(timezone.utc) + timedelta(seconds=120)
    ranges = []
    offset = 0
    for index, keys in enumerate((4, 5)):
        attempts = keys + (offset + keys) // 3 - offset // 3
        ranges.append(dict(index=index, unique_keys=keys, planned_attempts=attempts, key_offset=offset,
                           workers=2, queue=8, repeat_every=3, max_lag_ms=100, request_timeout_ms=1000,
                           journey=False, definition=False, ledger_bytes=attempts * 64,
                           worker_buffer_bytes=3 * 65536))
        offset += keys
    return dict(mode='distributed-plan-inspection', complete=True, plan_sha256=DIGEST,
                planned_unique_keys=9, planned_attempts=12, scheduled_seconds=60,
                start_at=start.isoformat(), ends_at=(start + timedelta(seconds=60)).isoformat(), generators=ranges)


def clock():
    return dict(verified=True, clock_step_detected=False, samples=3, valid_samples=3,
                max_clock_error_ms=10, rtt_ms=1, offset_lower_ms=-.5, offset_upper_ms=.5)


def identity(item):
    return dict(plan_sha256=DIGEST, generator=item['index'], planned_unique_keys=item['unique_keys'],
                planned_attempts=item['planned_attempts'])


def load(item):
    # A complete generator may have skips, unknowns, and only one ACK. Its
    # completion must never be turned into a claim that storage is correct.
    result = dict(identity(item), mode='http-post', complete=True, cancelled=False, scheduled_seconds=60,
                  workers=item['workers'], queue=item['queue'], max_lag_ms=item['max_lag_ms'],
                  repeat_every=item['repeat_every'], ledger_bytes=item['ledger_bytes'], http_post_sent=2,
                  valid_recorded_ack=1, unknown=1, generator_skipped=item['planned_attempts'] - 2,
                  invalid_successful_responses=0, clock=clock(),
                  transport=dict(proxy='disabled', protocol='http/1.1', tls_verification=True))
    return result


class Process:
    def __init__(self, pid, code=0):
        self.pid, self.code = pid, code
        self.terminated = self.killed = False

    def poll(self):
        return self.code

    def terminate(self):
        self.terminated, self.code = True, -signal.SIGTERM

    def kill(self):
        self.killed, self.code = True, -signal.SIGKILL

    def wait(self, timeout=None):
        if self.code is None:
            raise subprocess.TimeoutExpired('owned-fixture', timeout)
        return self.code


class Fixture:
    def __init__(self, directory, failure=None):
        self.directory = Path(directory)
        self.failure = failure
        self.plan = inspection()
        self.calls, self.processes = [], []
        self.binary = self.directory / 'fixture-binary'
        self.binary.write_bytes(b'not executed: subprocess is mocked')
        self.binary.chmod(0o700)
        self.planfile = self.directory / 'fixture-plan.json'
        self.planfile.write_text('{"private_fixture":"never publish this value"}')
        self.planfile.chmod(0o600)
        self.output = self.directory / 'output'

    def popen(self, command, **kwargs):
        self.calls.append((command, kwargs))
        index = int(command[command.index('-generator') + 1]) if '-generator' in command else None
        item = self.plan['generators'][index] if index is not None else None
        code = 0
        if '-inspect-plan' in command:
            report = self.plan
        elif '-preflight-only' in command:
            report = dict(identity(item), mode='distributed-generator-preflight', complete=True,
                          ledger_bytes=item['ledger_bytes'], worker_buffer_bytes=item['worker_buffer_bytes'],
                          required_open_files=2 * item['workers'] + 64, clock=clock(),
                          transport=dict(proxy='disabled', protocol='http/1.1', tls_verification=True))
        elif '-verify-ledger' in command:
            actual = load(item)
            report = dict(identity(item), mode='distributed-generator-ledger-verification', complete=True,
                          client_ledger_valid=True, ledger_records=item['planned_attempts'])
            report.update({key: actual.get(key, 0) for key in (*runner.OUTCOMES, 'invalid_successful_responses')})
            if index == 1 and self.failure == 'ledger_digest':
                report['plan_sha256'] = 'b' * 64
            if index == 1 and self.failure == 'ledger_counts':
                report['unknown'] -= 1
                report['generator_skipped'] += 1
        else:
            report = load(item)
            ledger = Path(command[command.index('-ledger-dir') + 1])
            ledger.mkdir(mode=0o700)
            (ledger / 'manifest.json').write_text('{"partial_fixture":true}')
            if index == 1 and self.failure == 'population':
                report['planned_unique_keys'] -= 1
            if self.failure == 'child_exit':
                code = None if index == 0 else 1
            if self.failure == 'interrupt':
                if index == 1:
                    raise KeyboardInterrupt
                code = None
        kwargs['stdout'].write(json.dumps(report).encode())
        kwargs['stdout'].flush()
        process = Process(90000 + len(self.processes), code)
        self.processes.append(process)
        return process

    def run(self, preflight=False):
        args = argparse.Namespace(plan=str(self.planfile), indices='0,1', binary=str(self.binary),
                                  output=str(self.output), max_processes=4, max_clock_error_ms=10,
                                  preflight_only=preflight)
        stdout = io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(patch.object(runner.subprocess, 'Popen', side_effect=self.popen))
            stack.enter_context(patch.object(runner, 'physical_memory', return_value=16 * runner.GIB))
            stack.enter_context(patch.object(runner.resource, 'getrlimit', return_value=(256, 100000)))
            stack.enter_context(patch.object(runner.shutil, 'disk_usage', return_value=argparse.Namespace(free=32 * runner.GIB)))
            stack.enter_context(redirect_stdout(stdout))
            code = runner.run(args)
        return code, json.loads((self.output / 'report.json').read_text()), json.loads(stdout.getvalue())


class RunnerTests(unittest.TestCase):
    def test_global_repeat_boundary_and_exact_population(self):
        plan = inspection()
        selected = runner.inspect_ranges(plan, [1])
        self.assertEqual(selected[0]['planned_attempts'], 7)  # local floor(5/3) would miss one repeat
        for mutate in (lambda p: p['generators'][1].update(planned_attempts=6, ledger_bytes=384),
                       lambda p: p['generators'][1].update(key_offset=3),
                       lambda p: p.update(planned_attempts=11),
                       lambda p: p['generators'][0].update(unique_keys=0),
                       lambda p: p['generators'][0].update(worker_buffer_bytes=0)):
            broken = deepcopy(plan)
            mutate(broken)
            with self.assertRaises(RuntimeError):
                runner.inspect_ranges(broken, [0, 1])

    def test_resource_budget_is_host_sum_and_child_fd_only(self):
        ranges = inspection()['generators']
        result = runner.resource_budget(ranges, 16 * runner.GIB, 32 * runner.GIB, 100000)
        self.assertEqual(result['ledger_bytes'], 12 * 64)
        self.assertEqual(result['sampled_rss_stop_bytes'], 5 * runner.GIB)
        with self.assertRaisesRegex(RuntimeError, 'memory'):
            runner.resource_budget(ranges, 8 * runner.GIB, 32 * runner.GIB, 100000)
        with self.assertRaisesRegex(RuntimeError, 'disk'):
            runner.resource_budget(ranges, 16 * runner.GIB, result['required_free_disk_bytes'] - 1, 100000)
        with self.assertRaisesRegex(RuntimeError, 'open-file'):
            runner.resource_budget(ranges, 16 * runner.GIB, 32 * runner.GIB, 255)
        many = [dict(ranges[0], workers=4096) for _ in range(9)]
        with self.assertRaisesRegex(RuntimeError, 'worker/queue'):
            runner.resource_budget(many, 256 * runner.GIB, 64 * runner.GIB, 100000)
        many = [dict(ranges[0], queue=1048576) for _ in range(3)]
        with self.assertRaisesRegex(RuntimeError, 'worker/queue'):
            runner.resource_budget(many, 256 * runner.GIB, 64 * runner.GIB, 100000)
        with patch.object(runner.resource, 'setrlimit') as limit, patch.object(runner.signal, 'pthread_sigmask'):
            runner.child_setup(256, set())
            limit.assert_called_once_with(resource.RLIMIT_NOFILE, (256, 256))

    def test_clock_transport_and_partial_outcomes_cannot_pass(self):
        item = inspection()['generators'][0]
        runner.check_load(load(item), item, DIGEST, 10)
        mutations = (lambda v: v.update(valid_recorded_ack=0), lambda v: v.update(http_post_sent=3),
                     lambda v: v.update(invalid_successful_responses=1), lambda v: v.update(generator=True),
                     lambda v: v['clock'].update(valid_samples=0), lambda v: v['clock'].update(offset_upper_ms=11),
                     lambda v: v['transport'].update(proxy='environment'), lambda v: v.update(plan_sha256='b' * 64))
        for change in mutations:
            value = load(item)
            change(value)
            with self.assertRaises(RuntimeError):
                runner.check_load(value, item, DIGEST, 10)

    def test_environment_drops_proxies_credentials_and_ambient_go_tuning(self):
        with patch.dict(os.environ, {'HTTPS_PROXY': 'private', 'PGPASSWORD': 'private', 'GOMEMLIMIT': '99GiB',
                                     'GOMAXPROCS': '99', 'GODEBUG': 'http2client=1'}, clear=True):
            self.assertEqual(runner.child_environment(), {'GOMEMLIMIT': '2GiB', 'GOGC': '100'})

    def test_preflight_only_never_creates_ledgers_or_claims_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory)
            code, report, public = fixture.run(preflight=True)
            self.assertEqual(code, 0)
            self.assertTrue(report['preflight_complete'])
            self.assertFalse(report['generation_complete'])
            self.assertFalse((fixture.output / 'generator-000').exists())
            self.assertEqual(len(fixture.calls), 3)
            self.assertEqual((fixture.output / 'report.json').stat().st_mode & 0o777, 0o600)
            self.assertEqual((fixture.output / 'preflight-000.json').stat().st_mode & 0o777, 0o600)
            self.assertNotIn('never publish this value', json.dumps(report) + json.dumps(public))
            for command, _ in fixture.calls[1:]:
                self.assertEqual(command[command.index('-max-clock-error') + 1], '10ms')

    def test_complete_client_generation_does_not_claim_ack_or_storage_success(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory)
            code, report, _ = fixture.run()
            self.assertEqual(code, 0)
            self.assertTrue(report['generation_complete'])
            self.assertEqual(report['totals']['valid_recorded_ack'], 2)
            self.assertEqual(report['totals']['unknown'], 2)
            self.assertEqual(report['totals']['generator_skipped'], 8)
            self.assertEqual(report['planned_attempts'], 12)
            self.assertEqual(len(report['ledger_checks']), 2)
            self.assertNotIn('correct', report)
            self.assertNotIn('integrity', report)
            self.assertIn('Separate physical', report['execution_scope'])
            self.assertTrue(all(Path(command[0]) == fixture.output / 'httpbench' for command, _ in fixture.calls))

    def test_population_or_verified_ledger_mismatch_is_a_failure(self):
        for failure in ('population', 'ledger_digest', 'ledger_counts'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                fixture = Fixture(directory, failure)
                code, report, _ = fixture.run()
                self.assertEqual(code, 1)
                self.assertFalse(report['generation_complete'])
                self.assertTrue(report['errors'])
                self.assertTrue((fixture.output / 'generator-000/manifest.json').is_file())

    def test_failed_child_or_interrupt_stops_registered_siblings_and_keeps_partial(self):
        for failure in ('child_exit', 'interrupt'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                fixture = Fixture(directory, failure)
                foreign = Process(80000, None)
                code, report, _ = fixture.run()
                self.assertEqual(code, 1)
                self.assertFalse(report['generation_complete'])
                self.assertTrue(fixture.processes[3].terminated)
                self.assertFalse(foreign.terminated)
                self.assertTrue((fixture.output / 'generator-000/manifest.json').is_file())
                self.assertFalse(any('-verify-ledger' in command for command, _ in fixture.calls))

    def test_unavailable_rss_for_running_child_is_explicit_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            report = dict(phase='generation', samples=[])
            supervisor = runner.Supervisor(Path(directory), report, {})
            supervisor.children.append(dict(process=Process(90000, None), role='generator', generator=0, exit_code=None))
            with patch.object(runner.shutil, 'disk_usage', return_value=argparse.Namespace(free=32 * runner.GIB)), \
                 patch.object(runner.subprocess, 'run', return_value=argparse.Namespace(returncode=1, stdout='')):
                with self.assertRaisesRegex(RuntimeError, 'RSS unavailable'):
                    supervisor.sample()

    def test_cleanup_reaps_real_owned_child(self):
        with tempfile.TemporaryDirectory() as directory:
            supervisor = runner.Supervisor(Path(directory), dict(phase='test', samples=[]), runner.child_environment())
            try:
                child = supervisor.spawn([sys.executable, '-c', 'import time; time.sleep(30)'], 'cancel-fixture')
                self.assertIsNone(child['process'].poll())
                self.assertEqual(supervisor.cleanup(), [])
                self.assertIsNotNone(child['process'].poll())
            finally:
                supervisor.cleanup()

    def test_registration_precedes_delivered_interrupt(self):
        with tempfile.TemporaryDirectory() as directory:
            supervisor = runner.Supervisor(Path(directory), dict(phase='test', samples=[]), {})
            process = Process(90000, None)
            # Simulate SIGTERM delivered immediately when the parent's blocked
            # mask is restored. The child must already be in its cleanup registry.
            with patch.object(runner.subprocess, 'Popen', return_value=process), \
                 patch.object(runner.signal, 'pthread_sigmask', side_effect=[set(), KeyboardInterrupt]):
                with self.assertRaises(KeyboardInterrupt):
                    supervisor.spawn(['fixture'], 'fixture')
            self.assertEqual(len(supervisor.children), 1)
            self.assertEqual(supervisor.cleanup(), [])
            self.assertTrue(process.terminated)

    def test_cli_indices_and_new_private_output(self):
        self.assertEqual(runner.indices('2,0'), [0, 2])
        for value in ('', '0,0', '-1', '01', '128', '0, 1'):
            with self.assertRaises(RuntimeError):
                runner.indices(value)
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(directory)
            fixture.output.mkdir()
            with self.assertRaises(FileExistsError):
                fixture.run(preflight=True)


if __name__ == '__main__':
    unittest.main()
