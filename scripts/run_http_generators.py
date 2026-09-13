#!/usr/bin/env python3
"""Run assigned HTTP generator ranges on this host; never certifies storage.

No application/SSH/database management, automatic retries, or data deletion.
Keep the runner alive (for example in a persistent terminal when using SSH).
Collect every host's generator-NNN directory for the separate Go audit-plan.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
from functools import partial
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import stat
import subprocess
import sys
import time

GIB = 1024**3
RESERVE = 8 * GIB
HEAP_PER_PROCESS = 2 * GIB
RSS_PER_PROCESS = HEAP_PER_PROCESS + GIB // 2
HOST_HEADROOM = 2 * GIB
MAX_WORKERS = 32768
MAX_QUEUE = 2097152
MAX_JSON = 2 * 1024**2
GRACE_SECONDS = 20
KILL_SECONDS = 5
INTERRUPTS = {signal.SIGINT, signal.SIGTERM, signal.SIGHUP}
OUTCOMES = ('valid_recorded_ack', 'unknown', 'closed', 'not_admitted', 'not_open',
            'rejected', 'generator_skipped', 'journey_failed')


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def indices(value):
    check(isinstance(value, str) and re.fullmatch(r'(?:0|[1-9][0-9]*)(?:,(?:0|[1-9][0-9]*))*', value),
          'indices must be a nonempty comma-separated list of distinct integers')
    result = [int(x) for x in value.split(',')]
    check(len(result) <= 32 and len(set(result)) == len(result) and max(result) < 128,
          'indices must be distinct, below 128, with at most 32 local ranges')
    return sorted(result)


def read_json(path, limit=MAX_JSON):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        info = os.fstat(source.fileno())
        check(stat.S_ISREG(info.st_mode) and info.st_size <= limit,
              'bounded JSON report must be a regular file')
        data = source.read(limit + 1)
    check(len(data) <= limit, 'JSON report exceeded its bound')
    value = json.loads(data)
    check(type(value) is dict, 'JSON report must be an object')
    return value


def write_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as target:
        json.dump(value, target, indent=2, allow_nan=False)
        target.write('\n')
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, path)


def freeze_file(source, target, maximum, private=False, executable=False):
    # Pin the exact bytes that are inspected/executed, even if another shell
    # later rebuilds the original binary or replaces the original plan path.
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    digest = hashlib.sha256()
    with os.fdopen(fd, 'rb') as origin:
        info = os.fstat(origin.fileno())
        check(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= maximum,
              'source artifact must be a bounded nonempty regular file')
        check(not private or stat.S_IMODE(info.st_mode) & 0o077 == 0,
              'source plan must have private permissions')
        check(not executable or info.st_mode & 0o111, 'generator binary must be executable')
        check(shutil.disk_usage(target.parent).free >= RESERVE + info.st_size,
              'insufficient free disk to freeze artifacts while retaining reserve')
        out = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        count = 0
        with os.fdopen(out, 'wb') as frozen:
            while data := origin.read(1024**2):
                count += len(data)
                check(count <= maximum, 'source artifact grew beyond its bound')
                digest.update(data)
                frozen.write(data)
            frozen.flush()
            os.fsync(frozen.fileno())
        after = os.fstat(origin.fileno())
        check(count == info.st_size and (after.st_size, after.st_mtime_ns) == (info.st_size, info.st_mtime_ns),
              'source artifact changed while being frozen')
    target.chmod(0o500 if executable else 0o400)
    return digest.hexdigest(), count


def physical_memory():
    try:
        total = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')
    except (OSError, ValueError):
        result = subprocess.run(['sysctl', '-n', 'hw.memsize'], capture_output=True, text=True, timeout=3)
        check(result.returncode == 0 and result.stdout.strip().isdigit(), 'physical RAM could not be measured')
        total = int(result.stdout.strip())
    check(total > 0, 'physical RAM could not be measured')
    return total


def inspect_ranges(report, selected):
    check(report.get('mode') == 'distributed-plan-inspection' and report.get('complete') is True,
          'Go inspector did not validate the plan')
    digest = report.get('plan_sha256')
    check(isinstance(digest, str) and re.fullmatch('[0-9a-f]{64}', digest), 'inspector omitted the canonical plan digest')
    ranges = report.get('generators')
    check(type(ranges) is list and 1 <= len(ranges) <= 128 and max(selected) < len(ranges),
          'selected generator index is outside the inspected plan')
    end = total_attempts = 0
    for i, item in enumerate(ranges):
        check(type(item) is dict, 'invalid inspected generator entry')
        integer_fields = ('index', 'unique_keys', 'planned_attempts', 'key_offset', 'workers', 'queue',
                          'repeat_every', 'ledger_bytes', 'worker_buffer_bytes')
        check(all(type(item.get(k)) is int and item[k] >= 0 for k in integer_fields), 'invalid inspected range counters')
        check(item['index'] == i and item['key_offset'] == end and 1 <= item['unique_keys'] <= 6000000 and
              1 <= item['workers'] <= 4096 and 1 <= item['queue'] <= 1048576,
              'inspector ranges do not cover one bounded population')
        count = item['unique_keys']
        repeat = item['repeat_every']
        expected = count + ((end + count) // repeat - end // repeat if repeat else 0)
        check(item['planned_attempts'] == expected and expected <= 10000000 and item['ledger_bytes'] == expected * 64,
              'inspected attempt/ledger size differs from exact range repeats')
        check(item['worker_buffer_bytes'] == (item['workers'] + 1) * 65536,
              'inspected worker buffer requirement differs from the supported generator')
        check(all(isinstance(item.get(k), (int, float)) and not isinstance(item[k], bool) and
                  math.isfinite(item[k]) and 0 < item[k] <= bound
                  for k, bound in (('max_lag_ms', 1000), ('request_timeout_ms', 30000))),
              'invalid inspected request deadlines')
        check(type(item.get('journey')) is bool and type(item.get('definition')) is bool and
              (not item['definition'] or item['journey']), 'invalid inspected HTTP mode')
        end += count
        total_attempts += expected
    check(all(type(report.get(k)) is int for k in ('planned_unique_keys', 'planned_attempts', 'scheduled_seconds')) and
          report.get('planned_unique_keys') == end and 1 <= end <= 120000000 and
          report.get('planned_attempts') == total_attempts and total_attempts <= 200000000 and
          report.get('scheduled_seconds') == 60, 'inspected global population or minute differs')
    return [ranges[i] for i in selected]


def resource_budget(ranges, ram, free, hard_fd):
    workers = sum(x['workers'] for x in ranges)
    queue = sum(x['queue'] for x in ranges)
    check(workers <= MAX_WORKERS and queue <= MAX_QUEUE, 'combined host worker/queue bound exceeded')
    rss = len(ranges) * RSS_PER_PROCESS
    check(rss + HOST_HEADROOM <= ram * 3 // 4, 'declared host memory budget exceeds 75 percent of physical RAM')
    files = [max(256, 2 * x['workers'] + 64) for x in ranges]
    check(hard_fd == resource.RLIM_INFINITY or max(files) <= hard_fd, 'hard open-file limit is below a child requirement')
    ledger = sum(x['ledger_bytes'] for x in ranges)
    overhead = sum((x['workers'] + 1) * 4096 for x in ranges) + 64 * 1024**2
    required = ledger + overhead + RESERVE
    check(free >= required, 'insufficient host disk for all selected ledgers plus 8GiB reserve')
    return {'processes': len(ranges), 'workers': workers, 'queue': queue, 'ledger_bytes': ledger,
            'ledger_and_report_overhead_bytes': overhead, 'required_free_disk_bytes': required,
            'reserve_bytes': RESERVE, 'physical_memory_bytes': ram,
            'declared_heap_bytes': HEAP_PER_PROCESS * len(ranges), 'sampled_rss_stop_bytes': rss,
            'host_headroom_bytes': HOST_HEADROOM, 'worker_buffer_bytes': sum(x['worker_buffer_bytes'] for x in ranges),
            'child_open_file_limits': files,
            'memory_method': '2GiB soft Go heap per child +512MiB runtime allowance each +2GiB host headroom <=75% physical RAM. RSS is sampled, not an OS hard cap or a free-memory guarantee.'}


def child_environment():
    # No ambient proxies, service credentials or Go tuning can alter this run.
    keep = ('PATH', 'LANG', 'LC_ALL', 'TZ', 'HOME', 'TMPDIR', 'SSL_CERT_FILE', 'SSL_CERT_DIR')
    env = {k: os.environ[k] for k in keep if k in os.environ}
    env.update(GOMEMLIMIT='2GiB', GOGC='100')
    return env


def child_setup(open_files, old_mask):
    # A lower hard limit is applied only in the fresh child; Go cannot silently
    # raise its soft limit above the reserved child budget later.
    resource.setrlimit(resource.RLIMIT_NOFILE, (open_files, open_files))
    signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)


def clock_verified(value, maximum):
    check(type(value) is dict and value.get('verified') is True and value.get('clock_step_detected') is False,
          'generator clock was not verified')
    check(type(value.get('samples')) is int and type(value.get('valid_samples')) is int and
          value['samples'] == value['valid_samples'] and value['valid_samples'] >= 3,
          'generator clock does not contain three valid samples')
    check(value.get('max_clock_error_ms') == maximum and
          all(isinstance(value.get(k), (int, float)) and not isinstance(value[k], bool) and math.isfinite(value[k])
              for k in ('rtt_ms', 'offset_lower_ms', 'offset_upper_ms')) and
          value['rtt_ms'] >= 0 and -maximum <= value['offset_lower_ms'] <= value['offset_upper_ms'] <= maximum,
          'generator clock uncertainty exceeds the requested bound')


def check_transport(value):
    check(type(value) is dict and value.get('proxy') == 'disabled' and value.get('protocol') == 'http/1.1' and
          value.get('tls_verification') is True, 'generator transport differs from direct verified HTTP/1.1')


def check_range_report(value, item, digest):
    check(all(type(value.get(k)) is int for k in ('generator', 'planned_unique_keys', 'planned_attempts')) and
          value.get('plan_sha256') == digest and value.get('generator') == item['index'] and
          value.get('planned_unique_keys') == item['unique_keys'] and value.get('planned_attempts') == item['planned_attempts'],
          'child report differs from its assigned plan range')


def check_load(value, item, digest, clock_error):
    check_range_report(value, item, digest)
    check(value.get('complete') is True and value.get('cancelled') is False, 'generator did not complete its client ledger')
    mode = 'http-journey-definition-gzip' if item['definition'] else 'http-journey' if item['journey'] else 'http-post'
    check(value.get('mode') == mode and value.get('scheduled_seconds') == 60 and
          value.get('workers') == item['workers'] and value.get('queue') == item['queue'] and
          value.get('max_lag_ms') == item['max_lag_ms'] and value.get('repeat_every') == item['repeat_every'] and
          value.get('ledger_bytes') == item['ledger_bytes'], 'generator changed its workload mode or resource bounds')
    check(all(type(value.get(k, 0)) is int and value.get(k, 0) >= 0 for k in OUTCOMES) and
          sum(value.get(k, 0) for k in OUTCOMES) == item['planned_attempts'] and
          value.get('http_post_sent') == item['planned_attempts'] - value.get('generator_skipped', 0) - value.get('journey_failed', 0),
          'generator outcomes do not cover its complete range')
    check(type(value.get('invalid_successful_responses')) is int and value['invalid_successful_responses'] == 0,
          'generator observed an invalid successful HTTP response')
    clock_verified(value.get('clock'), clock_error)
    check_transport(value.get('transport'))


class Supervisor:
    def __init__(self, output, report, environment):
        self.output, self.report, self.environment = output, report, environment
        self.children = []
        self.streams = ExitStack()
        self.rss_limit = RSS_PER_PROCESS

    def spawn(self, command, role, index=None, open_files=256, filename=None):
        filename = filename or role
        handles = []
        for suffix in ('.json', '.stderr'):
            fd = os.open(self.output / (filename + suffix), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            handles.append(self.streams.enter_context(os.fdopen(fd, 'wb')))
        out, err = handles
        old = signal.pthread_sigmask(signal.SIG_BLOCK, INTERRUPTS)
        try:
            process = subprocess.Popen(command, env=self.environment, stdin=subprocess.DEVNULL,
                                       stdout=out, stderr=err, start_new_session=True,
                                       preexec_fn=partial(child_setup, open_files, old))
            child = {'process': process, 'role': role, 'generator': index,
                     'report_file': filename + '.json', 'exit_code': None}
            self.children.append(child)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old)
        return child

    def active(self):
        active = []
        for child in self.children:
            child['exit_code'] = child['process'].poll()
            if child['exit_code'] is None:
                active.append(child)
        return active

    def sample(self):
        active = self.active()
        free = shutil.disk_usage(self.output).free
        check(free >= RESERVE, 'free host disk crossed the 8GiB reserve')
        if not active:
            return
        result = subprocess.run(['ps', '-o', 'pid=,time=,rss=', '-p', ','.join(str(c['process'].pid) for c in active)],
                                capture_output=True, text=True, timeout=3)
        check(result.returncode in (0, 1) and len(result.stdout) <= 65536, 'owned process resource sampling failed')
        wanted = {c['process'].pid: c for c in active}
        samples = {}
        for line in result.stdout.splitlines():
            parts = line.split()
            check(len(parts) == 3 and parts[0].isdigit() and parts[2].isdigit(), 'owned process resource sampling was malformed')
            pid = int(parts[0])
            check(pid in wanted and pid not in samples, 'resource sample contained an unexpected process')
            samples[pid] = {'role': wanted[pid]['role'], 'generator': wanted[pid]['generator'],
                            'cpu_time': parts[1], 'rss_bytes': int(parts[2]) * 1024}
        check(all(c['process'].pid in samples or c['process'].poll() is not None for c in active),
              'RSS unavailable for a running owned child; capacity measurement incomplete')
        self.report['samples'].append({'at': utc_now(), 'phase': self.report['phase'], 'free_disk_bytes': free,
                                       'owned_processes': list(samples.values())})
        check(sum(s['rss_bytes'] for s in samples.values()) <= self.rss_limit,
              'sampled owned RSS exceeded the declared host budget')

    def wait(self, children, deadline):
        while True:
            codes = [c['process'].poll() for c in children]
            check(all(code is None or code == 0 for code in codes), 'owned child failed; remaining ranges stopped')
            if all(code is not None for code in codes):
                for child, code in zip(children, codes):
                    child['exit_code'] = code
                return
            check(time.monotonic() < deadline, 'owned phase exceeded its bounded deadline')
            self.sample()
            time.sleep(.5)

    def command(self, command, role, index=None, open_files=256, seconds=30):
        child = self.spawn(command, role, index, open_files,
                           role if index is None else role + '-' + f'{index:03d}')
        self.wait([child], time.monotonic() + seconds)
        return read_json(self.output / child['report_file'])

    def cleanup(self):
        # One shared shutdown deadline, not N times a per-child timeout.
        errors = []
        active = self.active()
        for child in active:
            try:
                child['process'].terminate()
            except ProcessLookupError:
                pass
            except OSError:
                errors.append('owned child could not receive TERM')
        deadline = time.monotonic() + GRACE_SECONDS
        while self.active() and time.monotonic() < deadline:
            time.sleep(.05)
        for child in self.active():
            try:
                child['process'].kill()
                errors.append('owned child required KILL after its graceful deadline')
            except ProcessLookupError:
                pass
            except OSError:
                errors.append('owned child could not receive KILL')
        deadline = time.monotonic() + KILL_SECONDS
        for child in self.children:
            try:
                child['exit_code'] = child['process'].wait(timeout=max(.001, deadline - time.monotonic()))
            except (OSError, subprocess.TimeoutExpired):
                errors.append('owned child could not be reaped within its bound')
        try:
            self.streams.close()
        except OSError:
            errors.append('owned output stream could not be closed')
        return errors


def run(args):
    selected = indices(args.indices)
    check(1 <= args.max_processes <= 32 and len(selected) <= args.max_processes, 'selected ranges exceed --max-processes')
    check(1 <= args.max_clock_error_ms <= 1000, 'clock error must be 1..1000ms')
    output = Path(args.output).absolute()
    parent = output.parent.lstat()
    check(stat.S_ISDIR(parent.st_mode), 'output parent must be an existing real directory')
    output.mkdir(mode=0o700, exist_ok=False)
    report = {'mode': 'host-local-http-generators', 'generation_complete': False, 'preflight_complete': False,
              'preflight_only': args.preflight_only, 'started_at': utc_now(), 'selected_indices': selected,
              'execution_scope': 'Only this host is observed. Separate physical generator/application hosts are not verified.',
              'verification_scope': 'Complete client ledgers only. Storage preservation requires collecting every range and a separate successful audit-plan.',
              'resource_sampling': 'Owned children only; ps cumulative CPU time in native time format and RSS KiB converted to bytes, sampled about every 0.5 seconds. Short-lived helpers may finish before a sample. No whole-host/application resource attribution.',
              'platform': {'system': os.uname().sysname, 'machine': os.uname().machine},
              'max_clock_error_ms': args.max_clock_error_ms, 'phase': 'inspect', 'samples': [], 'checks': [], 'errors': []}
    supervisor = Supervisor(output, report, child_environment())
    try:
        binary = output / 'httpbench'
        report['binary_sha256'], _ = freeze_file(Path(args.binary).resolve(), binary, 256 * 1024**2, executable=True)
        plan = output / 'plan.json'
        report['plan_file_sha256'], _ = freeze_file(Path(args.plan).absolute(), plan, 1024**2, private=True)
        inspected = supervisor.command([str(binary), '-inspect-plan', str(plan)], 'inspection')
        ranges = inspect_ranges(inspected, selected)
        report['plan_sha256'] = inspected['plan_sha256']
        report['plan_generators'] = len(inspected['generators'])
        report['start_at'], report['ends_at'] = inspected['start_at'], inspected['ends_at']
        start = datetime.fromisoformat(inspected['start_at'].replace('Z', '+00:00'))
        end = datetime.fromisoformat(inspected['ends_at'].replace('Z', '+00:00'))
        check(start.tzinfo is not None and end.tzinfo is not None and (end - start).total_seconds() == 60,
              'inspected window is not an explicit-zone minute')
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        budget = resource_budget(ranges, physical_memory(), shutil.disk_usage(output).free, hard)
        report['resource_budget'] = budget
        report['planned_unique_keys'] = sum(x['unique_keys'] for x in ranges)
        report['planned_attempts'] = sum(x['planned_attempts'] for x in ranges)
        supervisor.rss_limit = budget['sampled_rss_stop_bytes']
        clock_flag = str(args.max_clock_error_ms) + 'ms'
        report['phase'] = 'preflight'
        report['preflights'] = []
        for item, fd_limit in zip(ranges, budget['child_open_file_limits']):
            index = item['index']
            command = [str(binary), '-preflight-only', '-plan', str(plan), '-generator', str(index),
                       '-ledger-dir', str(output / f'generator-{index:03d}'), '-max-clock-error', clock_flag]
            preflight = supervisor.command(command, 'preflight', index, fd_limit)
            check(preflight.get('mode') == 'distributed-generator-preflight' and preflight.get('complete') is True,
                  'Go preflight did not approve the selected range')
            check_range_report(preflight, item, report['plan_sha256'])
            check(preflight.get('ledger_bytes') == item['ledger_bytes'] and
                  preflight.get('worker_buffer_bytes') == item['worker_buffer_bytes'] and
                  preflight.get('required_open_files') == 2 * item['workers'] + 64,
                  'preflight resource requirements differ from inspected range')
            clock_verified(preflight.get('clock'), args.max_clock_error_ms)
            check_transport(preflight.get('transport'))
            check(not (output / f'generator-{index:03d}').exists(), 'preflight unexpectedly created a workload ledger')
            report['preflights'].append(preflight)
        report['preflight_complete'] = True
        report['checks'].append('all_selected_ranges_passed_go_preflight')
        if not args.preflight_only:
            report['phase'] = 'generation'
            children = []
            drain = max(x['request_timeout_ms'] for x in ranges) / 1000 + 30
            remaining = end.timestamp() - time.time() + drain
            check(0 < remaining <= 17 * 60, 'original poll start/end exceeds bounded runner schedule')
            deadline = time.monotonic() + remaining
            report['hard_stop_after_end_seconds'] = drain
            for item, fd_limit in zip(ranges, budget['child_open_file_limits']):
                index = item['index']
                command = [str(binary), '-plan', str(plan), '-generator', str(index),
                           '-ledger-dir', str(output / f'generator-{index:03d}'), '-allow-high-load',
                           '-max-clock-error', clock_flag]
                children.append(supervisor.spawn(command, 'generator', index, fd_limit, f'load-{index:03d}'))
            supervisor.wait(children, deadline)
            loads = []
            for item in ranges:
                load = read_json(output / f'load-{item["index"]:03d}.json')
                check_load(load, item, report['plan_sha256'], args.max_clock_error_ms)
                loads.append(load)
            report['phase'] = 'verify_client_ledgers'
            report['ledger_checks'] = []
            verify_deadline = time.monotonic() + 600
            for item, load, fd_limit in zip(ranges, loads, budget['child_open_file_limits']):
                index = item['index']
                command = [str(binary), '-verify-ledger', '-plan', str(plan), '-generator', str(index),
                           '-ledger-dir', str(output / f'generator-{index:03d}')]
                check(time.monotonic() < verify_deadline, 'client ledger verification exceeded its shared deadline')
                verified = supervisor.command(command, 'verify', index, fd_limit,
                                              seconds=min(180, verify_deadline - time.monotonic()))
                check(verified.get('mode') == 'distributed-generator-ledger-verification' and verified.get('complete') is True and
                      verified.get('client_ledger_valid') is True, 'Go did not verify the complete private client ledger')
                check_range_report(verified, item, report['plan_sha256'])
                check(verified.get('ledger_records') == item['planned_attempts'] and
                      all(verified.get(k, 0) == load.get(k, 0) for k in (*OUTCOMES, 'invalid_successful_responses')),
                      'verified client ledger differs from the reported workload outcomes')
                report['ledger_checks'].append(verified)
            report['totals'] = {k: sum(load.get(k, 0) for load in loads) for k in OUTCOMES}
            report['totals']['http_post_sent'] = sum(load['http_post_sent'] for load in loads)
            report['totals']['acknowledged_attempt_fraction'] = report['totals']['valid_recorded_ack'] / report['planned_attempts']
            report['checks'].append('every_selected_manifest_and_client_ledger_verified_against_plan')
            report['generation_complete'] = True
    except (Exception, KeyboardInterrupt) as error:
        report['errors'].append({'type': type(error).__name__,
                                 'message': str(error) if isinstance(error, RuntimeError) else 'local generator operation failed; private artifacts preserved'})
    finally:
        # A second Ctrl-C/SIGTERM must not skip sibling cleanup or report saving.
        old_handlers = {signum: signal.getsignal(signum) for signum in INTERRUPTS}
        for signum in INTERRUPTS:
            signal.signal(signum, signal.SIG_IGN)
        try:
            for message in supervisor.cleanup():
                report['errors'].append({'type': 'CleanupError', 'message': message})
            if report['errors']:
                report['generation_complete'] = False
            report['children'] = [{k: v for k, v in c.items() if k != 'process'} for c in supervisor.children]
            report['finished_at'] = utc_now()
            report['phase'] = 'finished'
            write_json(output / 'report.json', report)
        finally:
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)
    success = not report['errors'] and (report['preflight_complete'] if args.preflight_only else report['generation_complete'])
    print(json.dumps({'generation_complete': report['generation_complete'], 'preflight_complete': report['preflight_complete'],
                      'preflight_only': args.preflight_only, 'selected_generators': len(selected),
                      'planned_attempts': report.get('planned_attempts'), 'errors': report['errors']}), flush=True)
    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--indices', required=True, help='assigned distinct global indices, e.g. 0,2,3')
    parser.add_argument('--binary', required=True, help='plan-capable Go httpbench binary for this host OS/architecture')
    parser.add_argument('--output', required=True, help='new private host-run directory; parent must exist')
    parser.add_argument('--max-clock-error-ms', type=int, default=10)
    parser.add_argument('--max-processes', type=int, default=4, help='explicit local process bound, 1..32')
    parser.add_argument('--preflight-only', action='store_true', help='check the plan, resources and clocks without sending votes')
    args = parser.parse_args()
    os.umask(0o077)
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    for signum in INTERRUPTS:
        signal.signal(signum, interrupted)
    try:
        return run(args)
    except (Exception, KeyboardInterrupt) as error:
        print(json.dumps({'generation_complete': False, 'error_type': type(error).__name__,
                          'message': str(error) if isinstance(error, RuntimeError) else 'local runner preflight failed'}), flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
