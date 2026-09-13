#!/usr/bin/env python3
"""Bounded local HTTP profile against an owned real application and isolated poll.

Never deletes data or controls existing applications/brokers. Each profile owns
its app, generator, config and private ledgers. Kafka metadata uses a new schema.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import fcntl
from functools import partial
import math
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import re
import resource
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

RESERVE = 8 * 1024**3
MAX_KAFKA_LAB = 26 * 1024**3
CHILD_OPEN_FILES = 16384


def child_limits(open_files=CHILD_OPEN_FILES, signal_mask=None):
    # Only the freshly forked benchmark child is changed, never this shell,
    # another service or a system-wide sysctl. 4096 workers use ledger + socket.
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (open_files, hard))
    if signal_mask is not None:
        signal.pthread_sigmask(signal.SIG_SETMASK, signal_mask)


def spawn_owned(command, registry, open_files, **kwargs):
    # Registration must precede delivery of a pending Ctrl-C/SIGTERM. Otherwise
    # Popen can return a live PID just as the handler raises, before assignment.
    # This harness targets Unix hosts (flock/resource were already required).
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
    try:
        kwargs['preexec_fn'] = partial(child_limits, open_files, previous)
        process = subprocess.Popen(command, **kwargs)
        registry.append(process)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    return process


def now():
    return datetime.now(timezone.utc).isoformat()


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def private_json(path, value):
    with path.open('x') as f:
        json.dump(value, f, indent=2, ensure_ascii=False)
        f.write('\n')
    path.chmod(0o600)


def process_samples(roles):
    if not roles:
        return []
    p = subprocess.run(['ps', '-o', 'pid=,time=,rss=', '-p', ','.join(map(str, roles.values()))],
                       capture_output=True, text=True, timeout=3)
    result = []
    for line in p.stdout.splitlines():
        parts = line.split()
        if len(parts) == 3:
            pid = int(parts[0])
            role = next((name for name, value in roles.items() if value == pid), 'unknown')
            result.append({'role': role, 'pid': pid, 'cpu_time': parts[1], 'rss_bytes': int(parts[2]) * 1024})
    return result


def local_database(url, runtime):
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ('postgres', 'postgresql'):
        return False
    query = urllib.parse.parse_qs(u.query)
    if set(query) - {'host', 'port', 'sslmode', 'connect_timeout'}:
        return False
    hosts = query.get('host', [u.hostname or ''])
    allowed = {'127.0.0.1', '::1', str(runtime / '.local')}
    return (u.hostname in (None, '127.0.0.1', '::1') and len(hosts) == 1 and hosts[0] in allowed)



def psql_environment(database):
    # PGDATABASE is a database name, not a libpq connection URI. Split the
    # already loopback-validated fixture DSN without putting secrets in argv.
    parsed = urllib.parse.urlsplit(database)
    query = urllib.parse.parse_qs(parsed.query)
    env = {k: v for k, v in os.environ.items() if not k.startswith('PG')}
    env['PGDATABASE'] = urllib.parse.unquote(parsed.path.lstrip('/'))
    env['PGHOST'] = query.get('host', [parsed.hostname or ''])[0]
    env['PGPORT'] = query.get('port', [str(parsed.port or 5432)])[0]
    if parsed.username is not None:
        env['PGUSER'] = urllib.parse.unquote(parsed.username)
    if parsed.password is not None:
        env['PGPASSWORD'] = urllib.parse.unquote(parsed.password)
    env['PGSSLMODE'] = query.get('sslmode', ['prefer'])[0]
    env['PGCONNECT_TIMEOUT'] = query.get('connect_timeout', ['10'])[0]
    return env


def plan_bytes(rate, repeat_every, mode, partitions=4, generators=1, workers=256):
    attempts = rate * 60 + (rate * 60 // repeat_every if repeat_every else 0)
    # Conservative old single-record encoding, RF3, index/protocol headroom,
    # 64-byte private client records and fixed service/segment allowance.
    per_attempt = (68 if mode == 'simple-files' else 96 * 3 * 2) + 80
    indexes = max(512 * 1024**2, partitions * 3 * 2 * 1024**2 + 64 * 1024**2) if mode == 'postgres-kafka' else 512 * 1024**2
    private_inventory = generators * (workers + 1) * 4096 + (1024**2 if generators > 1 else 0)
    return attempts, RESERVE + attempts * per_attempt + indexes + private_inventory



def local_budget(args):
    generators = getattr(args, 'generators', 1)
    workers = getattr(args, 'workers', 256)
    queue = getattr(args, 'queue', 4096)
    check(1 <= generators <= 4, 'local generator count must be 1..4')
    check(1 <= workers <= 4096 and 1 <= queue <= 1_000_000, 'invalid per-generator resource bounds')
    total_workers = generators * workers
    # A child generator owns one ledger and one connection per worker. The
    # application can receive connections from every generator simultaneously.
    open_files = max(CHILD_OPEN_FILES, 2 * total_workers + 512)
    return {'generators': generators, 'workers_per_generator': workers,
            'workers_total': total_workers, 'queue_per_generator': queue,
            'queue_total': generators * queue,
            'generator_GOMEMLIMIT_total_bytes': generators * 2 * 1024**3,
            'application_GOMEMLIMIT_bytes': int(getattr(args, 'app_memory', '3GiB')[0]) * 1024**3,
            'memory_limit_semantics': 'Go soft heap limits, not RSS caps or RAM reservations; excludes shared brokers and runtime overhead',
            'estimated_owned_open_files_total': 3 * total_workers + 512 * (generators + 1),
            'owned_child_open_file_limit': open_files}


def require_plan_support(binary, reader=False):
    try:
        result = subprocess.run([str(binary), '-h'], stdin=subprocess.DEVNULL,
                                capture_output=True, text=True, timeout=5)
    except subprocess.TimeoutExpired:
        raise RuntimeError('binary plan-capability probe exceeded its five-second deadline') from None
    help_text = result.stdout + result.stderr
    required = ('audit-plan', 'ledgers') if reader else ('prepare-plan', 'total-unique', 'generators', 'plan', 'generator')
    check(result.returncode == 0 and len(help_text) <= 65536 and
          all(re.search(r'(?m)^\s+-' + flag + r'(?:\s|$)', help_text) for flag in required),
          'multiple generators require plan-capable load and audit binaries; frozen POST-only binaries support --generators 1 only')


LOAD_SUM_FIELDS = (
    'planned_attempts', 'planned_unique_keys', 'http_post_sent', 'valid_recorded_ack',
    'unknown', 'closed', 'not_admitted', 'not_open', 'rejected', 'generator_skipped',
    'journey_failed', 'journey_started', 'journey_completed_before_post', 'http_get_sent',
    'http_get_failures', 'http_get_body_bytes', 'http_get_decoded_body_bytes',
    'invalid_successful_responses', 'ledger_bytes',
)
OUTCOME_FIELDS = ('valid_recorded_ack', 'unknown', 'closed', 'not_admitted', 'not_open',
                  'rejected', 'generator_skipped', 'journey_failed')
GET_SUM_FIELDS = ('sent', 'failures', 'response_body_bytes', 'decoded_body_bytes',
                  'timeouts', 'transport_failures', 'status_failures', 'validation_failures')


def aggregate_load_reports(loads, attempts, unique):
    check(1 < len(loads) <= 4, 'invalid generator report inventory')
    for load in loads:
        check(load.get('complete') is True and load.get('cancelled') is False, 'a generator did not finish its complete plan')
        check(all(type(load.get(key, 0)) is int and load.get(key, 0) >= 0 for key in LOAD_SUM_FIELDS),
              'invalid generator counter type')
        check(sum(load.get(key, 0) for key in OUTCOME_FIELDS) == load.get('planned_attempts'),
              'generator outcomes do not cover its planned attempts')
        check(all(load.get(key) == loads[0].get(key) for key in ('mode', 'repeat_every', 'max_lag_ms', 'scheduled_seconds', 'post_latency_scope')),
              'incompatible generator report modes or windows')
    result = {key: sum(load.get(key, 0) for load in loads) for key in LOAD_SUM_FIELDS}
    check(result['planned_attempts'] == attempts and result['planned_unique_keys'] == unique,
          'combined generator population differs from the single profile plan')
    result.update(mode=loads[0]['mode'], complete=True, cancelled=False, generators=len(loads),
                  planned_unique_per_second=unique / 60, scheduled_seconds=60,
                  repeat_every=loads[0]['repeat_every'], max_lag_ms=loads[0]['max_lag_ms'],
                  workers=sum(load['workers'] for load in loads), queue=sum(load['queue'] for load in loads),
                  post_latency_scope=loads[0]['post_latency_scope'],
                  method='Sum of disjoint ranges in one private plan; one independent reader reconciles every range. No combined latency quantiles are inferred from per-generator quantiles.',
                  latency_quantiles='See individual load reports; histograms are not exported by the generator.')
    for value, weight in (('latency_mean_ms', 'http_post_sent'), ('journey_mean_ms', 'journey_started')):
        check(all(isinstance(load.get(value, 0), (int, float)) and math.isfinite(load.get(value, 0)) and load.get(value, 0) >= 0 for load in loads),
              'invalid generator latency')
        result[value] = sum(load.get(value, 0) * load.get(weight, 0) for load in loads) / result[weight] if result[weight] else 0
    for key in ('latency_max_ms', 'journey_max_ms'):
        result[key] = max(load.get(key, 0) for load in loads)
    for key in ('post_sent_per_scheduled_second', 'ack_received_per_scheduled_second'):
        check(all(len(load.get(key, [])) == 60 and all(type(x) is int and x >= 0 for x in load[key]) for load in loads),
              'invalid generator per-second inventory')
        result[key] = [sum(load[key][i] for load in loads) for i in range(60)]
    stages = [load.get('http_get_stages', []) for load in loads]
    check(all([stage.get('stage') for stage in x] == [stage.get('stage') for stage in stages[0]] for x in stages),
          'generator GET stages differ')
    if stages[0]:
        result['http_get_stages'] = []
        for i, first in enumerate(stages[0]):
            members = [x[i] for x in stages]
            check(all(type(m.get(key, 0)) is int and m.get(key, 0) >= 0 for m in members for key in GET_SUM_FIELDS),
                  'invalid GET stage counters')
            combined = {key: sum(m.get(key, 0) for m in members) for key in GET_SUM_FIELDS}
            combined.update(stage=first['stage'], max_latency_ms=max(m.get('max_latency_ms', 0) for m in members))
            combined['mean_latency_ms'] = sum(m.get('mean_latency_ms', 0) * m['sent'] for m in members) / combined['sent'] if combined['sent'] else 0
            result['http_get_stages'].append(combined)
    return result


def check_manifest_identity(path, args, poll_id, origin, index, generators):
    # The summary cannot establish which population the binary really used.
    # Read only its bounded private header; the separate Go reader validates
    # every ledger record, digest, timestamp and journal relationship later.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        info = os.fstat(source.fileno())
        check(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) & 0o077 == 0 and info.st_size <= 2 * 1024**2,
              'generator manifest is not a bounded private regular file')
        header = json.loads(source.read(2 * 1024**2 + 1))
    check(type(header) is dict and header.get('version') == 1 and header.get('complete') is True,
          'generator manifest is incomplete')
    config = header.get('config', {})
    check(type(config) is dict and type(header.get('poll')) is dict and
          config.get('poll_id') == poll_id and header['poll'].get('id') == poll_id and config.get('url') == origin,
          'generator manifest targets a different poll or origin')
    total = args.rate * 60
    keys = total // generators + (index < total % generators)
    offset = (total // generators) * index + min(index, total % generators)
    expected = {'unique_rate': args.rate, 'key_offset': offset, 'duration_ns': 60_000_000_000,
                'repeat_every': args.repeat_every, 'workers': args.workers, 'queue': args.queue,
                'max_lag_ns': args.max_lag_ms * 1_000_000}
    check(all(type(config.get(key, 0)) is int and config.get(key, 0) == value for key, value in expected.items()),
          'generator manifest range, repeats or resource bounds differ from the requested profile')
    count = config.get('unique_count', 0)
    check(type(count) is int and count >= 0 and (count or config['unique_rate'] * 60) == keys and keys > 0,
          'generator manifest unique population differs from the requested profile')
    for key in ('journey', 'definition'):
        check(type(config.get(key, False)) is bool and config.get(key, False) == getattr(args, key, False),
              'generator manifest HTTP mode differs from the requested profile')


def allocated_bytes(root):
    return sum(p.stat().st_blocks * 512 for p in root.rglob('*') if p.is_file() and not p.is_symlink())


def run(args):
    os.umask(0o077)
    source = Path(__file__).resolve().parents[1]
    runtime = Path(args.runtime_root).resolve() if args.runtime_root else source
    check(runtime.is_dir(), 'runtime directory must exist')
    budget = local_budget(args)
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    check(hard == resource.RLIM_INFINITY or hard >= budget['owned_child_open_file_limit'], 'hard open-file limit is below the combined owned-child budget')
    app = Path(args.server_binary).resolve() if args.server_binary else source / 'bin/gigaquizz'
    generator = Path(args.load_binary).resolve() if args.load_binary else source / 'bin/httpbench'
    reader = Path(args.audit_binary).resolve() if getattr(args, 'audit_binary', None) else source / 'bin/httpbench'
    for binary in (app, generator, reader):
        check(binary.is_file() and os.access(binary, os.X_OK), 'build the application and httpbench first')
    attempts, required = plan_bytes(args.rate, args.repeat_every, args.mode, getattr(args, 'partitions', 4), budget['generators'], budget['workers_per_generator'])
    check(attempts <= 10_000_000, 'maximum 10 million planned attempts per local profile')
    if budget['generators'] > 1:
        require_plan_support(generator)
        require_plan_support(reader, reader=True)
    check(shutil.disk_usage(runtime).free >= required, 'insufficient disk for this profile plus reserve; old data is preserved')
    if args.mode == 'postgres-kafka':
        lab = runtime / '.local/kafka-lab'
        marker = lab / '.gigaquizz-kafka-lab'
        check(not lab.is_symlink() and marker.is_file() and not marker.is_symlink() and
              marker.read_text() == 'gigaquizz native Kafka lab v1\n', 'owned local Kafka lab marker required')
        check(allocated_bytes(lab) + required - RESERVE - attempts * 80 <= MAX_KAFKA_LAB,
              'predicted retained Kafka lab exceeds 26GiB; preserve old data and use a smaller profile')
    base = 'http://127.0.0.1:' + str(args.port)
    with socket.socket() as probe:
        # Like Go's listener, allow reuse after our previous run's TIME_WAIT.
        # An active listener still makes this bind fail; no SO_REUSEPORT.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(('127.0.0.1', args.port))
    root = runtime / '.local/http-full-profiles'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with ExitStack() as resources:
        lock = resources.enter_context((root / 'profile.lock').open('a'))
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.mode == 'postgres-kafka':
            shared = resources.enter_context((runtime / '.local/kafka-lab/profile.lock').open('a'))
            fcntl.flock(shared, fcntl.LOCK_EX | fcntl.LOCK_NB)
        directory = root / args.label
        directory.mkdir(mode=0o700, exist_ok=False)
        return owned_profile(args, source, runtime, app, generator, base, directory, attempts, required, reader)


def owned_profile(args, source, runtime, app, generator, base, directory, attempts, required, reader=None):
    # run() always supplies the current reader separately from a frozen workload.
    reader = reader or generator
    budget = local_budget(args)
    generators = budget['generators']
    owned_children = []
    spawn = partial(spawn_owned, registry=owned_children, open_files=budget['owned_child_open_file_limit'])
    password = secrets.token_urlsafe(32)
    values = {'HTTP_ADDR': '127.0.0.1:' + str(args.port), 'PUBLIC_URL': base,
              'ADMIN_PASSWORD': password, 'DATA_DIR': str(directory / 'data'),
              'FILE_PARTITIONS': str(getattr(args, 'file_partitions', 1)),
              'MAX_INFLIGHT': str(args.max_inflight), 'MAX_UNIQUE_VOTERS': str(args.rate * 60 + 100),
              'MAX_PARTITION_UNIQUE_VOTERS': str(args.rate * 60 + 100)}
    if args.mode == 'postgres-kafka':
        database = os.environ.get('TEST_DATABASE_URL', '')
        check(local_database(database, runtime), 'TEST_DATABASE_URL must target the local test database')
        values.update(DATABASE_URL=database, GIGAQUIZZ_SCHEMA='gqhttpfull_' + secrets.token_hex(8),
                      KAFKA_BROKERS='127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094',
                      KAFKA_PARTITIONS=str(args.partitions))
        for port in (19092, 19093, 19094):
            with socket.create_connection(('127.0.0.1', port), timeout=2):
                pass
    if args.batch_votes is not None:
        values['FILE_BATCH_VOTES' if args.mode == 'simple-files' else 'KAFKA_BATCH_VOTES'] = str(args.batch_votes)
    if args.queue_votes is not None:
        values['FILE_QUEUE_VOTES' if args.mode == 'simple-files' else 'KAFKA_QUEUE_VOTES'] = str(args.queue_votes)
    if args.linger_ms is not None:
        values['FILE_GROUP_LINGER' if args.mode == 'simple-files' else 'KAFKA_LINGER_MS'] = str(args.linger_ms) + ('ms' if args.mode == 'simple-files' else '')
    config = directory / 'service.env'
    with config.open('x') as f:
        f.write(''.join(k + '=' + v + '\n' for k, v in values.items()))
    config.chmod(0o600)
    env = os.environ.copy()
    # Local fixture configuration wins over the caller's normal application.
    for key in tuple(env):
        if key.startswith(('KAFKA_', 'FILE_', 'DURABILITY_', 'PG')) or key in values or key in ('POLL_PREPARATION_SECONDS', 'VOTE_DB_CONNECTIONS', 'MAX_STORED_POLLS'):
            env.pop(key, None)
    env.update(values, GOMEMLIMIT=args.app_memory, GOGC='100', GOMAXPROCS=str(os.cpu_count() or 1))
    env.pop('TEST_DATABASE_URL', None)
    anonymous = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    admin = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                       urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    child = workload = None
    generator_children = []
    app_starts = 0
    stage = 'startup'
    profile_started = time.monotonic()
    poll_window = None
    broker_pids = {}
    if args.mode == 'postgres-kafka':
        metadata = json.loads((runtime / '.local/kafka-lab/metadata.json').read_text())
        for key, node in metadata['nodes'].items():
            pid = node.get('pid')
            if isinstance(pid, int) and pid > 1:
                probe = subprocess.run(['ps', '-o', 'command=', '-p', str(pid)], capture_output=True, text=True, timeout=3)
                if str(runtime / '.local/kafka-lab') in probe.stdout and 'kafka.Kafka' in probe.stdout:
                    broker_pids['shared_kafka_' + key] = pid
    report = {'mode': args.mode, 'label': args.label, 'started_at': now(), 'correct': False,
              'scope': 'Real local HTTP application, isolated real 60-second poll, exact private-ledger audit, process restart; no TLS/CDN capacity claim',
              'diagnostic_cpu_profile': getattr(args, 'cpu_profile', False),
              'configuration': {'rate_unique_s': args.rate, 'planned_attempts': attempts,
                                'workers': args.workers, 'generator_queue': args.queue, 'generators': generators,
                                'combined_resource_budget': budget,
                                'max_lag_ms': args.max_lag_ms, 'repeat_every': args.repeat_every,
                                'journey': getattr(args, 'journey', False),
                                'definition': getattr(args, 'definition', False),
                                'file_partitions': getattr(args, 'file_partitions', 1) if args.mode == 'simple-files' else None,
                                'max_partition_unique_voters': args.rate * 60 + 100 if args.mode == 'simple-files' else None,
                                'max_inflight': args.max_inflight, 'kafka_partitions': args.partitions if args.mode == 'postgres-kafka' else None,
                                'batch_votes_override': args.batch_votes, 'queue_votes_override': args.queue_votes,
                                'linger_ms_override': args.linger_ms, 'app_GOMEMLIMIT': args.app_memory,
                                'generator_GOMEMLIMIT': '2GiB', 'GOGC': '100', 'GOMAXPROCS': os.cpu_count() or 1,
                                'owned_child_open_file_limit': budget['owned_child_open_file_limit']},
              'binary_sha256': {'application': hashlib.sha256(app.read_bytes()).hexdigest(),
                                'generator': hashlib.sha256(generator.read_bytes()).hexdigest(),
                                'reader': hashlib.sha256(reader.read_bytes()).hexdigest()},
              'disk': {'required_bytes_including_reserve': required, 'reserve_bytes': RESERVE,
                       'free_before': shutil.disk_usage(runtime).free},
              'samples': [], 'checks': [], 'errors': []}

    def request(path, body=None, authenticated=False):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base + path, data=data,
                                     headers={'Content-Type': 'application/json', 'Origin': base})
        try:
            with (admin if authenticated else anonymous).open(req, timeout=20) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as e:
            with e:
                return e.code, json.loads(e.read(65536))

    def sample():
        free = shutil.disk_usage(runtime).free
        roles = dict(broker_pids)
        for role, process in (('application', child), ('generator_or_reader', workload)):
            if process is not None and process.poll() is None:
                roles[role] = process.pid
        for i, process in enumerate(generator_children):
            if process.poll() is None:
                roles['generator_' + str(i).zfill(3)] = process.pid
        measured_stage = stage
        at = datetime.now(timezone.utc)
        if stage == 'workload' and poll_window:
            if at < poll_window[0]:
                measured_stage = 'preparation'
            elif at >= poll_window[1]:
                measured_stage = 'drain'
        report['samples'].append({'at': at.isoformat(), 'elapsed_seconds': time.monotonic() - profile_started,
                                  'phase': measured_stage, 'free_disk_bytes': free, 'processes': process_samples(roles)})
        check(free >= RESERVE, 'free disk crossed 8GiB reserve; profile stopped and data preserved')

    def phase(name):
        nonlocal stage
        stage = name
        print(json.dumps({'label': args.label, 'stage': name}), flush=True)

    def stop(process):
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError('owned child exceeded graceful shutdown deadline')

    def start(log):
        nonlocal child, app_starts
        app_starts += 1
        command = [str(app), '-env', str(config)]
        if getattr(args, 'cpu_profile', False):
            command += ['-cpu-profile', str(directory / ('cpu-' + str(app_starts) + '.pprof'))]
        child = spawn(command, cwd=source, env=env,
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        until = time.monotonic() + 90
        while time.monotonic() < until:
            check(child.poll() is None, 'owned application exited; inspect private application.log')
            try:
                if request('/readyz')[0] == 200:
                    check(request('/api/admin/login', {'password': password}, True)[0] == 200, 'admin login failed')
                    return
            except OSError:
                pass
            time.sleep(.2)
        raise RuntimeError('owned application readiness deadline exceeded')

    def wait_processes(processes, seconds, label):
        deadline = time.monotonic() + seconds
        while True:
            statuses = [process.poll() for process in processes]
            check(all(status is None or status == 0 for status in statuses), label + ' failed; inspect private report')
            if all(status is not None for status in statuses):
                return
            check(child.poll() is None, 'application exited during ' + label)
            check(time.monotonic() < deadline, label + ' exceeded bounded deadline')
            sample()
            time.sleep(1)

    def wait_final(results_path):
        deadline = time.monotonic() + 180
        while True:
            status, result = request(results_path, authenticated=True)
            if status == 200 and result.get('state') == 'final' and not result.get('pending'):
                return result
            check(child.poll() is None, 'application exited while finalization was pending')
            check(time.monotonic() < deadline, 'application finalization exceeded bounded deadline')
            sample()
            time.sleep(1)

    with (directory / 'application.log').open('xb') as log:
        try:
            phase('startup')
            start(log)
            starts = datetime.now(timezone.utc) + timedelta(seconds=35 if args.mode == 'postgres-kafka' else 20 if generators > 1 else 8)
            status, poll = request('/api/admin/polls', {'question': 'HTTP throughput and exact recovery', 'type': 'single',
                                  'options': ['First', 'Second'], 'starts_at': starts.isoformat()}, True)
            check(status == 201, 'isolated poll creation failed')
            uuid.UUID(poll['id'])
            private_json(directory / 'poll.json', poll)
            check((datetime.fromisoformat(poll['ends_at'].replace('Z', '+00:00')) -
                   datetime.fromisoformat(poll['starts_at'].replace('Z', '+00:00'))).total_seconds() == 60, 'poll window changed')
            poll_window = (datetime.fromisoformat(poll['starts_at'].replace('Z', '+00:00')),
                           datetime.fromisoformat(poll['ends_at'].replace('Z', '+00:00')))
            report['poll_window'] = {'starts_at': poll['starts_at'], 'ends_at': poll['ends_at']}
            report['checks'].append('real_60_second_window')
            command = [str(generator), '-url', base, '-poll', poll['id'], '-rate', str(args.rate),
                       '-duration', '60s', '-start-at', poll['starts_at'], '-workers', str(args.workers),
                       '-queue', str(args.queue), '-max-lag', str(args.max_lag_ms) + 'ms',
                       '-ledger-dir', str(directory / 'ledger'), '-repeat-every', str(args.repeat_every), '-allow-high-load']
            if getattr(args, 'journey', False):
                command.append('-journey')
            if getattr(args, 'definition', False):
                command.append('-definition')
            gen_env = os.environ.copy()
            for key in tuple(gen_env):
                if key.startswith(('KAFKA_', 'FILE_', 'DURABILITY_', 'PG')) or key in ('POLL_PREPARATION_SECONDS', 'VOTE_DB_CONNECTIONS', 'MAX_STORED_POLLS'):
                    gen_env.pop(key, None)
            gen_env.update(GOMEMLIMIT='2GiB', GOGC='100', GOMAXPROCS=str(os.cpu_count() or 1))
            gen_env.pop('TEST_DATABASE_URL', None)
            gen_env.pop('ADMIN_PASSWORD', None)
            plan_path = directory / 'plan.json'
            ledger_root = directory / 'ledger'
            if generators > 1:
                ledger_root.mkdir(mode=0o700)
                prepare = command[:]
                prepare.remove('-allow-high-load')
                index = prepare.index('-ledger-dir')
                del prepare[index:index + 2]
                prepare += ['-prepare-plan', str(plan_path), '-total-unique', str(args.rate * 60), '-generators', str(generators)]
                phase('prepare_plan')
                with (directory / 'plan-report.json').open('x') as output, (directory / 'plan.stderr').open('x') as error:
                    workload = spawn(prepare, cwd=source, env=gen_env, stdin=subprocess.DEVNULL,
                                                stdout=output, stderr=error)
                    wait_processes([workload], 30, 'plan preparation')
                prepared = json.loads((directory / 'plan-report.json').read_text())
                check(prepared.get('complete') is True and prepared.get('generators') == generators and
                      prepared.get('planned_unique_keys') == args.rate * 60 and prepared.get('planned_attempts') == attempts and
                      prepared.get('http_votes_sent') == 0, 'prepared plan differs from the bounded profile population')
                report['checks'].append('disjoint_global_plan_prepared')
                workload = None
            phase('workload')
            if generators == 1:
                with (directory / 'load.json').open('x') as output, (directory / 'load.stderr').open('x') as error:
                    workload = spawn(command, cwd=source, env=gen_env, stdin=subprocess.DEVNULL,
                                                stdout=output, stderr=error)
                    wait_processes([workload], 150, 'HTTP generator')
                report['load'] = json.loads((directory / 'load.json').read_text())
            else:
                with ExitStack() as streams:
                    for i in range(generators):
                        name = 'load-' + str(i).zfill(3)
                        output = streams.enter_context((directory / (name + '.json')).open('x'))
                        error = streams.enter_context((directory / (name + '.stderr')).open('x'))
                        launch = [str(generator), '-plan', str(plan_path), '-generator', str(i),
                                  '-ledger-dir', str(ledger_root / ('generator-' + str(i).zfill(3))), '-allow-high-load']
                        generator_children.append(spawn(launch, cwd=source, env=gen_env, stdin=subprocess.DEVNULL,
                                                                   stdout=output, stderr=error))
                    wait_processes(generator_children, 150, 'HTTP generators')
                loads = [json.loads((directory / ('load-' + str(i).zfill(3) + '.json')).read_text()) for i in range(generators)]
                report['generator_loads'] = loads
                report['load'] = aggregate_load_reports(loads, attempts, args.rate * 60)
                private_json(directory / 'load.json', report['load'])
            check(report['load'].get('complete') is True and report['load'].get('cancelled') is False,
                  'generator did not finish its full planned window')
            check(report['load'].get('planned_attempts') == attempts, 'generator plan differs from the profile disk/population plan')
            load = report['load']
            expected_mode = ('http-journey-definition-gzip' if getattr(args, 'definition', False) else
                             'http-journey' if getattr(args, 'journey', False) else 'http-post')
            check(load.get('planned_unique_keys') == args.rate * 60 and
                  load.get('repeat_every') == args.repeat_every and load.get('scheduled_seconds') == 60 and
                  load.get('mode') == expected_mode and load.get('workers') == budget['workers_total'] and
                  load.get('queue') == budget['queue_total'] and load.get('max_lag_ms') == args.max_lag_ms,
                  'generator population, mode or resource bounds differ from the requested profile')
            check(all(type(load.get(key, 0)) is int and load.get(key, 0) >= 0 for key in LOAD_SUM_FIELDS) and
                  sum(load.get(key, 0) for key in OUTCOME_FIELDS) == attempts and
                  load.get('http_post_sent') == attempts - load.get('generator_skipped', 0) - load.get('journey_failed', 0),
                  'generator outcomes and POST count do not cover the requested attempts')
            for i in range(generators):
                manifest_dir = ledger_root if generators == 1 else ledger_root / ('generator-' + str(i).zfill(3))
                check_manifest_identity(manifest_dir / 'manifest.json', args, poll['id'], base, i, generators)
            report['checks'].append('private_generator_manifests_match_requested_profile')
            phase('final_result')
            results_path = '/api/admin/polls/' + poll['id'] + '/results'
            result = wait_final(results_path)
            private_json(directory / 'results.json', result)
            report['final_unique_votes'] = result['total_votes']
            report['final_choice_counts'] = [x['votes'] for x in result['options']]
            report['checks'].append('production_final_result_ready')
            status, metrics = request('/api/admin/metrics', authenticated=True)
            check(status == 200, 'metrics unavailable')
            report['application_metrics'] = metrics
            phase('restart')
            stop(child)
            start(log)
            after = wait_final(results_path)
            check(after == result, 'final result changed after application restart')
            check(request('/api/polls/' + poll['id'] + '/votes', {'token': secrets.token_hex(16), 'choices': [1]})[0] == 410,
                  'restarted closed poll admitted a late vote')
            report['checks'] += ['final_result_unchanged_after_process_restart', 'late_post_410']
            stop(child)
            audit = [str(reader), '-results', str(directory / 'results.json')]
            if generators == 1:
                audit += ['-audit-only', str(directory / 'ledger/manifest.json')]
            else:
                audit += ['-audit-plan', str(plan_path), '-ledgers', str(ledger_root)]
            if args.mode == 'simple-files':
                audit += ['-file-journal', str(directory / 'data/polls' / poll['id'])]
            else:
                psql = args.psql or shutil.which('psql') or '/Applications/Postgres.app/Contents/Versions/latest/bin/psql'
                check(Path(psql).is_file(), 'psql is required to read the isolated journal configuration')
                query = 'SELECT journal FROM "' + values['GIGAQUIZZ_SCHEMA'] + '".polls WHERE id=\'' + poll['id'] + '\''
                sql_env = psql_environment(values['DATABASE_URL'])
                sql = subprocess.run([psql, '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', query], env=sql_env,
                                     capture_output=True, text=True, timeout=10)
                check(sql.returncode == 0, 'could not read the isolated Kafka configuration')
                kafka = json.loads(sql.stdout)
                private_json(directory / 'journal-config.json', kafka)
                audit += ['-kafka-config', str(directory / 'journal-config.json')]
            phase('independent_reader')
            with (directory / 'audit.json').open('x') as output, (directory / 'audit.stderr').open('x') as error:
                workload = spawn(audit, cwd=source, env=gen_env, stdin=subprocess.DEVNULL,
                                            stdout=output, stderr=error)
                deadline = time.monotonic() + 180
                while workload.poll() is None:
                    sample()
                    check(time.monotonic() < deadline, 'independent audit deadline exceeded')
                    time.sleep(1)
                check(workload.returncode == 0, 'independent audit failed; inspect private report')
            report['audit'] = json.loads((directory / 'audit.json').read_text())
            check(report['audit'].get('correct') is True and report['audit'].get('saved_service_results_matched') is True,
                  'independent reader did not certify the saved result')
            check(report['audit'].get('matched_ack_attempts') == report['load']['valid_recorded_ack'] and
                  report['audit'].get('missing_ack_attempts') == 0 and report['audit'].get('unverified_ack_attempts') == 0,
                  'independent reader did not verify every confirmed HTTP attempt')
            if args.mode == 'postgres-kafka':
                inspection = report['audit'].get('journal_inspection', {})
                check(inspection.get('method') == 'kafka-stable-snapshot' and
                      inspection.get('strict_snapshot_complete') is True and
                      inspection.get('snapshot_partitions') == args.partitions,
                      'Kafka audit must certify a complete stable snapshot, not only a CLOSED prefix')
            check(report['audit'].get('generators') == generators and report['audit'].get('planned_unique_keys') == args.rate * 60 and
                  report['audit'].get('planned_attempts') == attempts, 'independent reader did not certify every planned generator range')
            report['load_coverage'] = {'acknowledged_attempt_fraction': report['load']['valid_recorded_ack'] / attempts,
                                       'planned_attempts_without_ack': attempts - report['load']['valid_recorded_ack'],
                                       'all_planned_attempts_acknowledged': report['load']['valid_recorded_ack'] == attempts}
            report['checks'].append('independent_exact_journal_audit')
            report['correct'] = True
        except (Exception, KeyboardInterrupt) as error:
            report['errors'].append({'type': type(error).__name__, 'message': str(error) if isinstance(error, RuntimeError) else 'local profile operation failed; inspect private artifacts'})
        finally:
            for process in reversed(owned_children):
                try:
                    stop(process)
                except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
                    report['correct'] = False
                    report['errors'].append({'type': type(error).__name__, 'message': 'owned child cleanup did not complete within its bound'})
            report['finished_at'] = now()
            report['disk']['free_after'] = shutil.disk_usage(runtime).free
            private_json(directory / 'report.json', report)
    print(json.dumps({'label': args.label, 'correct': report['correct'], 'errors': report['errors'],
                      'final_unique_votes': report.get('final_unique_votes'), 'checks': report['checks']}), flush=True)
    return 0 if report['correct'] else 1


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=('simple-files', 'postgres-kafka'), required=True)
    p.add_argument('--label', required=True)
    p.add_argument('--runtime-root')
    p.add_argument('--server-binary')
    p.add_argument('--load-binary')
    p.add_argument('--audit-binary', help='current independent reader; defaults to bin/httpbench even with a frozen load binary')
    p.add_argument('--port', type=int, default=8093)
    p.add_argument('--rate', type=int, default=1000)
    p.add_argument('--workers', type=int, default=256, help='workers per generator; shared-host budget includes every process')
    p.add_argument('--generators', type=int, default=1, help='1..4 local processes sharing the total --rate; >1 requires plan-capable binaries, frozen POST-only binaries are unsupported')
    p.add_argument('--queue', type=int, default=4096)
    p.add_argument('--max-lag-ms', type=int, default=100)
    p.add_argument('--repeat-every', type=int, default=0)
    p.add_argument('--max-inflight', type=int, default=4096)
    p.add_argument('--partitions', type=int, default=4)
    p.add_argument('--file-partitions', type=int, default=1)
    p.add_argument('--batch-votes', type=int)
    p.add_argument('--queue-votes', type=int)
    p.add_argument('--linger-ms', type=int)
    p.add_argument('--app-memory', choices=('2GiB', '3GiB', '4GiB'), default='3GiB')
    p.add_argument('--psql')
    p.add_argument('--journey', action='store_true', help='five sequential cold GETs before each original POST; repeats POST-only; no JS/browser/TLS simulation')
    p.add_argument('--definition', action='store_true', help='journey uses new cacheable definition endpoint; preserved baseline uses old GET')
    p.add_argument('--cpu-profile', action='store_true', help='separate diagnostic run: profile each owned app startup through shutdown; needs an application built with -cpu-profile')
    a = p.parse_args()
    if not re.fullmatch('[a-z0-9_]{1,80}', a.label):
        p.error('label must contain 1..80 lowercase ASCII letters, digits or underscores')
    if not (1024 < a.port <= 65535 and 1 <= a.rate <= 100_000 and 1 <= a.workers <= 4096 and
            1 <= a.generators <= 4 and 1 <= a.queue <= 1_000_000 and 1 <= a.max_lag_ms <= 1000 and 0 <= a.repeat_every <= 100_000 and
            1 <= a.max_inflight <= 100_000 and 1 <= a.partitions <= 256 and 1 <= a.file_partitions <= 256):
        p.error('profile exceeds bounded local limits')
    if a.definition and not a.journey:
        p.error('--definition requires --journey')
    if ((a.batch_votes is not None and not 1 <= a.batch_votes <= (131072 if a.mode == 'simple-files' else 4096)) or
        (a.queue_votes is not None and not 1 <= a.queue_votes <= (1048576 if a.mode == 'simple-files' else 8192)) or
        (a.linger_ms is not None and not 1 <= a.linger_ms <= 1000)):
        p.error('invalid storage overrides')
    try:
        def interrupted(_signum, _frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, interrupted)
        return run(a)
    except (RuntimeError, OSError) as error:
        print(json.dumps({'correct': False, 'error_type': type(error).__name__,
                          'message': str(error) if isinstance(error, RuntimeError) else 'local preflight failed; no shared services were changed'}), flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
