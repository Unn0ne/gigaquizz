#!/usr/bin/env python3
"""Bounded local HTTP profile against an owned real application and isolated poll.

Never deletes data or controls existing applications/brokers. Each profile owns
its app, generator, config and private ledgers. Kafka metadata uses a new schema.
"""
import argparse
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import fcntl
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


def child_limits():
    # Only the freshly forked benchmark child is changed, never this shell,
    # another service or a system-wide sysctl. 4096 workers use ledger + socket.
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (CHILD_OPEN_FILES, hard))


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


def plan_bytes(rate, repeat_every, mode):
    attempts = rate * 60 + (rate * 60 // repeat_every if repeat_every else 0)
    # Conservative old single-record encoding, RF3, index/protocol headroom,
    # 64-byte private client records and fixed service/segment allowance.
    per_attempt = (68 if mode == 'simple-files' else 96 * 3 * 2) + 80
    return attempts, RESERVE + attempts * per_attempt + 512 * 1024**2


def allocated_bytes(root):
    return sum(p.stat().st_blocks * 512 for p in root.rglob('*') if p.is_file() and not p.is_symlink())


def run(args):
    os.umask(0o077)
    source = Path(__file__).resolve().parents[1]
    runtime = Path(args.runtime_root).resolve() if args.runtime_root else source
    check(runtime.is_dir(), 'runtime directory must exist')
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    check(hard == resource.RLIM_INFINITY or hard >= CHILD_OPEN_FILES, 'child open-file budget requires a hard limit of at least 16384')
    app = Path(args.server_binary).resolve() if args.server_binary else source / 'bin/gigaquizz'
    generator = Path(args.load_binary).resolve() if args.load_binary else source / 'bin/httpbench'
    for binary in (app, generator):
        check(binary.is_file() and os.access(binary, os.X_OK), 'build the application and httpbench first')
    attempts, required = plan_bytes(args.rate, args.repeat_every, args.mode)
    check(attempts <= 10_000_000, 'maximum 10 million planned attempts per local profile')
    check(shutil.disk_usage(runtime).free >= required, 'insufficient disk for this profile plus reserve; old data is preserved')
    if args.mode == 'postgres-kafka':
        lab = runtime / '.local/kafka-lab'
        marker = lab / '.gigaquizz-kafka-lab'
        check(not lab.is_symlink() and marker.is_file() and not marker.is_symlink() and
              marker.read_text() == 'gigaquizz native Kafka lab v1\n', 'owned local Kafka lab marker required')
        check(allocated_bytes(lab) + attempts * 96 * 3 * 2 + 512 * 1024**2 <= MAX_KAFKA_LAB,
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
        return owned_profile(args, source, runtime, app, generator, base, directory, attempts, required)


def owned_profile(args, source, runtime, app, generator, base, directory, attempts, required):
    password = secrets.token_urlsafe(32)
    values = {'HTTP_ADDR': '127.0.0.1:' + str(args.port), 'PUBLIC_URL': base,
              'ADMIN_PASSWORD': password, 'DATA_DIR': str(directory / 'data'),
              'MAX_INFLIGHT': str(args.max_inflight), 'MAX_UNIQUE_VOTERS': str(args.rate * 60 + 100)}
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
                                'workers': args.workers, 'generator_queue': args.queue,
                                'max_lag_ms': args.max_lag_ms, 'repeat_every': args.repeat_every,
                                'journey': getattr(args, 'journey', False),
                                'max_inflight': args.max_inflight, 'kafka_partitions': args.partitions if args.mode == 'postgres-kafka' else None,
                                'batch_votes_override': args.batch_votes, 'queue_votes_override': args.queue_votes,
                                'linger_ms_override': args.linger_ms, 'app_GOMEMLIMIT': args.app_memory,
                                'generator_GOMEMLIMIT': '2GiB', 'GOGC': '100', 'GOMAXPROCS': os.cpu_count() or 1,
                                'owned_child_open_file_limit': CHILD_OPEN_FILES},
              'binary_sha256': {'application': hashlib.sha256(app.read_bytes()).hexdigest(),
                                'generator_and_reader': hashlib.sha256(generator.read_bytes()).hexdigest()},
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
        child = subprocess.Popen(command, cwd=source, env=env,
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=log, preexec_fn=child_limits)
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

    with (directory / 'application.log').open('xb') as log:
        try:
            phase('startup')
            start(log)
            starts = datetime.now(timezone.utc) + timedelta(seconds=35 if args.mode == 'postgres-kafka' else 8)
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
            gen_env = os.environ.copy()
            for key in tuple(gen_env):
                if key.startswith(('KAFKA_', 'FILE_', 'DURABILITY_', 'PG')) or key in ('POLL_PREPARATION_SECONDS', 'VOTE_DB_CONNECTIONS', 'MAX_STORED_POLLS'):
                    gen_env.pop(key, None)
            gen_env.update(GOMEMLIMIT='2GiB', GOGC='100', GOMAXPROCS=str(os.cpu_count() or 1))
            gen_env.pop('TEST_DATABASE_URL', None)
            gen_env.pop('ADMIN_PASSWORD', None)
            phase('workload')
            with (directory / 'load.json').open('x') as output, (directory / 'load.stderr').open('x') as error:
                workload = subprocess.Popen(command, cwd=source, env=gen_env, stdin=subprocess.DEVNULL,
                                            stdout=output, stderr=error, preexec_fn=child_limits)
                deadline = time.monotonic() + 150
                while workload.poll() is None:
                    sample()
                    check(child.poll() is None, 'application exited during load')
                    check(time.monotonic() < deadline, 'bounded workload deadline exceeded')
                    time.sleep(1)
                check(workload.returncode == 0, 'HTTP generator failed; inspect private report')
            report['load'] = json.loads((directory / 'load.json').read_text())
            check(report['load'].get('complete') is True and report['load'].get('cancelled') is False,
                  'generator did not finish its full planned window')
            check(report['load'].get('planned_attempts') == attempts, 'generator plan differs from the profile disk/population plan')
            phase('final_result')
            results_path = '/api/admin/polls/' + poll['id'] + '/results'
            deadline = time.monotonic() + 180
            while True:
                status, result = request(results_path, authenticated=True)
                if status == 200 and result.get('state') == 'final' and not result.get('pending'):
                    break
                check(time.monotonic() < deadline, 'application finalization exceeded bounded deadline')
                sample()
                time.sleep(1)
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
            status, after = request(results_path, authenticated=True)
            check(status == 200 and after == result, 'final result changed after application restart')
            check(request('/api/polls/' + poll['id'] + '/votes', {'token': secrets.token_hex(16), 'choices': [1]})[0] == 410,
                  'restarted closed poll admitted a late vote')
            report['checks'] += ['final_result_unchanged_after_process_restart', 'late_post_410']
            stop(child)
            audit = [str(generator), '-audit-only', str(directory / 'ledger/manifest.json'), '-results', str(directory / 'results.json')]
            if args.mode == 'simple-files':
                audit += ['-file-journal', str(directory / 'data/polls' / poll['id'] / 'journal')]
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
                workload = subprocess.Popen(audit, cwd=source, env=gen_env, stdin=subprocess.DEVNULL,
                                            stdout=output, stderr=error, preexec_fn=child_limits)
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
            report['checks'].append('independent_exact_journal_audit')
            report['correct'] = True
        except (Exception, KeyboardInterrupt) as error:
            report['errors'].append({'type': type(error).__name__, 'message': str(error) if isinstance(error, RuntimeError) else 'local profile operation failed; inspect private artifacts'})
        finally:
            for process in (workload, child):
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
    p.add_argument('--port', type=int, default=8093)
    p.add_argument('--rate', type=int, default=1000)
    p.add_argument('--workers', type=int, default=256)
    p.add_argument('--queue', type=int, default=4096)
    p.add_argument('--max-lag-ms', type=int, default=100)
    p.add_argument('--repeat-every', type=int, default=0)
    p.add_argument('--max-inflight', type=int, default=4096)
    p.add_argument('--partitions', type=int, default=4)
    p.add_argument('--batch-votes', type=int)
    p.add_argument('--queue-votes', type=int)
    p.add_argument('--linger-ms', type=int)
    p.add_argument('--app-memory', choices=('2GiB', '3GiB', '4GiB'), default='3GiB')
    p.add_argument('--psql')
    p.add_argument('--journey', action='store_true', help='five sequential cold GETs before each original POST; repeats POST-only; no JS/browser/TLS simulation')
    p.add_argument('--cpu-profile', action='store_true', help='separate diagnostic run: profile each owned app startup through shutdown; needs an application built with -cpu-profile')
    a = p.parse_args()
    if not re.fullmatch('[a-z0-9_]{1,80}', a.label):
        p.error('label must contain 1..80 lowercase ASCII letters, digits or underscores')
    if not (1024 < a.port <= 65535 and 1 <= a.rate <= 100_000 and 1 <= a.workers <= 4096 and
            1 <= a.queue <= 1_000_000 and 1 <= a.max_lag_ms <= 1000 and 0 <= a.repeat_every <= 100_000 and
            1 <= a.max_inflight <= 100_000 and 1 <= a.partitions <= 32):
        p.error('profile exceeds bounded local limits')
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
