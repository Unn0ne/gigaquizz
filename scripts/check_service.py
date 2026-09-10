#!/usr/bin/env python3
"""Full local service check using an owned child and isolated data/schema.

Build bin/gigaquizz first. For postgres-kafka, set TEST_DATABASE_URL and
KAFKA_BROKERS. No existing application is stopped and no prior data is deleted.
"""
import argparse
import concurrent.futures
from datetime import datetime, timedelta, timezone
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def check(condition, description):
    if not condition:
        raise RuntimeError(description)


def run(args):
    root = Path(__file__).resolve().parents[1]
    binary = (root / 'bin/gigaquizz').resolve()
    check(binary.is_file(), 'build bin/gigaquizz first')
    check(1024 < args.port <= 65535, 'port must be 1025..65535')
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', args.port))
    directory = root / '.local/service-checks' / (args.mode + '_' + secrets.token_hex(6))
    directory.mkdir(parents=True, mode=0o700)
    password = secrets.token_urlsafe(32)
    base = 'http://127.0.0.1:' + str(args.port)
    values = {'HTTP_ADDR': '127.0.0.1:' + str(args.port), 'PUBLIC_URL': base,
              'ADMIN_PASSWORD': password, 'MAX_INFLIGHT': '256',
              'MAX_UNIQUE_VOTERS': '10000', 'DATA_DIR': str(directory / 'data')}
    if args.mode == 'postgres-kafka':
        database = os.environ.get('TEST_DATABASE_URL', '')
        check(bool(database), 'TEST_DATABASE_URL is required for an isolated Kafka metadata schema')
        values.update(DATABASE_URL=database, GIGAQUIZZ_SCHEMA='gqcheck_' + secrets.token_hex(8),
                      KAFKA_BROKERS=os.environ.get('KAFKA_BROKERS', '127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094'))
    config = directory / 'service.env'
    fd = os.open(config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as f:
        f.write(''.join(k+'='+v+'\n' for k, v in values.items()))
    env = os.environ.copy()
    # The generated private config must win over unrelated inherited settings.
    for key in values:
        env.pop(key, None)
    env.pop('DURABILITY_REQUIRED_STANDBYS', None)
    env.pop('DURABILITY_STANDBY_NAMES', None)
    child, logfile = None, (directory / 'application.log').open('ab')
    report = {'mode': args.mode, 'started_at': datetime.now(timezone.utc).isoformat(),
              'correct': False, 'checks': [], 'errors': [], 'acknowledged_attempts': 0,
              'scope': 'Real local HTTP app, isolated data/schema, real 60-second poll, owned-process SIGKILL and restart. No physical power loss or throughput claim.'}
    cookie = http.cookiejar.CookieJar()
    anonymous = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    admin = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(cookie))

    def request(path, body=None, authenticated=False):
        headers = {'Content-Type': 'application/json', 'Origin': base}
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base+path, data=data, headers=headers)
        try:
            with (admin if authenticated else anonymous).open(req, timeout=15) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read(65536))

    def phase(name):
        print(json.dumps({'mode': args.mode, 'stage': name}), flush=True)

    def start():
        nonlocal child
        child = subprocess.Popen([str(binary), '-env', str(config)], cwd=root, env=env,
                                 stdin=subprocess.DEVNULL, stdout=logfile, stderr=logfile)
        deadline = time.monotonic()+60
        while time.monotonic() < deadline:
            check(child.poll() is None, 'owned application exited during startup; inspect its private log')
            try:
                status, _ = request('/readyz')
                if status == 200:
                    return
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(.1)
        raise RuntimeError('owned application did not become ready')

    def stop(hard=False):
        if child is not None and child.poll() is None:
            child.kill() if hard else child.terminate()
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.kill(); child.wait(timeout=5)
                raise RuntimeError('graceful shutdown exceeded 20 seconds')

    def login():
        status, _ = request('/api/admin/login', {'password': password}, True)
        check(status == 200, 'administrator login failed')

    def wait_until(when):
        while (remaining := (when-datetime.now(timezone.utc)).total_seconds()) > 0:
            time.sleep(min(.2, remaining))

    try:
        phase('startup')
        start(); login()
        starts = datetime.now(timezone.utc)+timedelta(seconds=30 if args.mode == 'postgres-kafka' else 3)
        status, p = request('/api/admin/polls', {'question': 'Service recovery check', 'type': 'single',
                                               'options': ['First', 'Second'], 'starts_at': starts.isoformat()}, True)
        check(status == 201, 'poll creation failed')
        path = '/api/polls/'+p['id']; results = '/api/admin/polls/'+p['id']+'/results'
        check((instant(p['ends_at'])-instant(p['starts_at'])).total_seconds() == 60, 'window is not exactly 60 seconds')
        a, b, c = [secrets.token_hex(16) for _ in range(3)]
        check(request(results)[0] == 401, 'results accessible without administrator session')
        check(request(path+'/votes', {'token': a, 'choices': [1]})[0] == 425, 'early vote was admitted')
        phase('waiting_for_real_window')
        wait_until(instant(p['starts_at'])+timedelta(milliseconds=20))

        def vote(token, choice):
            status, receipt = request(path+'/votes', {'token': token, 'choices': [choice]})
            check(status == 202 and receipt.get('status') == 'recorded', 'vote did not return a durable-attempt receipt')
            admitted = instant(receipt['accepted_at'])
            check(instant(p['starts_at']) <= admitted < instant(p['ends_at']), 'receipt admitted outside original window')
            return 1

        report['acknowledged_attempts'] += vote(a, 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            report['acknowledged_attempts'] += sum(pool.map(lambda _: vote(a, 2), range(8)))
        report['acknowledged_attempts'] += vote(b, 2)
        check(request(results, authenticated=True)[1].get('pending') is True, 'unfinished aggregate was exposed as complete')
        phase('sigkill_and_recover_inside_window')
        stop(hard=True); start()
        check(request('/api/admin/session', authenticated=True)[0] == 401, 'old administrative session survived process restart')
        login()
        status, recovered = request(path)
        check(status == 200 and recovered['starts_at'] == p['starts_at'] and recovered['ends_at'] == p['ends_at'], 'restart changed the poll window')
        report['acknowledged_attempts'] += vote(a, 2)
        report['acknowledged_attempts'] += vote(c, 1)
        report['checks'].append('resume_inside_original_window_after_sigkill')
        phase('sigkill_then_restart_after_deadline')
        stop(hard=True)
        wait_until(instant(p['ends_at'])+timedelta(milliseconds=150))
        start(); login()
        for token in (a, secrets.token_hex(16)):
            check(request(path+'/votes', {'token': token, 'choices': [2]})[0] == 410, 'closed poll admitted a new or repeated attempt')
        until = time.monotonic()+60
        final = None
        while time.monotonic() < until:
            status, final = request(results, authenticated=True)
            if status == 200 and final.get('state') == 'final' and not final.get('pending'):
                break
            time.sleep(.2)
        check(final is not None and final.get('state') == 'final', 'final aggregate did not complete')
        check(final['total_votes'] == 3 and [x['votes'] for x in final['options']] == [2, 1], 'lost or double-counted confirmed choice')
        report['checks'].append('exact_first_choice_after_expired_unsealed_restart')
        report['final_unique_votes'] = final['total_votes']
        report['final_choice_counts'] = [x['votes'] for x in final['options']]
        phase('graceful_restart_of_final_result')
        stop(); start(); login()
        status, repeated = request(results, authenticated=True)
        check(status == 200 and repeated == final, 'persisted final result changed after restart')
        report['checks'].extend(['final_result_survives_graceful_restart', 'late_new_and_repeat_rejected', 'admin_sessions_revoked_on_restart'])
        report['correct'] = True
    except Exception as error:
        # Do not copy network/SQL exceptions, bodies, IDs or credentials into public output.
        report['errors'].append(str(error) if isinstance(error, RuntimeError) else type(error).__name__)
    finally:
        try:
            stop()
        except RuntimeError as error:
            report['errors'].append(str(error)); report['correct'] = False
        logfile.close()
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    (directory/'report.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2), flush=True)
    print('Private artifacts: '+str(directory.relative_to(root)), file=sys.stderr)
    return 0 if report['correct'] and not report['errors'] else 1


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['simple-files', 'postgres-kafka'], required=True)
    parser.add_argument('--port', type=int, required=True)
    try:
        sys.exit(run(parser.parse_args()))
    except Exception as error:
        print('check_service: '+(str(error) if isinstance(error, RuntimeError) else type(error).__name__), file=sys.stderr)
        sys.exit(1)
