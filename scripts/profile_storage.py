#!/usr/bin/env python3
"""Run one bounded storagebench profile on the owned local replication lab.

Read-only diagnostics contain aggregate waits, counters and resource use, never
SQL text, request bodies, tokens or connection credentials. Commands are passed
as argv, not through a shell. This script never starts or changes database nodes.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parent.parent
LAB = ROOT / '.local/replication-lab'
SQL = """SELECT json_build_object(
 'at', clock_timestamp(),
 'activity', (SELECT coalesce(json_agg(x), '[]') FROM (
   SELECT state, wait_event_type, wait_event, count(*) AS sessions
   FROM pg_stat_activity WHERE application_name='gigaquizz'
   GROUP BY state, wait_event_type, wait_event) x),
 'wal', (SELECT row_to_json(w) FROM pg_stat_wal w),
 'checkpoints', (SELECT row_to_json(b) FROM pg_stat_bgwriter b),
 'database', (SELECT json_build_object('commits',xact_commit,'rollbacks',xact_rollback,
   'blocks_read',blks_read,'blocks_hit',blks_hit,'temp_bytes',temp_bytes,
   'deadlocks',deadlocks,'stats_reset',stats_reset)
   FROM pg_stat_database WHERE datname=current_database()),
 'replication', (SELECT coalesce(json_agg(json_build_object(
   'name',application_name,'state',state,'sync_state',sync_state,
   'flush_behind_bytes',pg_wal_lsn_diff(pg_current_wal_insert_lsn(),flush_lsn))), '[]')
   FROM pg_stat_replication));"""


def cpu_seconds(value):
    days, value = value.split('-', 1) if '-' in value else ('0', value)
    result = 0.0
    for part in value.split(':'):
        result = result * 60 + float(part)
    return result + int(days) * 86400


def resources(roots):
    result = subprocess.run(['ps', '-axo', 'pid=,ppid=,time=,rss='],
                            capture_output=True, text=True, check=True, timeout=3)
    processes = {}
    for line in result.stdout.splitlines():
        pid, parent, cpu, rss = line.split()
        processes[int(pid)] = (int(parent), cpu_seconds(cpu), int(rss))
    groups = {}
    for group, root in roots.items():
        members = {root}
        while True:
            children = {pid for pid, values in processes.items() if values[0] in members}
            expanded = members | children
            if expanded == members:
                break
            members = expanded
        values = [processes[pid] for pid in members if pid in processes]
        groups[group] = {'live_processes': len(values),
                         'live_process_cpu_seconds': sum(v[1] for v in values),
                         'sum_rss_kib': sum(v[2] for v in values)}
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', required=True)
    parser.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch('[a-z0-9_]{1,80}', args.label):
        parser.error('label must contain 1..80 lowercase letters, digits or underscores')
    arguments = args.arguments[1:] if args.arguments[:1] == ['--'] else args.arguments
    meta = json.loads((LAB / 'metadata.json').read_text())
    owner = json.loads((LAB / 'ownership.json').read_text())
    if owner['uid'] != os.getuid() or Path(owner['root']) != LAB or meta.get('pending_promotion'):
        parser.error('expected owned, stable local replication lab')
    directory = LAB / 'tuning'
    directory.mkdir(mode=0o700, exist_ok=True)
    report_path = directory / (args.label + '.json')
    telemetry_path = directory / (args.label + '_telemetry.json')
    if report_path.exists() or telemetry_path.exists():
        parser.error('profile already exists; choose a new label')
    env = os.environ.copy()
    env.update(STORAGEBENCH_DATABASE_URL=meta['primary_url'],
               STORAGEBENCH_REQUIRED_STANDBYS='1',
               STORAGEBENCH_STANDBY_NAMES=','.join(meta['durability_policy']['expected_standby_names']),
               PGAPPNAME='gigaquizz_sampler', PGOPTIONS='-c statement_timeout=2000')
    roots = {}
    for name in meta['nodes']:
        pidfile = LAB / name / 'postmaster.pid'
        if pidfile.exists():
            roots[name] = int(pidfile.read_text().splitlines()[0])
    command = [str(ROOT / 'bin/storagebench'), *arguments]
    samples = []
    started = time.monotonic()
    with report_path.open('x') as output, (directory / (args.label + '.stderr')).open('x') as errors:
        child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output, stderr=errors)
        roots['generator'] = child.pid
        try:
            while child.poll() is None:
                if time.monotonic() - started > 130:
                    raise TimeoutError('bounded profile exceeded 130 seconds')
                tick = time.monotonic()
                sample = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          'seconds_since_process_start': tick-started}
                try:
                    sample['resources'] = resources(roots)
                    query = subprocess.run([str(Path(meta['pg_bin']) / 'psql'), meta['primary_url'],
                                            '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', SQL],
                                           env=env, capture_output=True, text=True, timeout=3)
                    if query.returncode == 0:
                        sample['postgres'] = json.loads(query.stdout)
                    else:
                        sample['sampler_error'] = 'postgres_snapshot_unavailable'
                except (subprocess.SubprocessError, ValueError, OSError):
                    sample['sampler_error'] = 'snapshot_unavailable'
                samples.append(sample)
                time.sleep(max(0, 0.5-(time.monotonic()-tick)))
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
            telemetry_path.write_text(json.dumps({
                'label': args.label, 'command': ['bin/storagebench', *arguments],
                'primary': meta['current_primary'], 'epoch': meta['epoch'],
                'interval_seconds': 0.5, 'samples': samples,
                'limitations': [
                    'Snapshots include setup, workload, waiting for real close and finalization; align with workload timestamps.',
                    'Wait samples are state observations, not exact time attribution; sampling adds overhead.',
                    'CPU counters cover live processes only; process exit can decrease the sum.',
                    'Sum of PostgreSQL RSS double-counts shared pages; it is not unique RAM use.',
                    'WAL/database counters are primary-wide, cumulative and may lag; fsync timings may be disabled.']
            }, indent=2)+'\n')
    report = json.loads(report_path.read_text())
    work = report.get('workload', {})
    print(json.dumps({'label':args.label, 'exit_code':child.returncode,
                      'confirmed':work.get('logical_with_client_confirmation'),
                      'skipped':work.get('attempts_skipped'), 'outcomes':work.get('outcomes'),
                      'p99_ms':work.get('first_client_confirmation_from_schedule',{}).get('p99_ms'),
                      'audit':report.get('reconciliation',{}).get('per_key_correct'),
                      'errors':report.get('errors')}))
    raise SystemExit(child.returncode)


if __name__ == '__main__':
    main()
