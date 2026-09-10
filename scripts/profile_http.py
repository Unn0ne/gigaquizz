#!/usr/bin/env python3
"""Bounded local HTTP vote test, with a disposable app/schema on the HA lab."""
import argparse
import datetime
import http.cookiejar
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import urllib.parse
import urllib.request

from profile_storage import LAB, ROOT, SQL, resources


COUNTER_FIELDS = (
    'logical_planned', 'logical_scheduled', 'logical_not_scheduled', 'logical_enqueued',
    'logical_dispatched', 'logical_skipped', 'http_attempts',
    'http_attempts_started_during_window', 'http_attempts_started_after_window',
    'http_response_headers_received', 'http_responses_fully_read', 'accepted_201_responses',
    'duplicate_200_responses', 'transport_errors_unknown_outcome', 'response_body_errors',
    'local_request_errors', 'unknown_503_responses', 'logical_with_acceptance_confirmation',
    'logical_without_confirmation_unknown_outcome', 'logical_without_confirmation_only_other_responses',
)
LATENCY_FIELDS = (
    'initial_dispatch_lag', 'http_attempt_elapsed',
    'http_attempt_end_to_end_from_logical_schedule', 'logical_end_to_end_including_optional_retry',
)


def aggregate_load(reports):
    """Sum populations, retaining conservative bounds instead of averaging p99."""
    if len(reports) == 1:
        return reports[0]
    result = {key: sum(report[key] for report in reports) for key in COUNTER_FIELDS}
    for key in ('http_statuses', 'skipped_by_reason'):
        totals = {}
        for report in reports:
            for outcome, count in report[key].items():
                totals[outcome] = totals.get(outcome, 0) + count
        result[key] = totals
    result['configured_logical_ops_per_second'] = sum(report['configured_logical_ops_per_second'] for report in reports)
    windows = {report['configured_window_seconds'] for report in reports}
    if len(windows) != 1:
        raise ValueError('generator arrival windows differ')
    result['configured_window_seconds'] = windows.pop()
    result['http_attempts_during_window_per_configured_second'] = (
        result['http_attempts_started_during_window'] / result['configured_window_seconds'])
    result['interrupted_or_drain_deadline_reached'] = any(report['interrupted_or_drain_deadline_reached'] for report in reports)
    result['generator_count'] = len(reports)
    result['latency_aggregation_method'] = (
        'Counts are summed and means weighted by observation count. Each p50/p95/p99 upper bound '
        'is the maximum individual generator bound, a conservative bound for the pooled observations, '
        'not their measured percentile. Individual reports retain all latency distributions. '
        'Generator windows start independently; aggregate send rate uses the common configured duration.')
    for key in LATENCY_FIELDS:
        populations = [report[key] for report in reports if report[key]['count']]
        count = sum(population['count'] for population in populations)
        latency = {'count': count, 'mean_ms': (
            sum(population['mean_ms'] * population['count'] for population in populations) / count if count else 0),
            'max_ms': max((population['max_ms'] for population in populations), default=0),
            'percentile_method': 'maximum_individual_upper_bound_conservative_for_pooled_observations'}
        for percentile in ('p50_upper_bound_ms', 'p95_upper_bound_ms', 'p99_upper_bound_ms'):
            latency[percentile] = max((population[percentile] for population in populations), default=0)
        result[key] = latency
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', required=True)
    parser.add_argument('--rate', type=int, default=6000)
    parser.add_argument('--duration', type=int, default=55)
    parser.add_argument('--workers', type=int, default=128)
    parser.add_argument('--queue', type=int, default=2048)
    parser.add_argument('--generators', type=int, default=1)
    parser.add_argument('--max-lag-ms', type=int, default=500)
    parser.add_argument('--db-connections', type=int, default=64)
    parser.add_argument('--max-inflight', type=int, default=256)
    args = parser.parse_args()
    if not re.fullmatch('[a-z0-9_]{1,80}', args.label):
        parser.error('invalid label')
    if not (1 <= args.rate <= 10000 and 1 <= args.duration <= 55 and args.rate*args.duration <= 500000
            and 1 <= args.workers <= 256 and 1 <= args.queue <= 8192 and 1 <= args.max_lag_ms <= 1000
            and 1 <= args.db_connections <= 128 and 1 <= args.max_inflight <= 2048):
        parser.error('profile exceeds local bounds: rate<=10000, duration<=55, keys<=500000, workers<=256, queue<=8192, lag<=1000ms')
    if not 1 <= args.generators <= 4:
        parser.error('generators must be 1..4')
    if any(value % args.generators for value in (args.rate, args.workers, args.queue)):
        parser.error('total rate, workers and queue must each be divisible by generators')
    meta = json.loads((LAB / 'metadata.json').read_text())
    owner = json.loads((LAB / 'ownership.json').read_text())
    if owner['uid'] != os.getuid() or Path(owner['root']) != LAB or meta.get('pending_promotion'):
        parser.error('expected owned, stable local lab')
    directory = LAB / 'tuning'
    directory.mkdir(mode=0o700, exist_ok=True)
    prefix = directory / args.label
    if prefix.with_suffix('.json').exists():
        parser.error('profile already exists; choose a new label')
    schema = 'gqhttp_' + secrets.token_hex(8)
    psql = str(Path(meta['pg_bin']) / 'psql')
    def sql(query):
        result = subprocess.run([psql, meta['primary_url'], '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', query],
                                capture_output=True, text=True, timeout=10)
        if result.returncode:
            raise RuntimeError('local SQL failed; query details suppressed')
        return result.stdout.strip()
    sql('CREATE SCHEMA ' + schema)
    dsn = urllib.parse.urlsplit(meta['primary_url'])
    query = urllib.parse.parse_qs(dsn.query)
    query['search_path'] = [schema + ',pg_catalog']
    dsn = urllib.parse.urlunsplit(dsn._replace(query=urllib.parse.urlencode(query, doseq=True)))
    base = 'http://127.0.0.1:18088'
    password = secrets.token_urlsafe(32)
    env = os.environ.copy()
    env.update(DATABASE_URL=dsn, ADMIN_PASSWORD=password, PUBLIC_URL=base, HTTP_ADDR='127.0.0.1:18088',
               VOTE_DB_CONNECTIONS=str(args.db_connections), MAX_INFLIGHT=str(args.max_inflight), DURABILITY_REQUIRED_STANDBYS='1',
               DURABILITY_STANDBY_NAMES=','.join(meta['durability_policy']['expected_standby_names']))
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}),
              urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    def request(path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base+path, data, {'Content-Type':'application/json'})
        with client.open(req, timeout=15) as response:
            return json.load(response)
    app_log = prefix.with_suffix('.app.log').open('x')
    app = subprocess.Popen([str(ROOT/'bin/gigaquizz'), '-env', '/dev/null'],
                           cwd=ROOT, env=env, stdout=app_log, stderr=app_log)
    loads = []
    outputs = []
    samples = []
    result = {'profile':vars(args), 'schema':schema, 'replication_required':1,
              'vote_db_connections':args.db_connections, 'server_max_inflight':args.max_inflight, 'server_pool_warmed':True,
              'generator_reports':[],
              'errors':[], 'limitations':[
                  'Single-host HTTP POST path; page delivery, TLS, CDN and geographic distribution excluded.',
                  'HTTP generator checks response status and aggregate reconciliation, not a per-key ledger.',
                  'Latency quantiles in HTTP generator are bucket upper bounds; queues and skipped slots remain visible.',
                  'Generators start independently. Launch timestamps bracket Popen, not the exact internal monotonic schedule start.',
                  'Sampler includes setup/drain/finalization and adds overhead. No power or zone failure is simulated.']}
    try:
        until = time.monotonic()+35
        while True:
            if app.poll() is not None:
                raise RuntimeError('test app exited before readiness')
            try:
                request('/readyz')
                break
            except (OSError, ValueError):
                if time.monotonic() > until:
                    raise RuntimeError('test app readiness deadline')
                time.sleep(.2)
        request('/api/admin/login', {'password':password})
        poll = request('/api/admin/polls', {'question':'Local HTTP load profile','type':'single','options':['A','B']})
        result['server_metrics_before'] = request('/api/admin/metrics')
        result['server_metrics_before_observed_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        result['poll_id'] = poll['id']
        result['poll_starts_at'] = poll['starts_at']
        result['poll_ends_at'] = poll['ends_at']
        command = [str(ROOT/'bin/loadtest'), '-url', base, '-poll', poll['id'], '-rate', str(args.rate//args.generators),
                   '-duration', str(args.duration)+'s', '-workers', str(args.workers//args.generators), '-queue', str(args.queue//args.generators),
                   '-max-lag', str(args.max_lag_ms)+'ms', '-allow-high-load']
        roots = {'application':app.pid}
        for node in meta['nodes']:
            roots[node] = int((LAB/node/'postmaster.pid').read_text().splitlines()[0])
        result['load_started_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        load_start = time.monotonic()
        for index in range(args.generators):
            name = 'generator' if args.generators == 1 else 'generator_'+str(index+1)
            path = prefix.with_suffix('.load.json' if args.generators == 1 else '.'+name+'.load.json')
            output = path.open('x')
            outputs.append(output)
            entry = {'name':name, 'raw_report_file':str(path.relative_to(ROOT)),
                     'logical_rate':args.rate//args.generators, 'workers':args.workers//args.generators,
                     'queue':args.queue//args.generators, 'duration_seconds':args.duration,
                     'launch_requested_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                     'launch_requested_offset_ms':(time.monotonic()-load_start)*1000}
            result['generator_reports'].append(entry)
            process = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=subprocess.DEVNULL)
            loads.append(process)
            entry['process_started_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            entry['process_started_offset_ms'] = (time.monotonic()-load_start)*1000
            roots[name] = process.pid
        result['generator_launch_spread_ms'] = (
            result['generator_reports'][-1]['launch_requested_offset_ms'] -
            result['generator_reports'][0]['launch_requested_offset_ms'])
        until = time.monotonic()+args.duration+20
        while True:
            running = False
            for process, entry in zip(loads, result['generator_reports']):
                if process.poll() is None:
                    running = True
                elif 'exit_observed_at' not in entry:
                    entry['exit_observed_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    entry['exit_code'] = process.returncode
            if not running:
                break
            if time.monotonic() > until:
                raise RuntimeError('HTTP workload deadline')
            tick = time.monotonic()
            sample = {'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
            try:
                sample['resources'] = resources(roots)
                sample['postgres'] = json.loads(sql(SQL))
            except (OSError, ValueError, subprocess.SubprocessError, RuntimeError):
                sample['sampler_error'] = 'snapshot_unavailable'
            samples.append(sample)
            time.sleep(max(0,.5-(time.monotonic()-tick)))
        result['load_finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        result['load_observed_wall_seconds'] = time.monotonic()-load_start
        result['server_metrics_after_workload'] = request('/api/admin/metrics')
        result['server_metrics_after_workload_observed_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for output in outputs:
            output.close()
        for entry in result['generator_reports']:
            entry['report'] = json.loads((ROOT/entry['raw_report_file']).read_text())
        result['load'] = aggregate_load([entry['report'] for entry in result['generator_reports']])
        if args.generators > 1:
            with prefix.with_suffix('.load.json').open('x') as aggregate_file:
                json.dump(result['load'], aggregate_file, indent=2)
                aggregate_file.write('\n')
        if any(process.returncode for process in loads):
            raise RuntimeError('HTTP generator failed')
        until = time.monotonic()+75
        while True:
            final = request('/api/admin/polls/'+poll['id']+'/results')
            if final['state'] == 'final':
                break
            if time.monotonic() > until:
                raise RuntimeError('actual poll close/finalization deadline')
            time.sleep(1)
        result['final_results'] = final
        result['server_metrics'] = request('/api/admin/metrics')
        counts = json.loads(sql("SELECT json_build_object('rows',count(*),'distinct_keys',count(DISTINCT token),"
                                "'wrong_choices',count(*) FILTER (WHERE choices<>ARRAY[1])) FROM "+schema+'.votes'))
        result['database_counts'] = counts
        result['aggregate_reconciliation_ok'] = (counts['rows']==counts['distinct_keys']==final['total_votes']
             == result['load']['logical_with_acceptance_confirmation'] and counts['wrong_choices']==0)
        if not result['aggregate_reconciliation_ok']:
            result['errors'].append('aggregate_reconciliation_mismatch')
    except Exception as error:
        result['errors'].append(type(error).__name__+': '+str(error))
    finally:
        for process in loads+[app]:
            if process is not None and process.poll() is None:
                process.terminate()
        for process in loads+[app]:
            if process is not None and process.poll() is None:
                try:
                    process.wait(timeout=22)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for output in outputs:
            output.close()
        app_log.close()
        result['schema_retained'] = True
        prefix.with_suffix('.json').write_text(json.dumps(result,indent=2)+'\n')
        Path(str(prefix)+'_telemetry.json').write_text(json.dumps({'samples':samples},indent=2)+'\n')
    work = result.get('load', {})
    print(json.dumps({'label':args.label,'generators':args.generators,
          'total_confirmed':work.get('logical_with_acceptance_confirmation'),
          'confirmed':work.get('logical_with_acceptance_confirmation'),
          'skipped':work.get('logical_skipped'),'http_statuses':work.get('http_statuses'),
          'statuses':work.get('http_statuses'),
          'p99_ms':work.get('http_attempt_end_to_end_from_logical_schedule',{}).get('p99_upper_bound_ms'),
          'p99_method':'individual_bucket_upper_bound' if args.generators == 1 else 'maximum_individual_upper_bound_conservative_for_pooled_observations',
          'aggregate_audit':result.get('aggregate_reconciliation_ok'),'errors':result['errors']}))
    raise SystemExit(bool(result['errors']))


if __name__ == '__main__':
    main()
