#!/usr/bin/env python3
"""Prepare the owned local lab for a measured event, without weakening WAL.

Enable diagnostic I/O timing and request a checkpoint/restartpoint before the
poll, on each node. Does not change fsync, synchronous_commit, quorum, buffers,
WAL limits, or automatic checkpoint intervals. Run with no active benchmark.
"""
import datetime
import json
import os
from pathlib import Path
import subprocess

from profile_storage import LAB


def main():
    meta = json.loads((LAB/'metadata.json').read_text())
    owner = json.loads((LAB/'ownership.json').read_text())
    if owner['uid'] != os.getuid() or Path(owner['root']) != LAB or meta.get('pending_promotion'):
        raise SystemExit('expected owned, stable local lab')
    psql = str(Path(meta['pg_bin'])/'psql')
    nodes = [meta['current_primary'], *[n for n in meta['nodes'] if n != meta['current_primary']]]
    snapshots = {}
    for name in nodes:
        node = meta['nodes'][name]
        def sql(query):
            env = os.environ.copy()
            env.update(PGOPTIONS='-c statement_timeout=25000', PGAPPNAME='gigaquizz_event_preparation')
            p = subprocess.run([psql,node['url'],'-XAt','-v','ON_ERROR_STOP=1','-c',query],
                               env=env,capture_output=True,text=True,timeout=30)
            if p.returncode:
                raise RuntimeError('event preparation SQL failed on '+name)
            return p.stdout.strip()
        sql('ALTER SYSTEM SET track_wal_io_timing=on')
        sql('ALTER SYSTEM SET track_io_timing=on')
        sql('SELECT pg_reload_conf()')
        sql('CHECKPOINT')
        snapshots[name] = json.loads(sql("""SELECT json_build_object(
            'in_recovery',pg_is_in_recovery(),'fsync',current_setting('fsync'),
            'synchronous_commit',current_setting('synchronous_commit'),
            'synchronous_standby_names',current_setting('synchronous_standby_names'),
            'track_wal_io_timing',current_setting('track_wal_io_timing'),
            'track_io_timing',current_setting('track_io_timing'),
            'checkpoints',(SELECT row_to_json(b) FROM pg_stat_bgwriter b))"""))
    result = {'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'primary':meta['current_primary'],'epoch':meta['epoch'],'nodes':snapshots,
              'action':'enable I/O timings; checkpoint primary, request restartpoints on standbys before workload'}
    print(json.dumps(result,indent=2))


if __name__ == '__main__':
    main()
