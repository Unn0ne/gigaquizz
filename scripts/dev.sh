#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
project_dir="$(pwd)"
mkdir -p .local
chmod 700 .local

pg_bin=""
if command -v initdb >/dev/null 2>&1; then
  pg_bin="$(dirname "$(command -v initdb)")"
elif [ -x /Applications/Postgres.app/Contents/Versions/latest/bin/initdb ]; then
  pg_bin=/Applications/Postgres.app/Contents/Versions/latest/bin
fi

if [ -n "$pg_bin" ]; then
  if [ ! -f .local/postgres/PG_VERSION ]; then
    "$pg_bin/initdb" -D .local/postgres -U gigaquizz --auth-local=trust --auth-host=scram-sha-256 --no-instructions >.local/initdb.log
  fi
  if ! "$pg_bin/pg_ctl" -D .local/postgres status >/dev/null 2>&1; then
    "$pg_bin/pg_ctl" -D .local/postgres -l .local/postgres.log -o "-k '$project_dir/.local' -p 55432 -c listen_addresses='' -c max_connections=200 -c log_statement=none -c log_min_error_statement=panic -c log_parameter_max_length_on_error=0" start
  fi
  if [ "$("$pg_bin/psql" -h "$project_dir/.local" -p 55432 -U gigaquizz -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname='gigaquizz'")" != "1" ]; then
    "$pg_bin/createdb" -h "$project_dir/.local" -p 55432 -U gigaquizz gigaquizz
  fi
  export GIGAQUIZZ_DEV_SOCKET="$project_dir/.local"
else
  if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    echo 'Start Docker, install PostgreSQL 16+, or configure DATABASE_URL and use make run.' >&2
    exit 1
  fi
  export GIGAQUIZZ_DEV_SOCKET=""
fi

python3 - <<'PY'
import os, secrets, urllib.parse
from pathlib import Path
p = Path('.env')
if not p.exists():
    database_password = secrets.token_urlsafe(32)
    socket = os.environ['GIGAQUIZZ_DEV_SOCKET']
    if socket:
        url = 'postgres://gigaquizz@/gigaquizz?' + urllib.parse.urlencode({'host': socket, 'port': 55432, 'sslmode': 'disable'})
    else:
        url = f'postgres://gigaquizz:{database_password}@127.0.0.1:55432/gigaquizz?sslmode=disable'
    data = f'HTTP_ADDR=127.0.0.1:8080\nPUBLIC_URL=http://127.0.0.1:8080\nDATABASE_URL={url}\nADMIN_PASSWORD={secrets.token_urlsafe(32)}\nPOSTGRES_PASSWORD={database_password}\nVOTE_DB_CONNECTIONS=64\nMAX_INFLIGHT=256\n'
    with open(os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
        f.write(data)
    print('Created .env. The administrator password is in ADMIN_PASSWORD.')
PY

if [ -z "$pg_bin" ]; then docker compose up -d --wait db; fi
go build -o bin/gigaquizz ./cmd/gigaquizz
exec ./bin/gigaquizz
