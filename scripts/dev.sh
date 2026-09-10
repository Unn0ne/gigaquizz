#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .local
chmod 700 .local
export GIGAQUIZZ_ENV_FILE="${GIGAQUIZZ_ENV_FILE:-.env.simple-files}"
python3 - <<'PY'
import os, secrets
from pathlib import Path
p = Path(os.environ['GIGAQUIZZ_ENV_FILE'])
if not p.exists():
    data = 'HTTP_ADDR=127.0.0.1:8091\nPUBLIC_URL=http://127.0.0.1:8091\nDATA_DIR=.local/files\nADMIN_PASSWORD=' + secrets.token_urlsafe(32) + '\nMAX_INFLIGHT=4096\n'
    with open(os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
        f.write(data)
    print('Created branch configuration. The administrator password is in ADMIN_PASSWORD.')
PY
go build -o bin/gigaquizz ./cmd/gigaquizz
exec ./bin/gigaquizz -env "$GIGAQUIZZ_ENV_FILE"
