#!/usr/bin/env python3
"""Build a private, self-contained release; preserve incomplete outputs on error.

Requires Python >=3.8, Git and the Go toolchain declared by this repository.
No deployment, service startup, tests, or user configuration/data copying.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
TARGETS = ('linux/amd64', 'linux/arm64', 'darwin/arm64', 'darwin/amd64')


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def now():
    return datetime.now(timezone.utc).isoformat()


def sha_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def private_write(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as target:
        target.write(data if isinstance(data, bytes) else data.encode())
        target.flush()
        os.fsync(target.fileno())


def save_report(output, report):
    temporary = output / 'release-report.tmp'
    private_write(temporary, json.dumps(report, indent=2, allow_nan=False) + '\n')
    os.replace(temporary, output / 'release-report.json')


def git(root, *args):
    result = subprocess.run(['git', '-c', 'core.fsmonitor=false', '-c', 'core.untrackedCache=false', *args],
                            cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    check(result.returncode == 0, 'Git source inspection failed')
    return result.stdout


def source_snapshot(root, output):
    # Go package inputs (including go:embed) can include ignored untracked
    # files. Git's ordinary source inventory omits them, so an ignored .env
    # could otherwise enter a binary without affecting its source fingerprint.
    # Inspect names only and reject; never read or report private contents.
    ignored = git(root, 'ls-files', '-z', '--others', '--ignored', '--exclude-standard', '--', 'cmd', 'internal')
    check(not ignored, 'ignored files under cmd/internal make release inputs unsafe; partial release preserved')
    # Exclude only this newly owned release directory if it is inside the repo.
    # Other untracked files still make the source dirty; ignored private data
    # are neither hashed nor copied.
    scope = ['--', '.']
    try:
        relative = output.relative_to(root)
    except ValueError:
        pass
    else:
        scope.append(':(exclude,literal)' + relative.as_posix())
    status = git(root, 'status', '--porcelain=v1', '-z', '--untracked-files=all', *scope)
    paths = sorted(set(git(root, 'ls-files', '-z', '--cached', '--others', '--exclude-standard', *scope).split(b'\0')) - {b''})
    digest = hashlib.sha256()
    for raw in paths:
        name = os.fsdecode(raw)
        path = root / name
        check(not Path(name).is_absolute() and '..' not in Path(name).parts, 'invalid source inventory path')
        digest.update(len(raw).to_bytes(8, 'big') + raw)
        try:
            info = path.lstat()
        except FileNotFoundError:
            digest.update(b'deleted\0')
            continue
        if stat.S_ISLNK(info.st_mode):
            digest.update(b'symlink\0' + os.fsencode(os.readlink(path)))
        else:
            check(stat.S_ISREG(info.st_mode), 'unsupported non-file source entry')
            digest.update(b'file\0' + str(stat.S_IMODE(info.st_mode)).encode() + bytes.fromhex(sha_file(path)))
    return dict(commit=git(root, 'rev-parse', 'HEAD').decode().strip(),
                branch=git(root, 'branch', '--show-current').decode().strip(), dirty=bool(status),
                status_sha256=hashlib.sha256(status).hexdigest(), source_tree_sha256=digest.hexdigest(),
                source_files=len(paths))


def running_text(backend, targets):
    target = targets[0].replace('/', '-')
    storage = """Файловая ветка не требует PostgreSQL или Kafka. Укажите абсолютный DATA_DIR,
например /var/lib/gigaquizz/data. Родитель /var/lib/gigaquizz должен быть
доступен сервису для записи: инициализация создаёт рядом временный каталог.
DATA_DIR и все его родители должны быть без symlink: используйте физический
абсолютный путь (например /private/var вместо /var на macOS).
При systemd StateDirectory используйте его дочерний data, не сам StateDirectory.
Нужен локальный исправный диск. FILE_PARTITIONS — параллельные журналы,
не реплики; очереди и batch limits заданы на партицию. Только один процесс
может владеть DATA_DIR. Перед переносом остановите его и копируйте весь DATA_DIR.
""" if backend == 'simple-files' else """PG-ветке дополнительно нужны подготовленная PostgreSQL и три Kafka-брокера:
- DATABASE_URL указывает выделенную БД; роль должна создавать/использовать
  GIGAQUIZZ_SCHEMA. Owner-соединение — напрямую к primary или через session
  pooling. Transaction/statement pooling несовместим с постоянной owner-сессией.
- KAFKA_BROKERS содержит доступные advertised addresses. Poll topics используют
  RF3 и min.insync.replicas=2. Нужны рабочий transaction coordinator и разрешения
  Kafka на создание/описание/read/write topics и transactional producers.
- Для удалённых брокеров задайте KAFKA_ALLOW_REMOTE_BROKERS=true, при необходимости
  KAFKA_TLS, абсолютные KAFKA_TLS_CA_FILE/CERT_FILE/KEY_FILE/SERVER_NAME.
  KAFKA_SASL_MECHANISM/USERNAME/PASSWORD поддерживают PLAIN и SCRAM-SHA-256/512
  только с проверенным TLS. Секреты храните в приватном env, не в journal JSON.
Один процесс владеет схемой. Потеря PG owner-сессии требует штатного stop/start:
живой процесс ещё не означает readiness. Данные PostgreSQL/Kafka сохраняются.
Комплект не устанавливает и не запускает PostgreSQL/Kafka. Три брокера на одном
хосте не защищают от потери хоста; шаблон не обещает PostgreSQL failover.
"""
    audit_flag = '-file-journal /absolute/data/polls/POLL_ID' if backend == 'simple-files' else '-kafka-config /absolute/collected/journal-config.json'
    audit_note = ('Files reader получает весь закрытый каталог опроса, не вложенный journal.'
                  if backend == 'simple-files' else
                  'Kafka reader получает сохранённый JSON поля journal этого опроса из\n'
                  'GIGAQUIZZ_SCHEMA.polls. Прочитайте его через psql с приватными PG settings,\n'
                  'без пароля в argv. Передайте reader то же окружение KAFKA_TLS/SASL:\n'
                  'он не читает app env-файл. PostgreSQL/Kafka для аудита не останавливают.')
    return f'''# Автономный комплект Gigaquizz: {backend}

Проверьте complete=true в release-report.json и все файлы: из корня комплекта
`sha256sum -c SHA256SUMS` на Linux или `shasum -a 256 -c SHA256SUMS` на macOS.
Доступные цели: {', '.join(targets)}. Выберите ОС/архитектуру своей машины.
source.json фиксирует commit, хеш фактических исходников и явный dirty override.
Dirty-сборка предназначена для ревью/локальной проверки; commit сам по себе
её не воспроизводит. Сборка с CGO_ENABLED=0 и -trimpath — не тест целевой ОС
и не доказательство производительности. Build logs остаются приватными.

Для одного Linux-сервера есть SERVER-DEPLOYMENT.md, deploy/gigaquizz.service
и deploy/server.env.example: стартовый профиль 64 vCPU/128 GiB, не обещание RPS.
Unit запускает один app owner, без автоматического restart/захвата хранилища.

## Один сервер приложения

1. Создайте приватный каталог конфигурации, выполните `umask 077`, скопируйте
   env.example в НОВЫЙ service.env. Задайте уникальный случайный ASCII
   ADMIN_PASSWORD минимум 16 символов, без CHANGE_ME. Каталог: 0700, env: 0600.
2. HTTP_ADDR — интерфейс:порт сервера; PUBLIC_URL — доступный пользователям
   HTTP(S) origin без префикса пути. Loopback доступен только на самом сервере.
   Незашифрованный HTTP используйте лишь в выделенной доверенной сети.
   Для HTTPS нужен ваш reverse proxy; app слушает loopback, PUBLIC_URL — HTTPS.
3. Env-файл не исполняется как shell и не раскрывает $VAR. Окружение процесса
   имеет приоритет. Передавайте абсолютный -env; иначе читается .env из cwd.
4. Запустите foreground или своим service manager (замените цель и env-путь):

```sh
./{target}/gigaquizz -env /absolute/private/service.env -check-config
./{target}/gigaquizz -env /absolute/private/service.env
```

-check-config проверяет значения без сети/хранилища/создания данных, не readiness.

{storage}
Серверу не нужны Go, Python, Node.js или CDN ассетов: страницы/скрипты/QR встроены.
Проверьте PUBLIC_URL/healthz и /readyz, затем откройте /admin, создайте опрос
и поделитесь ссылкой/QR. Окно — ровно 60 секунд. Перезапуск его не продлевает.
Останавливайте свой процесс Ctrl+C/SIGTERM и ждите выхода до нового запуска.
Сессии администратора не переживают restart. Следите за диском/RAM/FD:
приложение автоматически не резервирует 8GiB. На proxy сохраняйте Origin/Referer,
Date/Age/ETag/Vary/Cache-Control; не кэшируйте /api/admin/*, /api/time и POST,
не повторяйте voting POST автоматически. Подтверждение — сохранённая попытка;
в итог входит первый ответ каждого ID. Pending ещё не является финалом.

## Отдельные HTTP-генераторы и точная сверка

На каждом хосте генераторов нужны Python >=3.8, подходящий httpbench и `ps`
с `-o pid=,time=,rss= -p PIDLIST` (Linux procps; стандартный macOS ps).
Для HTTPS нужны доверенные CA: системные либо SSL_CERT_FILE/SSL_CERT_DIR.
Синхронизируйте часы; через SSH работайте в устойчивой foreground tmux-сессии.
Runner не управляет приложением/SSH и не доказывает разные физические хосты.

Создайте через /admin отдельный будущий опрос с несколькими минутами запаса.
Из ответа API возьмите его ID и точное starts_at; задайте HTTP_ORIGIN/POLL_ID/
POLL_START в shell. Ниже полный минутный POST smoke; новый каталог обязателен:

```sh
umask 077
mkdir -m 700 /absolute/private/new-plan
./{target}/httpbench -prepare-plan /absolute/private/new-plan/plan.json \
  -url "$HTTP_ORIGIN" -poll "$POLL_ID" -start-at "$POLL_START" \
  -total-unique 60000 -generators 2 -repeat-every 0 \
  -workers 64 -queue 4096 -max-lag 100ms -request-timeout 12s \
  > /absolute/private/new-plan/summary.json
```

Передайте один и тот же приватный plan всем генераторам; он содержит seed и
endpoint, его нельзя публиковать. Назначьте непересекающиеся глобальные индексы.
Пример владеет обоими 0,1; после переноса скорректируйте абсолютный путь плана:

```sh
python3 run_http_generators.py --plan /absolute/private/new-plan/plan.json --indices 0,1 \
  --binary "$PWD/{target}/httpbench" --output /absolute/private/new-preflight \
  --max-clock-error-ms 10 --preflight-only
python3 run_http_generators.py --plan /absolute/private/new-plan/plan.json --indices 0,1 \
  --binary "$PWD/{target}/httpbench" --output /absolute/private/new-generation \
  --max-clock-error-ms 10
```

Output всегда новый, его родитель существует. Default max-processes=4 (1..32),
резерв диска 8GiB сверх ledger; RAM: 2.5GiB на процесс +2GiB на хост <=75%
физической памяти. Heap limit мягкий, RSS измеряется выборками. Проверка часов
разовая, последующие скачки не отслеживаются. Исходное окно никогда не меняется.
SIGTERM/INT/HUP останавливают своих детей; SIGKILL/потеря хоста не гарантируют
cleanup. Нет автоматических повторов или удаления результатов. Не перезапускайте
индекс после начала опроса. -journey -definition добавляет пять холодных GET
перед исходным POST; повторы остаются POST-only. JS не исполняется. Используются
HTTP/1.1, отключённый proxy-env и проверенный TLS для HTTPS URL.

Host generation_complete подтверждает лишь полноту клиентского ledger, включая
skips/unknown. Соберите все generator-NNN/manifest.json и worker-*.bin без
переименования/перезаписи, сохраняя 0700/0600; сравните хеши после переноса.
Сохраните финальный admin results.json того же опроса (state=final, pending=false)
и остановите своё приложение. Прочитайте хранилище отдельным reader той же ветки:

```sh
./{target}/httpbench -audit-plan /absolute/collected/plan.json \
  -ledgers /absolute/collected/ledgers -results /absolute/collected/results.json \
  {audit_flag} \
  > /absolute/collected/audit.json
```

{audit_note}
Только exit 0 и correct=true полного audit подтверждают сверку ACK с журналом
и итогом. План/POST/ACK/unknown/skips/closed/busy/уникальный итог считайте отдельно.
План на 102M и успешная сборка не означают, что сервер выдерживает такую нагрузку.
'''


def build(args, root=ROOT):
    root = root.resolve()
    targets = args.target or ['linux/amd64', 'linux/arm64']
    check(len(set(targets)) == len(targets) and all(t in TARGETS for t in targets), 'unsupported or duplicate target')
    output = Path(args.output).absolute()
    check(stat.S_ISDIR(output.parent.lstat().st_mode), 'output parent must be an existing real directory')
    output = output.parent.resolve() / output.name
    output.mkdir(mode=0o700, exist_ok=False)
    report = dict(complete=False, started_at=now(), targets=targets, allow_dirty=args.allow_dirty, errors=[])
    try:
        before = source_snapshot(root, output)
        report['source'] = before
        check(not before['dirty'] or args.allow_dirty, 'source tree is dirty; use --allow-dirty only for explicit review builds')
        example = (root / '.env.example').read_bytes()
        backend = 'postgres-kafka' if b'KAFKA_BROKERS=' in example else 'simple-files'
        report['backend'] = backend
        assets = {'env.example': example,
                  'run_http_generators.py': (root / 'scripts/run_http_generators.py').read_bytes(),
                  'deploy/gigaquizz.service': (root / 'deploy/gigaquizz.service').read_bytes(),
                  'deploy/server.env.example': (root / 'deploy/server.env.example').read_bytes(),
                  'SERVER-DEPLOYMENT.md': (root / 'docs/server-deployment.md').read_bytes()}
        build_environment = dict(CGO_ENABLED='0', GOFLAGS='', GOENV='off', GOWORK='off', GOEXPERIMENT='',
                                 GOAMD64='v1', GOARM64='v8.0')
        env = dict(os.environ, **build_environment)
        version = subprocess.run(['go', 'version'], cwd=root, env=env, capture_output=True, timeout=60)
        check(version.returncode == 0, 'Go toolchain version check failed')
        report['go_version'] = version.stdout.decode().strip()
        # Explicit source.json is the VCS authority. Avoid an additional Go
        # implicit git-status call with inherited fsmonitor configuration.
        report['build_flags'] = ['-trimpath', '-mod=readonly', '-buildvcs=false']
        report['build_environment'] = build_environment
        logs = output / 'build-logs'
        logs.mkdir(mode=0o700)
        expected = []
        for target in targets:
            os_name, arch = target.split('/')
            destination = output / target.replace('/', '-')
            destination.mkdir(mode=0o700)
            for name in ('gigaquizz', 'httpbench'):
                binary = destination / name
                command = ['go', 'build', *report['build_flags'], '-o', str(binary), './cmd/' + name]
                log = logs / (target.replace('/', '-') + '-' + name + '.log')
                fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, 'wb') as stream:
                    result = subprocess.run(command, cwd=root, env=dict(env, GOOS=os_name, GOARCH=arch),
                                            stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT, timeout=1200)
                check(result.returncode == 0, 'Go build failed; partial release preserved')
                info = binary.lstat()
                check(stat.S_ISREG(info.st_mode) and info.st_size > 0 and info.st_mode & 0o111, 'Go build omitted a usable binary')
                binary.chmod(0o700)
                expected.append(binary.relative_to(output).as_posix())
        (output / 'deploy').mkdir(mode=0o700)
        for name, contents in assets.items():
            private_write(output / name, contents)
        private_write(output / 'RUNNING.md', running_text(backend, targets))
        after = source_snapshot(root, output)
        check(before == after, 'source tree changed during build; partial release is not complete')
        metadata = dict(before, allow_dirty=args.allow_dirty, backend=backend, go_version=report['go_version'],
                        targets=targets, build_flags=report['build_flags'], build_environment=report['build_environment'])
        private_write(output / 'source.json', json.dumps(metadata, indent=2) + '\n')
        expected += [*assets, 'RUNNING.md', 'source.json']
        files = sorted(p for p in output.rglob('*') if p.is_file())
        inventory = {p.relative_to(output).as_posix(): sha_file(p) for p in files}
        check(all(name in inventory for name in expected), 'release inventory is incomplete')
        private_write(output / 'SHA256SUMS', ''.join(f'{digest}  {name}\n' for name, digest in inventory.items()))
        report['files'] = inventory
        report['checksums_sha256'] = sha_file(output / 'SHA256SUMS')
        report['complete'] = True
    except (Exception, KeyboardInterrupt) as error:
        report['errors'].append(dict(type=type(error).__name__, message=str(error) if isinstance(error, RuntimeError)
                                    else 'release operation failed; private partial output preserved'))
    report['finished_at'] = now()
    save_report(output, report)
    print(json.dumps(dict(complete=report['complete'], backend=report.get('backend'), targets=targets,
                          dirty=report.get('source', {}).get('dirty'), errors=report['errors'])))
    return 0 if report['complete'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, help='new private directory; parent must exist')
    parser.add_argument('--target', action='append', choices=TARGETS, help='repeatable; defaults to both Linux architectures')
    parser.add_argument('--allow-dirty', action='store_true', help='explicit review build, recorded as dirty provenance')
    args = parser.parse_args()
    os.umask(0o077)
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        return build(args)
    except (Exception, KeyboardInterrupt) as error:
        print(json.dumps(dict(complete=False, error_type=type(error).__name__, message='release setup failed; nothing was overwritten')))
        return 1


if __name__ == '__main__':
    sys.exit(main())
