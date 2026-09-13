# Внешний HTTP-стенд: Linux-приложение и отдельные генераторы

Этот runbook описывает ручной запуск одного application process выбранной ветки на отдельном Linux-сервере. Генераторы работают на других машинах; итоговая сверка выполняется отдельным процессом рядом с хранилищем. Здесь нет аренды, установки через SSH или автоматического управления чужими сервисами. Пока адреса и доступы к стенду не предоставлены, внешний прогон остаётся невыполненным.

Сначала выполните smoke на **60 000 уникальных ID за 60 секунд, два генератора**, затем увеличивайте нагрузку ступенями. План на **102 000 000 ID за 60 секунд** задаёт 1,7 млн новых ID/s, но создание такого плана не доказывает пропускную способность. Три Kafka-процесса на одном сервере не дают устойчивости к потере сервера; файловые партиции не являются репликами.

## 1. Собрать и перенести проверенную версию

Собирайте из чистого проверенного коммита нужной ветки с Go версии из `go.mod` или новее. Приложение и reader должны принадлежать одной ветке. Сборка выполняется до измерений. Команды ниже создают новый каталог и не включают конфиги, пароли или данные:

```sh
test -z "$(git -c core.fsmonitor=false -c core.untrackedCache=false status --porcelain)"
umask 077
mkdir -p -m 700 "$PWD/.local"
RELEASE="$PWD/.local/external-release-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -m 700 "$RELEASE"
git rev-parse HEAD > "$RELEASE/source-commit.txt"
git branch --show-current > "$RELEASE/source-branch.txt"
go version > "$RELEASE/go-version.txt"
for arch in amd64 arm64; do
  mkdir -m 700 "$RELEASE/linux-$arch"
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -o "$RELEASE/linux-$arch/gigaquizz" ./cmd/gigaquizz
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -o "$RELEASE/linux-$arch/httpbench" ./cmd/httpbench
done
cp scripts/run_http_generators.py "$RELEASE/run_http_generators.py"
python3 - "$RELEASE" <<'PY'
import hashlib, pathlib, sys
root = pathlib.Path(sys.argv[1])
paths = sorted(p for p in root.rglob('*') if p.is_file())
with (root / 'SHA256SUMS').open('x') as out:
    for p in paths:
        out.write(hashlib.sha256(p.read_bytes()).hexdigest() + '  ' + str(p.relative_to(root)) + '\n')
PY
```

Перенесите release удобным согласованным способом. На каждой машине проверьте `sha256sum -c SHA256SUMS` в каталоге release и выберите бинарники под её архитектуру. `CGO_ENABLED=0` устраняет зависимость этих бинарников от локальной libc; это не заменяет запуск smoke на целевом Linux. Приложению Go/Python во время работы не нужны; host runner требует Python 3 на Linux. Kafka/PostgreSQL при выборе соответствующей ветки готовятся отдельно по [launch.md](launch.md).

Храните приватные каталоги с правами `0700`, env/plan/cookie/JSON/ledger — `0600`; бинарники должны оставаться исполняемыми. Не переносите ноутбучные `.env` или `.local` целиком. Не публикуйте план: он содержит seed, namespace, ID опроса и endpoint, достаточные для восстановления тестовых voter ID.

## 2. Запустить приложение и проверить доступность

Создайте отдельный приватный env-файл по `.env.example`. Значения ниже — шаблон: замените `PRIVATE_IP` реальным адресом частного интерфейса. Файл не исполняется как shell; подстановки `$VAR` внутри него не работают.

| Настройка | simple-files | postgres-kafka |
|---|---|---|
| `HTTP_ADDR` | `PRIVATE_IP:8091` | `PRIVATE_IP:8092` |
| `PUBLIC_URL` | `http://PRIVATE_IP:8091` | `http://PRIVATE_IP:8092` |
| Постоянные данные | Абсолютный `DATA_DIR` на локальном диске | Отдельная `GIGAQUIZZ_SCHEMA`, подготовленные PostgreSQL и Kafka |
| Авторизация | Случайный ASCII `ADMIN_PASSWORD`, минимум 16 байт, без `CHANGE_ME` | То же |
| Голосование | `FILE_PARTITIONS`, `FILE_BATCH_VOTES`, `FILE_QUEUE_VOTES`, `FILE_GROUP_LINGER` | `KAFKA_PARTITIONS`, `KAFKA_BATCH_VOTES`, `KAFKA_QUEUE_VOTES`, `KAFKA_LINGER_MS` |
| Общие лимиты | `MAX_INFLIGHT`, `MAX_UNIQUE_VOTERS`, `MAX_PARTITION_UNIQUE_VOTERS` | То же, плюс инвентарь `MAX_STORED_POLLS` |

Для Kafka на этом же Linux-сервере broker/PG listeners можно оставить loopback; внешним генераторам нужен только HTTP. Если брокеры расположены отдельно, требуются доступные advertised addresses, `KAFKA_ALLOW_REMOTE_BROKERS=true` и отдельно настроенные TLS/SASL. Этот runbook не проверяет Kafka TLS/SASL или PostgreSQL failover.

Приватный HTTP baseline допустим только в выделенной доверенной тестовой сети с ограниченным доступом. Он **не проверяет TLS** и передаёт административную авторизацию без шифрования. Для внешнего HTTPS используйте существующий настроенный reverse proxy, loopback listener приложения и точный `PUBLIC_URL=https://...`. Приложение не поднимает HTTPS listener самостоятельно. Сохраните Origin/Referer, не кэшируйте POST, `/api/admin/*`, `/api/time`; отключите автоматическую повторную отправку POST на upstream. Для статических ответов и `/definition` сохраняйте Date/Age/ETag/Vary/Cache-Control. URL должен быть origin без префикса пути. Генератор использует direct HTTP/1.1 без proxy из окружения. HTTPS origin проверяет TLS именно измеренного пути, с проверкой сертификата; результат не переносится автоматически на HTTP/2/3 или другой CDN/proxy.

Запустите приложение foreground в выделенной `tmux`-сессии на сервере:

```sh
./release/linux-amd64/gigaquizz -env /absolute/private/service.env
```

Путь и архитектуру замените своими. Всегда указывайте `-env`: прямой бинарник по умолчанию читает `.env`, а `make dev` использует имя своей ветки. Окружение процесса имеет приоритет над файлом. В другом терминале задайте origin через интерактивный ввод, затем проверьте доступность:

```sh
printf 'HTTP(S) origin приложения: '
read -r HTTP_ORIGIN
curl --fail --connect-timeout 5 --max-time 10 "$HTTP_ORIGIN/healthz"
curl --fail --connect-timeout 5 --max-time 10 "$HTTP_ORIGIN/readyz"
```

`HTTP_ORIGIN` здесь и ниже — полный HTTP(S) origin выбранного сервиса. `/healthz` проверяет процесс, `/readyz` — последнее состояние хранилища. Остановить собственное приложение можно Ctrl+C в его foreground-терминале либо SIGTERM его проверенному PID. Дождитесь выхода и закрытия писателей перед новым запуском; не запускайте вторую копию поверх того же DATA_DIR/схемы.

У PG-ветки потеря выделенной owner-сессии необратимо закрывает работу писателей, но процесс может остаться жив. Одного `systemd Restart=on-failure` недостаточно: требуется наблюдение за readiness и осознанный штатный stop/start после проверки PG ownership. Перезапуск использует прежний журнал и время; новая минута не начинается. Отказ Kafka writer во время минуты не означает автоматический failover: после закрытия контроллер умеет восстановить финализацию того же журнала. Плановые restart/fault checks выполняйте отдельно от измерения скорости.

## 3. Создать один изолированный опрос и общий план

Не допускайте посторонних голосов в тестовый опрос: строгая сверка считает их незапланированными. Установите корректную синхронизацию часов на app/генераторах и сохраните её состояние в материалах прогона. Исходное время опроса не меняется при запуске генераторов.

В новой управляющей сессии создайте приватный каталог `RUN`. Для входа пароль вводится интерактивно и не попадает в аргументы команд:

```sh
umask 077
RUN="$PWD/external-smoke-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -m 700 "$RUN"
python3 - "$RUN/login.json" <<'PY'
import getpass, json, pathlib, sys
with pathlib.Path(sys.argv[1]).open('x') as f:
    json.dump({'password': getpass.getpass('Administrator password: ')}, f)
PY
curl --fail --connect-timeout 5 --max-time 20 \
  -H "Origin: $HTTP_ORIGIN" -H 'Content-Type: application/json' --data-binary "@$RUN/login.json" \
  --cookie-jar "$RUN/admin.cookies" "$HTTP_ORIGIN/api/admin/login"
python3 - "$RUN/create.json" <<'PY'
import datetime, json, pathlib, sys
start = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=8)
with pathlib.Path(sys.argv[1]).open('x') as f:
    json.dump({'question':'Isolated external smoke', 'type':'single',
               'options':['A','B'], 'starts_at':start.isoformat()}, f)
PY
curl --fail --connect-timeout 5 --max-time 20 \
  --cookie "$RUN/admin.cookies" -H "Origin: $HTTP_ORIGIN" -H 'Content-Type: application/json' \
  --data-binary "@$RUN/create.json" "$HTTP_ORIGIN/api/admin/polls" > "$RUN/poll.json"
POLL_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$RUN/poll.json")
POLL_START=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["starts_at"])' "$RUN/poll.json")
```

Пример даёт восемь минут для подготовки и передачи плана. При несинхронных часах управляющей машины не используйте её дату: возьмите серверное время из `/api/time` или создайте опрос через кабинет. Go preflight/запуск допускают ожидание до 15 минут; оставляйте несколько минут до старта и время на все проверки. Нельзя создать новую дату «плюс минута» на каждом генераторе. При неизвестном исходе Create сначала проверьте список опросов; не создавайте дубликат вслепую.

Подготовка smoke-плана читает определение опроса, но не отправляет голоса:

```sh
./httpbench -prepare-plan "$RUN/plan.json" \
  -url "$HTTP_ORIGIN" -poll "$POLL_ID" -start-at "$POLL_START" \
  -total-unique 60000 -generators 2 -repeat-every 0 \
  -workers 64 -queue 4096 -max-lag 100ms -request-timeout 12s \
  > "$RUN/plan-summary.json"
./httpbench -inspect-plan "$RUN/plan.json" > "$RUN/inspection.json"
```

Это POST-only baseline. Для полного холодного открытия страницы создайте **отдельный** план с `-journey -definition`: каждый исходный ID выполняет пять GET до POST, повторные попытки — только POST. Режим, повторы и все лимиты должны совпадать с заявленным измерением. Инспектор проверяет план offline и выводит безопасные суммы, времена и ресурсы диапазонов, без seed/ID/endpoint. Не исправляйте JSON плана вручную после раздачи.

## 4. Раздать план и запустить host runner

На внешние генераторы перенесите соответствующий `httpbench`, `run_http_generators.py` и **одинаковый** `plan.json`. Сверьте SHA256 плана и release. Каждому процессу назначается уникальный индекс; объединение индексов всех машин должно совпадать со всеми диапазонами плана. Повтор индекса на другом хосте runner самостоятельно обнаружить не может.

На одном внешнем Linux-хосте smoke запускает оба индекса `0,1`. Если используются два хоста, назначьте `0` первому, `1` второму. В примере ниже сначала перейдите в каталог, куда перенесены `plan.json`, исполняемый Linux `httpbench` нужной архитектуры и runner. Родитель выходных каталогов должен существовать; каталоги самого прогона должны быть новыми.

```sh
PLAN="$PWD/plan.json"
BENCH="$PWD/httpbench"
HOST_PREFLIGHT="$PWD/smoke-preflight-$(date -u +%Y%m%dT%H%M%SZ)"
HOST_RUN="$PWD/smoke-run-$(date -u +%Y%m%dT%H%M%SZ)"
python3 run_http_generators.py --plan "$PLAN" --indices 0,1 \
  --binary "$BENCH" --output "$HOST_PREFLIGHT" \
  --max-clock-error-ms 10 --preflight-only
python3 run_http_generators.py --plan "$PLAN" --indices 0,1 \
  --binary "$BENCH" --output "$HOST_RUN" \
  --max-clock-error-ms 10
```

Для preflight и настоящего запуска используйте разные новые каталоги. Runner вызывает Go-инспектор и preflight каждого выбранного диапазона; preflight не отправляет POST. Go проверяет точное определение опроса, FD, диск и три ответа `/api/time`. Предел ошибки часов 10 ms — условие допуска, а не обещание точности сети. При отказе выясните причину; большое RTT может сделать такой предел недостижимым. Не увеличивайте допуск только ради прохождения проверки и не скрывайте его значение в отчёте. Настоящий генератор повторно проверяет часы перед POST, после подготовки ledger; исходные timestamps плана остаются прежними.

Проверка часов подтверждает допуск только в момент измерения. После неё генератор может ждать исходный старт до 15 минут; непрерывной проверки во время ожидания и голосования нет. Поддерживайте синхронизацию часов приложения и генераторов и отсутствие скачков времени до конца события. Изменение часов после guard может сделать профиль недостоверным, даже если `clock.verified` осталось `true`.

Runner сохраняет приватные копии `plan.json` и `httpbench`, `inspection.json`, `preflight-NNN.json/.stderr`; настоящий запуск добавляет `generator-NNN/`, `load-NNN.json/.stderr`, `verify-NNN.json/.stderr` и `report.json`. Индекс NNN глобальный: при сборке его не перенумеровывают. `generation_complete` в отчёте означает завершение клиентской генерации, а не сохранность записей в хранилище или доказательство разных физических машин.

Запускайте runner foreground внутри заранее открытой `tmux`-сессии и дождитесь его завершения. Он управляет только собственными дочерними генераторами, не приложением/БД/брокерами. Обрыв SSH вне устойчивой сессии, SIGKILL или отключение хоста не гарантируют cleanup; такой случай требует проверки оставшихся процессов и сохраняется как прерванный прогон. Не запускайте повторно тот же индекс/план после начала опроса и не дописывайте в прежний output.

## 5. Бюджеты перед увеличением нагрузки

Запишите CPU/RAM/архитектуру, версии ОС/Go, сетевой интерфейс и скорость линка, файловую систему, свободный диск, soft/hard FD limits, настройки приложения и каждого генератора. `MAX_INFLIGHT` ограничивает обработчики POST, но не общее число TCP-соединений, GET или весь RSS. Очереди хранения задаются **на партицию**. `GOMEMLIMIT` — мягкий предел Go heap, не резерв памяти и не жёсткий предел RSS. Текущие значения из ноутбучного профиля нельзя считать sizing сервера.

У прямого генератора максимум 100k новых ID/s, 6 млн ID/минуту, 10 млн попыток и 4096 workers на процесс. FD preflight требует минимум `2*workers+64` на процесс; суммарно на хосте нужны также запас для runner и остальных процессов, RAM, дисковая скорость и TCP-порты. Общий диск учитывается сразу для всех выбранных индексов, а не отдельно для каждого. Приложение не устанавливает автоматически резерв свободного места для своих файлов или Kafka retention: мониторинг и предварительный бюджет остаются обязательными.

У host runner по умолчанию максимум четыре выбранных процесса; большее число требует явного `--max-processes` в пределах 1–32 и прохождения общих проверок. Ограничены также суммарные workers (32768) и очередь (2 млн). Резерв RAM оценивается как 2,5 GiB на процесс плюс 2 GiB на хост, не более 75% физической памяти; каждому дочернему Go-процессу задаётся мягкий heap limit 2 GiB, RSS наблюдается отдельно. Эти консервативные ограничения могут отклонить даже маленький smoke на машине с малой RAM. Не размещайте 17 процессов на одном хосте только потому, что план содержит 17 индексов.

Для отдельного большого опроса замените smoke-параметры на `-total-unique 102000000 -generators 17 -repeat-every 0`. Ровно 17 процессов дают каждому 6 млн ID и план 100k ID/s; большее число процессов уменьшает назначенную долю. Это число **процессов генерации**, не необходимое или достаточное число машин, и не измеренная capacity. Сначала определите устойчивую ступень одного хоста. Создавайте новый опрос и новые каталоги для каждой ступени.

Пусть `U` — новые ID, `A` — все попытки с повторами. Для 102 млн без повторов:

| Ресурс | Нижняя оценка |
|---|---|
| Клиентские ledger | `64*A` = 6 528 000 000 B, около 6,08 GiB, суммарно на генераторах |
| Сбор копии ledger у reader | Ещё столько же диска; исходники сохраняются |
| Плотное состояние независимого audit | `9*A+U` = 1 020 000 000 B, около 0,95 GiB RAM, плюс Go/буферы/reader |
| Compact payload хранилища | `28*A` = 2 856 000 000 B до заголовков frames и служебных данных |
| Kafka RF3 | Три копии payload, затем batch/control/index/прочие накладные расходы |

Это не пиковый RSS и не прогноз полного физического размера. Добавьте старые данные, временные файлы, журналы процессов, копии при переносе, map одной партиции финализатора и запас. Для каждого задействованного тома сохраняйте минимум 8 GiB свободного места сверх планируемого нового объёма. Повторы увеличивают `A`; полный journey увеличивает HTTP-трафик, но не меняет правило 64 B на запланированную попытку. Не делайте сбор лишней копии на почти заполненном storage host.

## 6. Собрать все диапазоны и независимо проверить результат

Дождитесь завершения всех генераторов и финального ответа административного API. Сохраните `results.json` именно от этого опроса; pending не является результатом. Повторный вход после рестарта приложения обязателен: сессии не переживают перезапуск. Не продлевайте окно, даже если часть диапазонов не успела отправить запросы.

```sh
curl --fail --connect-timeout 5 --max-time 20 --cookie "$RUN/admin.cookies" \
  "$HTTP_ORIGIN/api/admin/polls/$POLL_ID/results" > "$RUN/results.json"
```

Перед аудитом убедитесь, что `pending` отсутствует/false и `state` равен `final`. Сохраните также host-run reports, stderr, телеметрию и административные counters. Соберите **все** каталоги диапазонов без изменения имён или содержимого manifest/worker-файлов. Итоговая структура у reader должна быть:

```text
collected/
  plan.json
  results.json
  ledgers/
    generator-000/manifest.json
    generator-000/worker-*.bin
    generator-001/manifest.json
    generator-001/worker-*.bin
    ... каждый индекс плана ровно один раз ...
```

Переносите `$HOST_RUN/generator-NNN` с сохранением `0700/0600`; не объединяйте каталоги с перезаписью. Хешируйте исходные и полученные файлы. Задайте `COLLECTED` абсолютным путём к новому каталогу сбора, в котором размещены указанные `plan.json`, `results.json` и `ledgers/`. `-verify-ledger -plan ... -generator N -ledger-dir ...` выполняет дополнительную offline-проверку одного перенесённого диапазона; она не заменяет общую сверку журнала.

Остановите **своё** приложение штатно перед независимым чтением, сохранив настоящий финал. Для files reader запускается на storage host либо на полной непротиворечивой копии DATA_DIR после остановки. Передаётся каталог опроса `DATA_DIR/polls/POLL_ID`, не вложенный `journal`:

```sh
./httpbench -audit-plan "$COLLECTED/plan.json" \
  -ledgers "$COLLECTED/ledgers" -results "$COLLECTED/results.json" \
  -file-journal "$DATA_DIR/polls/$POLL_ID" > "$COLLECTED/audit.json"
```

Для Kafka скопируйте сохранённый JSON поля `journal` **того же** опроса из `<GIGAQUIZZ_SCHEMA>.polls` в приватный `journal-config.json`. Это чтение метаданных, не новая конфигурация и не создание topic. Например, `psql` с заранее настроенными приватными PG connection settings/`.pgpass`, без DSN/пароля в командной строке:

```sh
psql -XAt -v ON_ERROR_STOP=1 -v schema="$GIGAQUIZZ_SCHEMA" -v poll_id="$POLL_ID" \
  > "$COLLECTED/journal-config.json" <<'SQL'
SELECT journal FROM :"schema".polls WHERE id = :'poll_id'::uuid;
SQL
./httpbench -audit-plan "$COLLECTED/plan.json" \
  -ledgers "$COLLECTED/ledgers" -results "$COLLECTED/results.json" \
  -kafka-config "$COLLECTED/journal-config.json" > "$COLLECTED/audit.json"
```

Kafka reader должен иметь доступ к сохранённым broker addresses; проще запускать его на app/storage host с тем же runtime TLS/SASL окружением. Секретов TLS/SASL в journal JSON нет, и reader не загружает app env-файл сам. Брокеры/PG для аудита не останавливаются. Kafka-сверка использует read_committed и полный стабильный snapshot, включая проверку хвоста после CLOSED; голос после CLOSED — ошибка. Допустимые recovery BOOT проверяются отдельно.

Успехом проверки является только успешный exit и полный отчёт независимого audit всех диапазонов. Он сопоставляет каждый ACK с исходными token/choice/admitted_at и точным итогом. Unknown может оказаться записанным; skip/busy не является потерей ранее подтверждённой записи. Полный успешный audit **не означает**, что все 102 млн запланированных попыток были приняты. В отчёте раздельно указывайте план, отправленные POST, ACK, unknown, busy/closed, skips/journey_failed, уникальный итог и missing/unexpected records. Отсутствующий диапазон, частичный ledger или timeout не превращаются в PASS.

## Зафиксированные замечания предварительного ревью

- Старый `profile_full_http.py` остаётся локальным harness и не является SSH-оркестратором; внешний запуск использует общий план и runner каждого хоста.
- Прямой app binary читает `.env` по умолчанию: portable-команды обязаны указывать конкретный `-env` и постоянные пути.
- В app нет TLS listener, общего лимита всех TCP-соединений или автоматического disk reserve. Это части конфигурации и измерения стенда, а не скрытые гарантии приложения.
- PG owner loss требует явного управления перезапуском; готовность процесса и готовность голосования различаются.
- Shared-host ноутбучные результаты не доказывают сетевую, TLS, дисковую или отказную способность внешнего стенда. Описанная процедура пока не выполнена на внешних хостах.
