# Перенос на один сервер приложения

Комплект предназначен для одного процесса Gigaquizz на Linux с systemd. Файловой ветке нужен локальный постоянный диск; ветке postgres-kafka дополнительно нужны подготовленные PostgreSQL и три Kafka-брокера. Приложению не нужны Go, Python, Node.js или внешние файлы сайта во время работы: страницы и QR встроены в бинарник. Установка ниже выполняется оператором; скрипт сборки не подключается по SSH и не управляет службами.

## Стартовый профиль

Кандидат для последующих измерений: **64 выделенных vCPU, 128 GiB RAM, локальный NVMe от 1 TB и 25GbE**, Linux amd64 или arm64 с systemd. Для журналов выбирайте накопитель с защитой от потери питания; объём 1 TB — стартовый запас под историю и диагностику, не измеренная потребность одного опроса. Это не гарантия 100M ответов за минуту. В `deploy/server.env.example` своей ветки задан следующий профиль:

| Параметр | simple-files | postgres-kafka |
|---|---:|---:|
| Партиции нового опроса | 32 | 32 |
| Batch на партицию | 4096 | 1024 |
| Очередь голосов на партицию | 8192 | 8192 |
| MAX_INFLIGHT | 65536 | 65536 |
| MAX_UNIQUE_VOTERS | 120000000 | 120000000 |
| MAX_PARTITION_UNIQUE_VOTERS | 8000000 | 8000000 |
| Подготовка до старта | При создании | 60 секунд |
| GOMEMLIMIT / GOGC | 96GiB / 100 | 96GiB / 100 |

`GOMAXPROCS` не задан: остаётся стандартное поведение Go для доступных CPU/cgroup. Параметры Go читаются приложением из того же env-файла; уже заданное окружение имеет приоритет. GOMEMLIMIT — мягкий предел памяти Go, не hard limit RSS или резерв 96 GiB. На машине с меньшим cgroup/memory budget его нужно уменьшить. Если рядом работают PostgreSQL/Kafka, их память и page cache входят в отдельный общий бюджет хоста; профиль не выделяет каждому процессу ещё 128 GiB.

Суммарная очередь профиля — 262144 голоса, помимо активных batch, HTTP и пересчёта. MAX_INFLIGHT ограничивает POST-обработчики, а не все TCP/GET или память процесса. При переполнении нужны видимые busy/skips/unknown в отчёте, а не бесконечная очередь. Лимит 8M на партицию пересчёта не обрезает результат: превышение оставляет ошибку/pending. Перед переносом старого однопартиционного опроса с большим числом уникальных ID поднимите этот лимит до достаточного значения.

Для цели 100M/60s = 1,667M голосов/с лимит 65536 означает максимум **39,32 ms средней занятости POST-слота** по L = λW; с 20% повторов — 32,77 ms. Это время внутри ограниченного участка обработчика, включая разбор тела, очередь и подтверждение записи; это не полный клиентский RTT. Нужен дополнительный запас. При 64 CPU бюджет составляет всего 38,4 µs CPU на уникальный голос при полной загрузке всех CPU, до расходов на GET и прочую работу. Если оставить 20% сетевого канала в запасе, 25GbE даёт около 1500 байт на голос в каждом направлении при этой частоте. Эти расчёты задают условия будущего теста; они не доказывают пропускную способность. Для страницы и статики нужен рассчитанный отдельно CDN/кэш; увеличение очереди само по себе не ускоряет обработку.

Ручной GOMAXPROCS допускает 1..1024; без него сохраняется автоматический выбор CPU. [Правила GOMAXPROCS](https://pkg.go.dev/runtime#GOMAXPROCS), [мягкий предел памяти Go](https://go.dev/doc/gc-guide#Memory_limit).

## Подготовить и проверить комплект

В проверенном checkout соберите release до нагрузки:

```sh
umask 077
mkdir -p .local
python3 scripts/build_release.py --output "$PWD/.local/server-release" --target linux/amd64
```

`server-release` должен быть новым. Для ARM выберите `linux/arm64`; без `--target` строятся обе Linux-архитектуры. Dirty tree по умолчанию запрещён, `--allow-dirty` годится только для явно отмеченной проверки. Перенесите только этот комплект согласованным способом и на сервере из его корня выполните `sha256sum -c SHA256SUMS`; в `release-report.json` требуется `complete=true`. Сохраните source.json и хеши для сравнения с будущими измерениями.

В release есть бинарники выбранной архитектуры, `deploy/gigaquizz.service`, `deploy/server.env.example`, эта инструкция `SERVER-DEPLOYMENT.md`, `RUNNING.md` и optional HTTP runner. Создание пользователя и изменение systemd требуют административных прав. Следующие команды предназначены для **новой выделенной установки**: если пользователь, каталоги, конфигурация или unit уже существуют, сначала разберитесь с ними и используйте раздел переноса, не перезаписывайте вслепую.

```sh
sudo groupadd --system gigaquizz
sudo useradd --system --gid gigaquizz --home-dir /var/lib/gigaquizz --no-create-home --shell /usr/sbin/nologin gigaquizz
sudo install -d -o root -g root -m 0755 /opt/gigaquizz
sudo install -d -o root -g gigaquizz -m 0750 /etc/gigaquizz
sudo install -d -o gigaquizz -g gigaquizz -m 0700 /var/lib/gigaquizz
sudo install -o root -g root -m 0755 linux-amd64/gigaquizz /opt/gigaquizz/gigaquizz
sudo install -o root -g root -m 0755 linux-amd64/httpbench /opt/gigaquizz/httpbench
sudo install -o root -g gigaquizz -m 0640 deploy/server.env.example /etc/gigaquizz/service.env
sudo install -o root -g root -m 0644 deploy/gigaquizz.service /etc/systemd/system/gigaquizz.service
```

Замените `linux-amd64` при другой архитектуре и проверьте путь `nologin` на своём дистрибутиве. Отредактируйте `/etc/gigaquizz/service.env` защищённым редактором: уникальный ADMIN_PASSWORD минимум 16 ASCII-символов, фактические PUBLIC_URL, адреса/учётные данные хранилищ. Файл `0640 root:gigaquizz` читается сервисным пользователем; каталог `0750` не раскрывает его посторонним. Не делайте env `0600 root:root`: приложение читает его само. Не добавляйте EnvironmentFile с другим набором значений в unit.

Для files используйте физический DATA_DIR `/var/lib/gigaquizz/data` без symlink во всех родителях. Родитель должен принадлежать сервисному пользователю: начальная публикация создаёт staging рядом с data. StateDirectory — `/var/lib/gigaquizz`; сам StateDirectory нельзя использовать как DATA_DIR. Unit разрешает запись во весь этот родитель и не блокирует fsync. Для другого пути нужно согласованно изменить WorkingDirectory/ReadWritePaths/права. StateDirectory сохраняется между остановками. [Правила systemd для каталогов и ограничения записи](https://raw.githubusercontent.com/systemd/systemd/main/man/systemd.exec.xml).

Для PG укажите ту же выделенную primary БД/схему и Kafka-брокеры. Owner-соединению нужен direct primary или session pooling; transaction/statement pooling несовместим. Роль PostgreSQL должна создавать/использовать схему, Kafka identity — создавать/описывать/читать/писать topics и использовать transactional producers. Настройте transaction coordinator; poll topics требуют RF3/minISR2. При удалённых брокерах нужны доступные advertised addresses и KAFKA_ALLOW_REMOTE_BROKERS; TLS/SASL и сертификаты настраиваются отдельно. Пути сертификатов — абсолютные, читаемые сервисом вне `/home`, например под `/etc/gigaquizz`. Эти службы комплект не устанавливает. Три брокера на одном хосте не дают защиты от потери хоста.

## Проверить конфигурацию и запустить

```sh
sudo -u gigaquizz /opt/gigaquizz/gigaquizz -env /etc/gigaquizz/service.env -check-config
sudo systemd-analyze verify /etc/systemd/system/gigaquizz.service
sudo systemctl daemon-reload
sudo systemctl enable --now gigaquizz.service
sudo systemctl status gigaquizz.service
```

`-check-config` проверяет конфигурацию без открытия хранилища, сетевых подключений и создания данных; это **не readiness**, не проверка диска/прав/SLA или Kafka/PG credentials на сервере. Unit повторяет dry check через ExecStartPre. После старта проверьте `/healthz` и `/readyz`, затем проведите отдельный минутный smoke и точную сверку. `Type=simple` не превращает успешный systemctl start в готовность приёма голосов. Проверка исторических журналов выполняется в фоне: после restart дождитесь `state=final` и отсутствия `pending` у каждого нужного результата, прежде чем сравнивать его с сохранённой копией. Readiness сама по себе этого не подтверждает.

Для HTTPS нужен ваш reverse proxy: unit по умолчанию слушает loopback, PUBLIC_URL задаёт внешний origin. Сохраняйте Origin/Referer, Date/Age/ETag/Vary/Cache-Control; не кэшируйте `/api/admin/*`, `/api/time` и POST и не повторяйте voting POST автоматически. Непосредственный HTTP на частном интерфейсе допустим лишь в выделенной доверенной сети и требует изменения HTTP_ADDR/PUBLIC_URL. QR с loopback адресом работает только на этом устройстве.

Unit задаёт LimitNOFILE=262144 и TasksMax=8192 (задачи ОС, не число goroutine). Нет CPUQuota/MemoryMax, изменяющих кандидат незаметно. HTTP shutdown имеет 20 секунд; TimeoutStopSec=90s оставляет запас для storage close. После общего timeout systemd может послать SIGKILL: проверьте фактический выход и восстановление, не называйте такой stop штатным. Restart=no — автоматического захвата PG ownership, watchdog или второй реплики здесь нет. Потеря PG owner-сессии может оставить процесс живым с readiness=false; оператор останавливает прежний процесс и только затем запускает новый. [Остановка и Restart в systemd](https://raw.githubusercontent.com/systemd/systemd/main/man/systemd.service.xml).

## Перенести данные или обновить бинарник

1. Сохраните финалы завершённых опросов и при необходимости выполните независимый audit. На исходном сервере `sudo systemctl stop gigaquizz.service`; дождитесь выхода PID. Одновременных владельцев не допускайте.
2. Files: перенесите **весь** DATA_DIR после остановки, включая definitions, все partition WAL, контрольные файлы, промежуточные результаты и финалы. Сохраните оригинал, проверьте хеши/размеры и выдайте новой сервисной учётной записи правильные права. Не выбирайте только `votes.wal` и не соединяйте каталоги разных запусков. Новый DATA_DIR размещается на локальном носителе, с writable physical parent.
3. PG: новый процесс подключается к **той же** БД/схеме и тем же Kafka topics. Перенос/backup самой БД и брокеров выполняется их штатными средствами отдельно; замена пустой схемой или новыми topics — это не recovery. Убедитесь, что старый owner остановлен.
4. Установите проверенный бинарник своей ветки, сохраните конфигурацию и данные, выполните `-check-config`, затем запустите только один экземпляр. Снова проверьте readiness и финальные результаты. Пароли и admin-сессии не являются частью клиентской нагрузки; после restart потребуется повторный вход.

Изменение FILE_PARTITIONS/KAFKA_PARTITIONS применяется к **новым** опросам. Топология существующих журналов сохранена с опросом, время начала/закрытия при переносе не меняется. Перезапуск и новые лимиты не превращают незавершённый опрос в новую минуту. Файловые партиции — локальный параллелизм, а не отказоустойчивые реплики. Производительность этого профиля устанавливается только следующими измерениями отдельными генераторами.
