> Архив предыдущего этапа. Актуальный запуск и поведение этой ветки описаны в [README](../README.md) и [сравнении версий](branches.md).

Локальный стенд синхронной репликации PostgreSQL, 10.09.2026.

`scripts/replication_lab.py` создаёт primary и два физических standby на одном компьютере. Он нужен для проверки условной записи, конкурентных повторов, задержек репликации, отмены ожидания и восстановления квитанций. Общие CPU, память, файловая система и питание означают, что такой стенд **не проверяет независимые зоны, потерю региона или production throughput**.

Нужны Python 3 и native PostgreSQL 16+. Поиск бинарников: `GIGAQUIZZ_PG_BIN` (либо `PG_BIN`), сохранённый путь существующего стенда, PATH, установленный Postgres.app 16/latest. Основная версия должна совпадать с созданным PGDATA. Docker и внешняя сеть не используются.

Из корня проекта:

```sh
python3 scripts/replication_lab.py up
python3 scripts/replication_lab.py status
```

Управляемые данные находятся исключительно в `.local/replication-lab/`:

| Узел | Каталог | Unix socket port | Исходная идентичность standby / slot |
|---|---|---:|---|
| primary | `primary/` | 55440 | — |
| standby_a | `standby_a/` | 55441 | `gigaquizz_ha_a` |
| standby_b | `standby_b/` | 55442 | `gigaquizz_ha_b` |

Сокеты — в `socket/`; `listen_addresses=''`, TCP-listener отсутствует. Доступ разрешён локальному PostgreSQL-пользователю `gigaquizz` через trust внутри каталога режима 0700. Это тестовый superuser, а не производственная модель доступа. Каталоги имеют режим 0700, метаданные и конфигурация — 0600. `.env`, обычная `.local/postgres` и dev-порт 55432 не изменяются.

`ownership.json` и маркеры каждого PGDATA защищают от управления чужим кластером. Непустой каталог без маркера не принимается; автоматического reset или удаления данных нет. Повторный `up` восстанавливает управляемую конфигурацию и запускает существующие узлы, сохраняя таблицы. Команды управления сериализованы локальной файловой блокировкой. Ожидания subprocess ограничены: до 45 секунд для initdb/base backup, до 20 секунд для запуска/остановки/повышения роли, до 15 секунд для появления репликации. Ошибка завершается ненулевым кодом; предыдущие данные не очищаются.

`metadata.json` содержит `database_url` / `primary_url`, URL каждого узла, `current_primary`, `epoch`, retired-узлы и `durability_policy`. После проверенной записи сохраняются `current_primary_system_identifier` и `current_primary_timeline` для привязки приложения к этой истории. Паролей в нём нет. При promotion URL меняется; не закрепляйте 55440 как вечного primary. Пример получения актуального URL без чтения `.env`:

```sh
python3 -c 'import json; print(json.load(open(".local/replication-lab/metadata.json"))["primary_url"])'
```

JSON команды `status` отражает текущие роли, LSN, физические слоты и `pg_stat_replication`. Поле `ready` означает наличие доступного primary с заданной конфигурацией и как минимум одного подходящего потокового physical standby; у соответствующего узла также проверяются `fsync`, `full_page_writes` и принадлежность той же системе БД. Это снимок состояния, не гарантия будущего ACK. Проверка конкретной квитанции должна доказывать достижение её WAL позиции нужной репликой отдельно.

Исходная политика — `synchronous_commit=on` и `synchronous_standby_names='ANY 1 (gigaquizz_ha_a,gigaquizz_ha_b)'`. Оба standby используют постоянные physical replication slots. На всех узлах включены `fsync` и `full_page_writes`; `shared_buffers=128MB`, `max_connections=200`, `max_replication_slots=4`. Это управляемые локальные параметры, а не результат подбора производственной мощности. WAL для остановленного standby ограничен `max_slot_wal_keep_size=512MB`: после большого отставания слот может стать непригодным и потребуется новая base backup. Диск дополнительно расходуется на WAL, базы и сохранённые старые ветки; 128MB shared buffers не ограничивают всю память процесса этим числом.

Только во время первоначального bootstrap remote synchronous wait отключён: создаётся база `gigaquizz` и маленькая служебная таблица `replication_lab.probe`, затем `pg_basebackup -R -X stream -S ... -C` создаёт standby. После подключения обоих включается окончательная политика и выполняется синхронная контрольная запись. До этого `up` не объявляет стенд готовым. После первого успешного bootstrap повторные команды никогда не отключают ожидание реплики для удобства запуска.

SQL-тексты, параметры, access logs, адреса клиентов и тела голосований не логируются скриптом. PostgreSQL сохраняет только критические сообщения в отдельных `primary.log` / `standby_a.log` / `standby_b.log`; обычные statement/error-statement/parameter/connection логи выключены. Для диагностики используются состояние ролей, slots и LSN. В стенд следует отправлять синтетические ID и варианты.

Управление отказами:

```sh
python3 scripts/replication_lab.py stop-node standby_a
python3 scripts/replication_lab.py start-node standby_a
python3 scripts/replication_lab.py stop-node primary --mode immediate
python3 scripts/replication_lab.py start-node primary
python3 scripts/replication_lab.py stop
python3 scripts/replication_lab.py up
```

`fast` корректно завершает сервер, прерывая его активные транзакции; `immediate` имитирует аварийную остановку PostgreSQL и требует recovery при следующем старте. Это не отключение питания и не уничтожение диска. При остановке обоих standby новые записи должны ждать синхронного подтверждения или завершаться неопределённым исходом по тайм-ауту клиента. Локальный commit при отменённом SyncRep wait может сохраниться без требуемой удалённой копии: восстановленная квитанция тоже требует проверки сохранности, а не только чтения строки.

Управляемое повышение роли выполняется отдельно:

```sh
python3 scripts/replication_lab.py stop-node primary
python3 scripts/replication_lab.py promote standby_a
python3 scripts/replication_lab.py status
```

Команда откажет, если прежний primary работает, кандидат не является доступным standby, другой доступный standby имеет более продвинутую WAL-позицию или среди остальных узлов обнаружился ещё один primary. Все не retired standby должны быть доступны для сравнения; при неполной информации автоматического выбора нет. Это консервативная локальная проверка, не полный HA-протокол. При сомнениях надо остановиться и исследовать LSN/историю; команда не обещает нулевой потери неизвестных локальных commit.

Перед promotion прежняя ветка записывается в durable metadata как retired; intent содержит system identifier и уже полученную WAL-позицию кандидата. После успешного promotion оставшийся standby получает новое подключение, physical slot создаётся у нового primary, `epoch` увеличивается. `start-node` и `up` не запускают retired PGDATA. При прерывании управляющей команды `pending_promotion` продолжается той же командой `promote <кандидат>` либо `up`. Если кандидат тоже упал, команда запускает его с **сохранённой** конфигурацией и состоянием `standby.signal`: нельзя заново вычислять его роль из ещё не обновлённого `current_primary`. Проверяются восстановление записанной WAL-позиции, остановленный прежний primary и роли остальных узлов. Остановленные сохранившиеся standby также могут быть запущены; неожиданная конфигурация второго primary останавливает продолжение. Имена каталогов остаются прежними и больше не обозначают текущую роль.

Для восстановления третьей копии нужен **отдельный rebuild**, а не restart старой ветки:

```sh
python3 scripts/replication_lab.py rebuild-node primary
python3 scripts/replication_lab.py status
```

Rebuild разрешён только для остановленного узла, который сейчас не primary. Его прежний PGDATA переносится в `retired-data/<узел>-epoch<номер>-<суффикс>` без удаления; слот пересоздаётся при отсутствии активного потребителя; новая base backup берётся с текущего primary. Узел получает свободную разрешённую replication identity, подключается standby и проверяется контрольной записью. Сохранённые каталоги старых веток не входят в автоматический запуск. При аварии rebuild скрипт не удаляет частичную копию и архив: дальнейшее восстановление требует осмотра состояния.

Проверка жизненного цикла должна включать повторный `up`, остановку/возврат одного standby, crash/restart primary без promotion, остановку primary и promotion, отказ запустить старую ветку, rebuild и полный stop/up. Результаты голосования и долю успешных квитанций проверяет отдельный генератор: сама контрольная таблица стенда не проверяет логику Gigaquizz и не измеряет RPS.

Фактически проверено 10.09.2026 на PostgreSQL 16.3 из установленного Postgres.app: bootstrap, повторный `up`, отказ promotion при живом primary, stop/start standby, `immediate` crash/restart primary, promotion в `standby_a`, отказ запуска retired primary, rebuild, обратный переход к исходному узлу primary, rebuild `standby_a`, полный stop/up. Синтетическая строка пережила весь цикл. Режимы каталогов/metadata проверены; inode, размер и время изменения `.env` и PID-файла обычного dev-кластера остались прежними. Итог smoke-команд сохранён локально в `.local/replication-lab/lifecycle-smoke.json`. После проверки исходные роли восстановлены, но epoch/timeline увеличены: приложение всегда читает актуальные значения из metadata.

Отдельно проверено восстановление `pending_promotion` при прерывании управляющего процесса в двух точках: после сохранения intent, но до вызова `pg_ctl promote`; и после фактического повышения роли, но до обновления `current_primary`. В обоих случаях кандидат и второй standby затем аварийно остановлены через `stop --mode immediate`. Команда `up` восстановила прежнюю либо уже повышенную роль кандидата, завершила pending intent и вернула готовый стенд; её stdout содержал один корректный JSON. Синтетические строки сохранились, запуск retired PGDATA был отклонён, явный rebuild вернул две физические реплики. Результат — `.local/replication-lab/pending-promotion-smoke.json`; dev-кластер и `.env` не изменились. Проверка отказа при неожиданном втором primary выполнена с подменёнными снимками состояния для обычного promotion и продолжения pending с работающим либо остановленным другим узлом, без создания реального split brain.

Семантика команд проверена по документации PostgreSQL 16: [pg_basebackup](https://www.postgresql.org/docs/16/app-pgbasebackup.html), [pg_ctl](https://www.postgresql.org/docs/16/app-pg-ctl.html), [синхронная репликация](https://www.postgresql.org/docs/16/warm-standby.html#SYNCHRONOUS-REPLICATION). Поведение отмены SyncRep — в [исходном коде ветки PostgreSQL 16](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/replication/syncrep.c#L297-L310). Проверено 10.09.2026.
