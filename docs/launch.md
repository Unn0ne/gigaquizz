# Запуск ветки PostgreSQL + Kafka

Актуальная инструкция: [PostgreSQL + Kafka — настройка и восстановление](postgres-kafka.md).

Рабочий сайт этой ветки использует PostgreSQL для описаний и итогов, Kafka для попыток голосования. Нужны оба хранилища; отдельные benchmark-команды сохранены для сравнений.

`make dev` и `make run` читают `.env.postgres-kafka`; путь можно изменить через `GIGAQUIZZ_ENV_FILE`. По умолчанию кабинет открывается на http://127.0.0.1:8092/admin. При прямом запуске передайте `-env .env.postgres-kafka`. Существующие `.env` и `.env.simple-files` не изменяются.

[Сравнение двух рабочих веток](branches.md), [контракт API](api.md), [проверки](branch-verification.md).

Поддерживается до 256 Kafka-партиций. `MAX_PARTITION_UNIQUE_VOTERS` ограничивает таблицу при последовательном пересчёте одной партиции; по умолчанию равен общему пределу. Число партиций фиксируется при создании опроса. Очередь, producer и пакеты умножаются на число партиций.

Для Kafka с проверяемым TLS задайте `KAFKA_TLS=true`, при собственной CA — `KAFKA_TLS_CA_FILE`; клиентский сертификат задаётся парой `KAFKA_TLS_CERT_FILE` / `KAFKA_TLS_KEY_FILE`. Для SASL доступны `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` через `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`; SASL требует TLS. Секреты хранятся в частном окружении, не в журнале. Эти настройки применяются также к независимому HTTP-reader. Они проверены конфигурационными тестами; локальный Kafka lab использует loopback plaintext.

Описание `/api/polls/{id}/definition` и страницы голосования можно отдавать через CDN с сохранением `Date`, `Age`, `ETag`, `Vary` и `Cache-Control`. Административные маршруты, POST, старый GET опроса и `/api/time` не кэшируются. Границы готовности приложения с одним владельцем схемы — [в отчёте ревью](review-scale.md).
