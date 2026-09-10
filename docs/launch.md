# Запуск ветки PostgreSQL + Kafka

Актуальная инструкция: [PostgreSQL + Kafka — настройка и восстановление](postgres-kafka.md).

Рабочий сайт этой ветки использует PostgreSQL для описаний и итогов, Kafka для попыток голосования. Нужны оба хранилища; отдельные benchmark-команды сохранены для сравнений.

`make dev` и `make run` читают `.env.postgres-kafka`; путь можно изменить через `GIGAQUIZZ_ENV_FILE`. По умолчанию кабинет открывается на http://127.0.0.1:8092/admin. При прямом запуске передайте `-env .env.postgres-kafka`. Существующие `.env` и `.env.simple-files` не изменяются.

[Сравнение двух рабочих веток](branches.md), [контракт API](api.md), [проверки](branch-verification.md).
