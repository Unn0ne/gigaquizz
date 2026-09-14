# Проверка серверного профиля и переноса

[Описание ревью](../../../server-readiness-review.md), [установка](../../../server-deployment.md).

summary.json содержит итоги обоих минутных HTTP-прогонов и переноса. В branch JSON — полная агрегированная сверка ACK с журналом и итогом, параметры старта и повторного запуска. checks.json фиксирует suite и хеши приватных логов; linux-smoke.json — фактически выполненный Linux scope. provenance.json связывает проверенные бинарники с review-комплектами. Clean rebuild подтверждается отдельным clean-rebuild.json после коммита.

100M HTTP/min не проверены. Планы, ID опросов, seeds, raw ledgers, адреса и credentials остаются в приватных артефактах. Docker PG/Kafka lab не создан из-за недостаточного резерва диска; Linux amd64 собран без исполнения, unit прошёл офлайн systemd-analyze verify; запуск под systemd на целевом сервере ещё требует проверки.
