Артефакты испытаний пакетного журнала, 10.09.2026.

Интерпретация и ограничения — в [отчёте](../../../log-results.md). JSON содержит агрегаты и пути к приватным синтетическим ledgers; сами ledgers сюда не копируются. Для каждого нагрузочного профиля telemetry сохраняет фактическую команду, SHA-256 бинарника, snapshots CPU/RSS и точные интервалы действий с брокерами.

| Профиль | Нагрузка/ошибка | CPU, RSS и отказы |
|---|---|---|
| direct_100000_60s_p8_l20 | [JSON](direct_100000_60s_p8_l20.json) | [telemetry](direct_100000_60s_p8_l20_telemetry.json) |
| http_100000_60s_p8_l20 | [JSON](http_100000_60s_p8_l20.json) | [telemetry](http_100000_60s_p8_l20_telemetry.json) |
| http_100000_60s_p8_l20_w8192 | [JSON](http_100000_60s_p8_l20_w8192.json) | [telemetry](http_100000_60s_p8_l20_w8192_telemetry.json) |
| http_1000_quorum_loss | [JSON](http_1000_quorum_loss.json) | [telemetry](http_1000_quorum_loss_telemetry.json) |
| http_20000_60s_p8_l20 | [JSON](http_20000_60s_p8_l20.json) | [telemetry](http_20000_60s_p8_l20_telemetry.json) |
| http_20000_60s_p8_l20_ready | [JSON](http_20000_60s_p8_l20_ready.json) | [telemetry](http_20000_60s_p8_l20_ready_telemetry.json) |
| http_20000_60s_p8_l20_repeat | [JSON](http_20000_60s_p8_l20_repeat.json) | [telemetry](http_20000_60s_p8_l20_repeat_telemetry.json) |
| http_50000_60s_p8_l20 | [JSON](http_50000_60s_p8_l20.json) | [telemetry](http_50000_60s_p8_l20_telemetry.json) |
| http_50000_60s_p8_l20_repeat | [JSON](http_50000_60s_p8_l20_repeat.json) | [telemetry](http_50000_60s_p8_l20_repeat_telemetry.json) |
| http_6000_60s_p8_l20 | [JSON](http_6000_60s_p8_l20.json) | [telemetry](http_6000_60s_p8_l20_telemetry.json) |
| http_6000_duplicates_broker1_crash | [JSON](http_6000_duplicates_broker1_crash.json) | [telemetry](http_6000_duplicates_broker1_crash_telemetry.json) |
| http_6000_duplicates_broker1_wait30 | [JSON](http_6000_duplicates_broker1_wait30.json) | [telemetry](http_6000_duplicates_broker1_wait30_telemetry.json) |

Дополнительные проверки:

- [Восстановление первого отказа одного брокера](http_6000_duplicates_broker1_crash_recovered.json).
- [Восстановление после потери кворума](http_1000_quorum_loss_recovered.json).
- [Сверка всех квитанций после отказов](after_fault_audits.json).
- [Подсчёт квитанций в гарантированных интервалах остановки](fault_intervals.json).
- [Сводные метрики и арифметика](profile_summary.json).
- [Среда](environment.json), [финальные проверки](verification.json), [Go-тесты](final-tests.log).
- [Первичная готовность](startup-verification.json), [финальное состояние](final-status.json), [ELR и другие features](features-initial.txt).
- [Проверка официального архива Kafka](download-verification.json).
- Конфигурации брокеров: [1](node1-server.properties), [2](node2-server.properties), [3](node3-server.properties).
- [Контрольные суммы опубликованных файлов](SHA256SUMS).
