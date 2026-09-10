Увеличение нагрузки до 200k/с, 10.09.2026.

[Интерпретация результатов](../../../log-expansion-results.md). Приватные ledgers с синтетическими токенами остаются в `.local/logbench`; здесь агрегаты и диагностические артефакты.

| Профиль | Результаты | Команда, CPU/RSS |
|---|---|---|
| expansion_http_100000_cpu | [JSON](expansion_http_100000_cpu.json) | [telemetry](expansion_http_100000_cpu_telemetry.json) |
| expansion_http_200000 | [JSON](expansion_http_200000.json) | [telemetry](expansion_http_200000_telemetry.json) |
| expansion_direct_200000_w8192 | [JSON](expansion_direct_200000_w8192.json) | [telemetry](expansion_direct_200000_w8192_telemetry.json) |
| expansion_direct_200000_l5 | [JSON](expansion_direct_200000_l5.json) | [telemetry](expansion_direct_200000_l5_telemetry.json) |

Дополнительные артефакты:

- [Сводные числа](expansion-summary.json).
- [Повторная сверка всех новых квитанций](expansion-after-audits.json).
- [CPU profile](expansion_http_100000_cpu.cpu.pprof), [flat](expansion_http_100000_cpu_top.txt), [cumulative](expansion_http_100000_cpu_cumulative.txt).
- [Сборка и проверки](verification.json), [Go-тесты](expansion-final-tests.log).
- [Исходная среда](environment.json), [план и дополнительные пределы](expansion-environment.json), [финальное состояние](expansion-final-status.json).
- [Контрольные суммы](SHA256SUMS).
