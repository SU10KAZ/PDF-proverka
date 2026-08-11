# 11J.1 — test report

## Live HTTPS fake E2E

Exact code commit: `3376c7a88456764971a9f4afb46bb7fbbf28a158`.

| Проверка | Итог |
|---|---|
| `.128` isolated center → `.31` worker, outbound HTTPS with TLS verification | PASS |
| Agent poll / claim / source download / Executor / EventOutbox | PASS |
| Result upload / ACK / import / central handoff | PASS |
| Claude+GPT+Codex | PASS, 14 predicted = 14 executed |
| Full Codex | PASS, 17 predicted = 17 executed |
| Full Codex targeted discipline/docnorm/mark_system | PASS, 1/1/1 |
| Freeze after claim and center frozen tail | PASS |
| Result routing hash round-trip | PASS |
| Test-secret scan | PASS, 0 leaks |
| Real Claude/Codex/OpenRouter runtime calls | 0 / 0 / 0 |

Harness result: **111/111 passed**. Финальный evidence:
`/tmp/11j1-network-integrity-3376c7a8/evidence/11j1_multi_provider_report.json`.

## Pytest

Команда охватила routing-plan contract/e2e, primary role mapping, OpenRouter
adapter/secret, multi-provider bridge, 11J.1 network contract и все
`test_distributed_workers_*` (включая package, EventOutbox, retention и stage
routing):

```text
1332 passed, 1 skipped, 3 failed in 289.28s
```

Три failure:

- `test_killing_agent_does_not_stop_the_audit`;
- `test_two_real_processes_overlap_and_third_waits`;
- `test_agent_restart_keeps_two_processes_and_creates_no_duplicates`.

Точно эти три теста отдельно запущены на base
`069987d5a1968461ef7f690e445a6676f4d0447c` и дали `3 failed in 224.20s` с
теми же timeout points. Новых failures относительно BASE: **0**.

Предупреждения: один `pytest-asyncio` default-scope deprecation и один
`passlib/crypt` deprecation; к маршрутизации 11J.1 не относятся.
