# 11J.1 — executable multi-provider routing: финальный отчёт

## Вердикт

- **ARCHITECTURE VERDICT: PASS.** Оба exact preset прошли настоящий outbound
  HTTPS fake E2E между isolated center `.128` и test-worker `.31`.
- **WORKER `.31` READINESS: NEEDS_OPENROUTER_PROVISIONING.** Ambient Claude и
  Codex авторизованы; настоящий OpenRouter credential отсутствует.

Architecture PASS не означает, что production rollout выполнен: production
не менялся, реальные provider calls и реальный аудит запрещены и не запускались.

## Итоговая матрица

| № | Вопрос | Ответ |
|---:|---|---|
| 1 | Architecture verdict | **PASS** |
| 2 | Worker `.31` readiness | **NEEDS_OPENROUTER_PROVISIONING** |
| 3 | Final base commit | `069987d5a1968461ef7f690e445a6676f4d0447c` |
| 4 | Final code commit | `3376c7a88456764971a9f4afb46bb7fbbf28a158`; docs-only evidence commit — exact `HEAD` в итоговом ответе |
| 5 | Live HTTPS `.128 ↔ .31` | **YES**; инициатор соединения — worker `.31 → .128`, TLS verified |
| 6 | Claude+GPT+Codex network | **PASS**, 14/14 |
| 7 | Full Codex network | **PASS**, 17/17 |
| 8 | Routing hash center→worker→result | **YES** для обоих jobs; mismatch test rejects |
| 9 | Block actions per block, third leg ON | **4**: OpenRouter detector + 2 Codex detectors + Codex judge |
| 10 | OpenRouter detector worker-side | **YES** |
| 11 | Codex detectors worker-side | **YES**, standard и strong |
| 12 | Judge worker-side after detector barrier | **YES** |
| 13 | Full Codex targeted merge actually executed | **YES**: base + discipline + docnorm + mark_system = 1/1/1/1 |
| 14 | Absence guard route | **Correct**: Claude/cheap_review в обоих presets |
| 15 | Optimization dual-provider | **Correct**: Claude primary ‖ Codex visual `xhigh`; merge/fix deterministic |
| 16 | Center tail frozen plan | **YES**, `FROZEN_ROUTING_PLAN FOUND` для A и B |
| 17 | Global preset switch changed running A | **NO**; A остался `codex_exec`, B получил новый preset |
| 18 | Grants automatic | **YES**, отдельные runtime grant IDs для Claude/Codex/OpenRouter; ручных 0 |
| 19 | Per-action exactly-once | **PASS**, unique ledger 17/17 и 14/14 |
| 20 | B=40 predicted | См. таблицу ниже |
| 21 | Fake executed trace equals budget | **YES** для обоих network fixtures, включая per-provider breakdown |
| 22 | Real OpenRouter credential `.31` | **UNCONFIGURED**; значение не читалось |
| 23 | Claude auth `.31` | **CONFIGURED**, official zero-inference auth status |
| 24 | Codex auth `.31` | **CONFIGURED**, official zero-inference login status |
| 25 | Real runtime Claude calls | **0** |
| 26 | Real runtime Codex calls | **0** |
| 27 | Real runtime OpenRouter calls | **0** |
| 28 | Credential leaks | **0** |
| 29 | Secret URL fragment leaks | **0**; diagnostic содержит только parsed host/scheme |
| 30 | Production changed by 11J.1 | **NO**; controlled hashes before=after |
| 31 | Final immutable review | **PASS**, 6/6 lenses, confirmed defects 0 |
| 32 | Operator action | Provision OpenRouter locally, then normal 11J rollout/re-registration |
| 33 | Safe provisioning | `11J_OPERATOR_PROVISIONING.md`; ключ вводится только на VPS, не в чат |
| 34 | Этот отчёт | `docs/distributed_audit_workers/11j/11J_FINAL_REPORT.md` |
| 35 | Можно ли переходить к одному real exact-preset audit | **YES, но только после provisioning и штатного production rollout** |
| 36 | Следующий рекомендуемый real test | Один небольшой non-sensitive AR document, preset Full Codex, B=1–3, MD+mark_system condition ON; заранее сверить plan hash/budget и остановиться после одного job |

## B=40, worker scope

Для fixture AR с MD и включённым optional `mark_system`:

| Preset | Third leg | Claude | Codex | OpenRouter | Total |
|---|---:|---:|---:|---:|---:|
| Claude+GPT+Codex | ON | 5 | 121 | 40 | **166** |
| Claude+GPT+Codex | OFF | 5 | 81 | 40 | **126** |
| Full Codex | ON | 2 | 127 | 40 | **169** |
| Full Codex | OFF | 2 | 87 | 40 | **129** |

Старый cap 64 не используется. Полные B=1/3/40 — в
`11J_CALL_BUDGET_NETWORK_COMPARISON.json`.

## Network evidence

- code revision: `git:3376c7a88456764971a9f4afb46bb7fbbf28a158`;
- worker id: `wrk_726eb885`;
- safe center host: `vegetarian-floyd-thorough-exotic.trycloudflare.com`;
- Full Codex job: `c4ed7aaa-dcc5-466b-934a-1652cfd8a35e`, hash
  `sha256:078a4dde6768ed599724d6ae2427fc9cb38ba967ba438f4114f744bca92d15be`;
- Claude+GPT+Codex job: `d8e704e5-662a-49af-b616-c83c925e1c49`, hash
  `sha256:7b978205cfc41b225b1301e86851bbe59a9975d50081d47169426c8a2609b611`;
- harness: **111/111 PASS**;
- source package, upload, ACK, import and center handoff: **PASS**;
- worker inbound runtime port: **not opened**;
- SSH transported job/package/provider data: **NO**.

## Production integrity

Before/after совпали: production `.env`, nginx tree, batch queue и metadata
238 941 файлов `projects_v2` (40 962 192 255 bytes). Production backend был
запущен до теста и не перезапускался им; production worker units не трогались.
В основной чужой ветке во время прогона появился отдельный concurrent commit
`9168c393`; он не создан 11J worktree, не входит в controlled surfaces и
сохранён без вмешательства. Подробности — `11J_PRODUCTION_INTEGRITY.json`.

## Tests и review

Relevant suite: `1332 passed, 1 skipped, 3 failed`. Ровно те же три slow
process-timeout теста отдельно падают на base `069987d5`; новых failures: 0.
Detached six-lens review итогового кода: PASS. Никакой реальный audit runtime
не вызывал Claude, Codex или OpenRouter.
