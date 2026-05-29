# Controlled rollout — evidence_first_s2_fallback

**Дата:** 2026-05-29 · **Готовность:** принято на КР2, флаг по умолчанию OFF.

Цель: безопасно включить fallback для `too_large` enriched MD пар в production,
с возможностью мгновенного отката и без риска для остальных пар.

## Предусловия (gate перед enable)

- [x] Фича реализована (`d4e0c34`) и acceptance-фиксы влиты (`8d4c295`).
- [x] 20 unit-тестов зелёные (`tests/test_stage_comparison_evidence_first_fallback.py`).
- [x] Real Opus shadow run на КР2: `status=done`, 5/5 чанков, 55 grounded changes,
      831s. См. [results/kr2_acceptance/](results/kr2_acceptance/).
- [x] Batch preflight: `too_large → run` при флаге ON (а не `skip_too_large`).
- [x] Claude Code provider доступен (`claude --version` ok), модель `opus`.
- [ ] Оператор подтвердил бюджет времени: ~**170 s на чанк** Opus
      (КР2: 831s / 5). Большие пары = несколько чанков последовательно.

## Что включается одним флагом

```env
# .env (gitignored). Default = false (поведение too_large как раньше).
STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED=true
```

Дополнительные параметры (defaults безопасны, менять только при необходимости):

```env
STAGE_COMPARISON_EVIDENCE_FIRST_CHUNK_MAX_CHARS=200000   # бюджет (left+right) на чанк
STAGE_COMPARISON_EVIDENCE_FIRST_MAX_CHUNKS=16            # cap числа Opus-вызовов на пару
STAGE_COMPARISON_EVIDENCE_FIRST_HEADER_MAX_CHARS=12000   # cap shared global header
STAGE_COMPARISON_EVIDENCE_FIRST_MIN_QUOTE_LEN=8
STAGE_COMPARISON_EVIDENCE_FIRST_FUZZY_THRESHOLD=0.6      # порог grounding (token-overlap)
STAGE_COMPARISON_EVIDENCE_FIRST_DROP_UNGROUNDED=true     # выкидывать changes без evidence
```

Срабатывает ТОЛЬКО в too_large-ветке `run_enriched_comparison` и ТОЛЬКО при
доступном provider. Пары под лимитом 600K идут обычным путём — не затрагиваются.

## Этапы rollout

### Этап 0 — staging smoke (1 пара)

```bash
pkill -f "uvicorn backend.app.main"; uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload &
# Запустить unified-analysis на ИЗВЕСТНОЙ too_large паре с force_compare=true.
python experiments/stage_comparison_evidence_first_s2/scripts/verify_shadow.py  # проверить контракт+grounding
```
Acceptance: `status=done`, `strategy=evidence_first_s2_fallback`, `final_changes>0`,
все changes `evidence_verified=true`.

### Этап 1 — одна реальная сессия, ручной trigger

Включить флаг, прогнать unified-analysis по ОДНОЙ сессии с too_large парами
(в Балчуге их 3: ИОС-4.2 639K, КР2 865K, ООС1 1.19M). Проверить:
- batch preflight summary: `skip_too_large=0`, `will_run_fallback=N`;
- каждая too_large пара → `done` со strategy-маркером;
- diagnostics: `dropped_ungrounded` мало, `duplicates_removed` адекватно.

### Этап 2 — включить по умолчанию для too_large

После 1-2 успешных сессий оставить флаг ON постоянно. Пары под лимитом не
затронуты; too_large перестают быть «слепой зоной».

## Мониторинг (на что смотреть)

Per-pair `comparison_result.json → diagnostics`:

```bash
jq '{status, strategy, n:(.changes|length),
     det:.diagnostics.deterministic_changes,
     llm_raw:.diagnostics.llm_changes_raw,
     dropped:.diagnostics.llm_changes_dropped_ungrounded,
     dups:.diagnostics.duplicates_removed,
     final:.diagnostics.final_changes,
     chunks:[.diagnostics.chunk_results[]|{id:.chunk_id,st:.status,n:.changes_count}]}' \
  comparison/sessions/<sid>/pairs/<pid>/enriched_comparison/comparison_result.json
```

Red flags:
- `dropped_ungrounded` высок (> ~30% llm_raw) → enriched MD битый или Opus
  галлюцинирует; снизить `FUZZY_THRESHOLD` до 0.5 или проверить вход.
- много чанков `status != done` → provider/timeout; см. `warnings`.
- `final_changes=0` при `status=done` → проверить scope_map (возможно, нет
  common scope — пары несопоставимы).

## Rollback (мгновенный)

```env
STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED=false
```
Перезапустить backend. too_large пары снова отдают `too_large`/`changes=[]` как
раньше. Уже записанные fallback-результаты остаются на диске (status=done) —
при необходимости перезапустить пару с force_compare после отката (даст
too_large) или удалить `comparison_result.json` пары.

## Риски и митигации

| Риск | Вероятность | Митигация |
|---|---|---|
| Время: большие пары = N×170s Opus последовательно | средняя | cap `MAX_CHUNKS=16`; запускать too_large сессии вне пиков; chunk-параллелизм — будущая оптимизация (не в этом PR) |
| Стоимость Opus-вызовов (subscription) | средняя | срабатывает только на too_large (редкие пары); chunk budget ограничивает вход |
| Description-variance из low-confidence Qwen → ложные changes | средняя | evidence verification + `DROP_UNGROUNDED=true` снимают ungrounded; на КР2 — 0 ложных |
| Плохой alignment (пары с разной структурой листов) | низкая | scope-only разделы идут детерминированно; при отсутствии common scope `final_changes` может быть 0 — видно в diagnostics |
| Cross-chunk дубли глобальных фактов (штамп) | закрыто | global-singleton collapse (stamp 5→1) + chunk prompt запрещает дублировать штамп/состав |
| Дубли deterministic↔LLM по составу листов | закрыто | стадия 3.3 убрана; per-sheet — только LLM, scope-only — только deterministic |
| `confirmed_unique` ≠ `final_changes` (пайплайн не делает семантическую адъюдикацию) | принято | grounding + dedup автоматизированы; финальная аудит-оценка — на человеке (как и для обычного сравнения). Critic/corrector — следующий шаг при необходимости |
| Изменение SYSTEM_PROMPT сломает chunk-вывод | низкая | снимок `scripts/system_prompt_snapshot.txt`; diff при изменениях |

## Вне scope этого rollout (намеренно)

- НЕ добавляем OpenRouter / сторонние API (только Claude Code subscription).
- НЕ переписываем fallback-архитектуру.
- Параллелизация чанков, critic/corrector над fallback-changes, UI-бейдж
  strategy — возможные будущие улучшения, не блокируют enable.
