# 01 Блоки — внутренняя механика

Точка входа: `backend/app/pipeline/stages/block_analysis/runner.py:618` →
`gemma_findings_only.run_findings_only_for_project()`.
Развилка транспорта: `gemma_findings_only.py:1874-1881`.

```python
use_claude_cli = is_claude_cli_model(model)          # claude-*
use_codex_cli  = is_codex_model(model)               # codex/*
use_dual       = model == STAGE02_DUAL_MODEL_ID      # ensemble/gpt-codex
```

Оба пресета кладут `ensemble/gpt-codex` → работает ветка `use_dual`.
**Схема этапа 01 в двух пресетах ИДЕНТИЧНА.**

---

## Фактическая схема (оба пресета, действующий `.env`)

```
                    ОДИН графический блок
                             │
        ┌────────────────────┴────────────────────┐
        │  ОДИНАКОВЫЙ вход для всех трёх ног:     │
        │   · PNG-кроп блока (blocks_dir/file)    │
        │   · build_effective_block_context():    │
        │       block_context / routed_context,   │
        │       document_context (retrieval),     │
        │       document_type, page_neighbors,    │
        │       absence caveat                    │
        │   · один system_prompt (по дисциплине)  │
        │   · reasoning_effort = "low" (жёстко)   │
        └────────────────────┬────────────────────┘
                             │  asyncio.gather(return_exceptions=True)
      ┌──────────────────────┼──────────────────────┐
      ▼                      ▼                      ▼
 [нога 1] GPT           [нога 2] Codex         [нога 3] Codex-Sol
 openai/gpt-5.4         codex/gpt-5.4          codex/gpt-5.6-sol
 provider=openrouter    provider=codex_cli     provider=codex_cli
 HTTP + json_schema     codex exec             codex exec
 (платно, paid_guard)   (подписка)             (за STAGE01_THIRD_LEG_ENABLED=true)
      │                      │                      │
      └──────────────────────┼──────────────────────┘
                             ▼
              combine_detector_results()  — Python, БЕЗ дедупликации
              union всех находок, ref: gpt_openrouter:NNN / codex:NNN
              (у двух codex-ног сквозное смещение номеров)
                             │
                 detectors_complete?  (все ноги ok)
                    ├── нет → судья ПРОПУСКАЕТСЯ (status=skipped,
                    │          reason=partial_detector_failure)
                    │          + STAGE01_ABORT_ON_LEG_FAILURE_ENABLED=true
                    │            → этап останавливается целиком
                    └── да
                             ▼
        [СУДЬЯ] dual_review.review_dual_findings()
        модель = STAGE01_DUAL_REVIEW_MODEL = codex/gpt-5.6-sol
        транспорт = codex_runner.run_codex_json_messages
        вход: block_context (обрезка 120 000 симв.) + список находок
              + PNG блока, если gap_search_enabled
                             │
                 ОДИН вызов делает ДВЕ работы:
                 (а) сопоставление  (б) gap-search
                             ▼
        нормализация Python (normalize_review_payload):
          match      — та же проблема, эквивалентная детализация
          extension  — та же проблема, одно замечание существенно дополняет
          disputed   — один факт, но ответы противоречат друг другу
          new        — не сопоставлено ни с чем (unmatched_refs)
          gap        — новая проблема, найденная судьёй; cap 5 на блок
                       (STAGE01_MAX_GAP_FINDINGS), дедуп против уже найденного
                             ▼
        combined["parsed"]["findings"] = аннотированный union
        (ничего НЕ удаляется — судья только размечает и добавляет)
```

### Ответы на вопросы задания

| Вопрос | Ответ |
|---|---|
| Сколько независимых анализов на блок | **3** (при `STAGE01_THIRD_LEG_ENABLED=true`); 2 при выключенном флаге |
| Какие provider/model | openrouter `openai/gpt-5.4`; codex_cli `codex/gpt-5.4`; codex_cli `codex/gpt-5.6-sol` |
| Параллельно ли | Да, один `asyncio.gather`. Судья — строго после |
| Одинаковое ли изображение | Да, один и тот же PNG из `blocks_dir/block["file"]` |
| Одинаковый ли контекст | Да, `build_effective_block_context` с одними аргументами |
| Одинаковый ли prompt | Да, один `system_prompt` + один user-текст. Различаются только транспортные обёртки |
| Кто сравнивает | `codex/gpt-5.6-sol` (семантика) + Python (нормализация, дедуп gap-находок) |
| Кто судья | `codex/gpt-5.6-sol` |
| Кто делает gap-search | Тот же судья, тем же вызовом |
| Проверка после свода | Да, но не моделью: Верификатор = Python-проверки + Claude-«страж отсутствия» |
| Вызовов на один блок | **4** (3 ноги + 1 судья). Без третьей ноги — 3. При выпавшей ноге — 3 и стоп этапа |

### Термины разметки (dual_review.py:41-85, 384-393)

* `matches` — число relation=`match`;
* `extensions` — relation=`extension`, поле `extends` = `gpt_openrouter|codex|both`;
* `disputed` — relation=`disputed`, замечания об одном факте противоречат;
* `new` = `len(unmatched_refs) + len(accepted_gap)` — уникальные находки одной ноги **плюс** gap-находки;
* `gap_findings` — только добавленные судьёй.

### Что НЕ работает в этой схеме

* `STAGE01_PROTECTION_TABLE_CHECK_ENABLED=false` → детерминированная нога
  `deterministic/protection` не подключается.
* Промпт судьи (`REVIEW_SYSTEM_PROMPT`) описан для **двух** списков — «GPT и Codex».
  При трёх ногах обе codex-ноги схлопываются в одну сущность `codex:*`
  (`_detector_name`: любой `codex/` → `codex`). Пары в `relationships` могут быть
  только `gpt_ref`↔`codex_ref`; **сопоставление двух codex-ног между собой
  структурно невозможно** — их находки уходят в `new` или ловятся дедупом gap.
  Это факт кода, не предположение (`normalize_review_payload` отбрасывает пару,
  если `_detector_name(by_ref[gpt_ref]) != "gpt_openrouter"`).

### Параллельность блоков

`AUDIT_STAGE02_CODEX_PARALLELISM=2` → `asyncio.Semaphore(2)`.
Итого одновременно в воздухе: 2 блока × (3 ноги ‖) = до 6 вызовов, потом до 2 судей.
Общий бюджет процессов — `BUDGET_CODEX_CLI=20`, `BUDGET_CLAUDE_CLI=6` (`resource_budget`).

### Backstop

`block_hard_timeout_s = timeout_s + BLOCK_HARD_TIMEOUT_BUFFER_S`; `timeout_s`
по умолчанию `DEFAULT_TIMEOUT_S = 200`. Превышение → блок помечается неудачным,
семафор освобождается.
