# Stage Comparison Pipeline V2 — постоянный Claude LLM Runner

**Дата:** 2026-06-10
**Статус:** default-OFF — runner создаётся только явным вызовом, dry-run по
умолчанию работает с `llm_runner=None`.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_llm_runner.py](../backend/app/services/stage_comparison/pipeline_v2_llm_runner.py)

## Зачем нужен runner

Слой [Delta Explanation / Critic](stage_comparison_pipeline_v2_delta_explanation.md)
объясняет **только уже найденные deterministic deltas** (этап 5 — entity diff).
LLM в Pipeline V2 принципиально НЕ ищет отличия и НЕ просматривает том:
промпт строится по ОДНОЙ дельте (~2 КБ: значения + evidence обеих сторон +
запреты «НЕ ищешь новые отличия / НЕ добавляешь замечания / ТОЛЬКО переданную»).

До этого модуля real runner жил в одноразовом smoke-скрипте. Теперь это
постоянная, тестируемая фабрика, инъектируемая в dry-run.

```text
entity_diff_report.deltas
  → select_deltas_for_explanation (priority_only, max_deltas)
  → build_delta_explanation_prompt (одна дельта + evidence)
  → runner(prompt)                 ← ЭТОТ МОДУЛЬ (claude -p, subscription)
  → parse_delta_explanation_response (fail-soft)
  → delta_explanation_report.json
```

## Что runner НЕ делает

- НЕ запускается автоматически (ни при импорте, ни в default dry-run);
- НЕ запускает Qwen/Opus batch-jobs, md-enrichment, unified-analysis,
  pipeline-queue — никакого пересечения со старой Stage Comparison логикой;
- НЕ трогает LM Studio, runtime comparison data, production-артефакты;
- НЕ ищет отличия и НЕ добавляет дельты: deltas_total до/после прогона
  идентичен (проверено на real smoke ИОС5.2 — 373 → 373).

## Default-OFF

`run_pipeline_v2_dry_run(..., llm_runner=None)` — поведение без изменений:
каждая выбранная дельта получает `skipped_no_runner`. Канонический способ
«выключено» — именно `llm_runner=None`.

## Как передать runner в dry-run

```python
from backend.app.services.stage_comparison.pipeline_v2_llm_runner import (
    build_pipeline_v2_claude_runner,
)
from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
    run_pipeline_v2_dry_run,
)

runner = build_pipeline_v2_claude_runner()          # явное включение
summary = run_pipeline_v2_dry_run(
    left_package, right_package, out_dir,
    options={"delta_explanation": {"enabled": True,
                                   "selection_strategy": "priority_only",
                                   "max_deltas": 5,
                                   "include_high_confidence": False}},
    llm_runner=runner,
)
```

`build_pipeline_v2_claude_runner(options)`:

| Ключ | Default | Назначение |
|---|---|---|
| `enabled` | `True` | `False` → noop runner (`skipped`/`disabled`) |
| `model` | `sonnet` | модель `claude -p --model …` |
| `timeout_sec` | `240` | таймаут одного вызова |
| `work_dir` | временная папка | CWD subprocess'а (изолирован от CLAUDE.md репо); всегда абсолютизируется |
| `provider` | `ClaudeCodeProvider()` | инъекция провайдера (mock в тестах) |
| `check_availability` | `True` | быстрый `claude --version` при build; CLI нет → noop с причиной |

Контракт ответа (читает `_invoke_runner` delta explanation; provider/model
self-report из cleanup-коммита `563cf87`):

```json
{"provider": "claude", "model": "sonnet", "raw_status": "ok|failed|skipped",
 "raw_response": "...", "error": null}
```

## Fail-soft

- исключение/timeout/ошибка провайдера → `raw_status="failed"` у конкретной
  explanation (`status=failed`, `needs_human_review` critic), dry-run
  завершается сам (`completed_with_warnings`) — проверено на живом CLI-сбое
  в real smoke;
- CLI недоступен при build / `enabled=False` → noop runner
  (`raw_status="skipped"`, причина в `error`). Delta explanation трактует
  `skipped` как сознательный пропуск: дельты получают `skipped_no_runner` —
  ровно как при `llm_runner=None`, БЕЗ инфляции failed-метрик и
  `llm_failed_explanations`-warning'ов;
- битый JSON ответа → строка передаётся дальше,
  `parse_delta_explanation_response` отрабатывает fail-soft
  (`llm_response_parse_failed`).

## CLI envelope

`claude -p --output-format json` возвращает обёртку
`{"type": "result", "result": "<текст ответа>", ...}`.
`unwrap_claude_cli_response` разворачивает её до текста; plain-JSON
explanation проходит как есть; не-JSON/битый JSON возвращается строкой.

## Исправленный work_dir bug (ClaudeCodeProvider)

Smoke 2026-06-10 нашёл latent bug в
[text_llm_provider.py](../backend/app/services/stage_comparison/text_llm_provider.py):
при ОТНОСИТЕЛЬНОМ `work_dir` провайдер ставил `cwd=work_dir` и одновременно
передавал относительный `--append-system-prompt-file` — Claude CLI резолвил
его от нового CWD → задвоенный путь
(`work_dir/work_dir/_text_llm_system_prompt.tmp.md`) и ошибка
«Append system prompt file not found».

Фикс: `work_dir = Path(work_dir).resolve()` в начале `invoke` — sys-файл и
`cwd` всегда абсолютные. Регресс-тест (mock subprocess, относительный
work_dir, проверка существования файла от CWD subprocess'а) падает на старом
коде и проходит после фикса. Runner дополнительно абсолютизирует `work_dir`
на своей стороне.

## Controlled smoke (max_deltas=5/10)

1. Worktree `/home/coder/projects/PDF-proverka-pv2`, ветка Pipeline V2,
   чистое дерево. Output — только в git-excluded
   `diagnostics_pipeline_v2/smoke_*/`.
2. Пара: реальные prepared-квартеты (например ИОС5.2 stage_1 ↔ stage_2 из
   `comparison_sources/272…`), read-only.
3. `runner = build_pipeline_v2_claude_runner()`; options как в примере выше,
   `max_deltas=5` (первый прогон) или `10` (~40s/дельта → ~7 мин).
4. Проверить: 12 артефактов; `deltas_total` не изменился vs no-runner прогон;
   все `explanation.delta_id` существуют в diff; `provider="claude"`,
   `raw_status="ok"`; explanations непустые; coverage_notes сохранились;
   `git status --short` чист.
5. Эталон оценки качества — real smoke 2026-06-10 (5/5 ok, 0 hallucination,
   модель сама поднимала OCR-гипотезы и сомнения в сопоставлении).

## Тесты

[tests/test_stage_comparison_pipeline_v2_llm_runner.py](../tests/test_stage_comparison_pipeline_v2_llm_runner.py)
— unwrap envelope / plain / битый JSON, normalize (ProviderResult/dict/строка),
noop (disabled / provider_not_available), исключение → failed, source-grep на
отсутствие локальных/batch-LLM зависимостей, no-network с mock-провайдером,
интеграция с `explain_entity_diff_report` (provider/model metadata),
`llm_runner=None` без изменений, регресс work_dir-бага. Real Claude в тестах
НЕ вызывается.

## Связанные файлы

- [pipeline_v2_llm_runner.py](../backend/app/services/stage_comparison/pipeline_v2_llm_runner.py)
- [text_llm_provider.py](../backend/app/services/stage_comparison/text_llm_provider.py) — `ClaudeCodeProvider` (`claude -p`) + work_dir fix
- [pipeline_v2_delta_explanation.py](../backend/app/services/stage_comparison/pipeline_v2_delta_explanation.py) — потребитель runner'а
- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — точка инъекции `llm_runner`
