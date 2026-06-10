# Stage Comparison Pipeline V2 — LLM Delta Explanation / Critic

**Дата:** 2026-06-10
**Статус:** первый LLM-слой Pipeline V2, **fail-soft**, runner инъектируется; по
умолчанию noop. НЕ UI, НЕ замена deterministic diff, НЕ старая логика.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_delta_explanation.py](../backend/app/services/stage_comparison/pipeline_v2_delta_explanation.py)
**Вход:** `entity_diff_report` (этап 4) + опц. graphic descriptor readiness.

## Зачем нужен LLM Delta Explanation / Critic

Deterministic diff (этап 4) даёт список атомарных дельт OLD↔NEW с evidence и
confidence. Этот слой берёт КОНКРЕТНУЮ дельту и: даёт краткое инженерное
объяснение, оценивает влияние для генподрядчика, проверяет grounded/evidence,
помечает сомнительные / OCR-шумные / требующие ручной проверки дельты. Это
делает дельты «читаемыми» для инженера, не теряя воспроизводимости.

```text
entity_diff_report.deltas (+ optional graphic readiness)
  → select_deltas_for_explanation     (priority_only / changed / low_conf / …)
  → per delta: build_graphic_context_for_delta
               build_delta_explanation_prompt  (контракт «только эта дельта»)
               llm_runner(prompt) -> raw        (INJECTABLE; в тестах — fake)
               parse_delta_explanation_response (fail-soft JSON)
  → build_delta_explanation_report (+ coverage_notes по слабой графике)
```

## Почему LLM НЕ ищет отличия

Ключевой инвариант (зашит в prompt и в архитектуру):

- LLM **НЕ** ищет новые отличия и **НЕ** просматривает весь том;
- LLM **НЕ** добавляет новые дельты и **НЕ** выдумывает замечания;
- LLM **НЕ** заменяет deterministic diff;
- LLM **только** объясняет/критически проверяет переданную дельту по её
  evidence (left/right).

Так сохраняется воспроизводимость: список отличий детерминированный, а LLM лишь
комментирует его. Это снимает «плавающие» результаты (34 vs 24 замечания),
характерные для «один большой Opus ищет отличия по всему тому».

## Вход — `entity_diff_report.deltas`

Обрабатываются дельты этапа 4 (`delta_type`, `entity_type`, `old/new_value`,
`evidence.left/right`, `confidence`, `quality_flags`, `page_numbers`, block ids).
Никакие исходные PDF/MD/crop не читаются.

## Selection (что отправляем в LLM)

`select_deltas_for_explanation(report, options)` — стратегии:

- `all` — все дельты;
- `changed_only` — только `changed`;
- `low_confidence` — confidence < `high_confidence_threshold` (0.75);
- `needs_human_review` — дельты с флагом `needs_human_review`;
- `priority_only` (**default**) — дельты с приоритетными флагами
  (`needs_human_review`/`possible_ocr_noise`/`fuzzy_match`/`low_match_score`/
  `one_sided_entity`), либо `uncertain`/`added`/`removed`, либо `changed` с
  невысокой уверенностью. **High-confidence clear `changed` пропускаются** (если
  не задан `include_high_confidence`).

`max_deltas` (default 20) ограничивает выборку — чтобы не отправлять весь том.
Options: `{mode, selection_strategy, max_deltas, include_high_confidence,
high_confidence_threshold}`.

## Runner — инъектируемый, fail-soft

`llm_runner: Callable[[str], str | dict]` — ИНЪЕКТИРУЕТСЯ. Модуль НЕ импортирует
claude/provider и НЕ делает сетевых вызовов сам. Возврат строки → raw text;
возврат dict (`{status, raw_response, error, provider?, model?}`) →
совместимость с provider-обёрткой (напр., будущая обёртка вокруг
`ClaudeCodeProvider` — но подключается СНАРУЖИ, не здесь).

### Provider metadata (cleanup 2026-06-10)

`explanation.model.provider` НЕ хардкодится — runner self-report'ит себя:

- dict-ответ с `provider`/`model` → значения попадают в `explanation.model`
  (`{"provider": "mock", "model": "fake-model-1"}` для теста; реальный
  claude-wrapper явно передаёт `provider="claude"`);
- string-ответ или dict без `provider` → `provider="custom_runner"`
  (анонимный инъектированный runner, НЕ «claude»);
- `llm_runner=None` → `provider="none"` (как раньше).

Fail-soft поведение:

- `llm_runner=None` → каждая дельта `skipped_no_runner` (noop, не падение);
- исключение runner'а → `failed`, дельта не теряется;
- битый/неполный JSON → `parse_delta_explanation_response` fail-soft
  (`needs_human_review` + флаг `llm_response_parse_failed`), поля обрезаются по
  длине, enum'ы клампятся к допустимым.

## Graphic readiness

`build_graphic_context_for_delta(delta, graphic_descriptor_report)` находит
descriptor блока дельты (поддерживает single-side report с `descriptors` и
combined `{left,right,matched}`). Если блок `low`/`not_usable` /
`needs_vision_enrichment` / `manual_review_recommended` — дельте добавляется флаг
`possible_weak_graphic`, и (важно) НЕ делается вывод «изменений нет»: пустота
трактуется как «нужна дообработка графики».

## Статусы и critic verdict

Статус explanation: `explained | critic_rejected | needs_human_review |
skipped_no_runner | failed`. Critic verdict: `accept | reject |
needs_human_review | possible_ocr_noise | possible_weak_graphic`. Маппинг:
accept→explained, reject→critic_rejected, остальные→needs_human_review.
groundedness verdict: `grounded | partially_grounded | not_grounded | unclear`.

## Coverage notes по графике

`coverage_notes` строятся из graphic descriptor **независимо от наличия
runner'а** (даже при `llm_runner=None`):

- `weak_graphic` — для блоков с `readiness` low/not_usable или флагами
  vision-enrichment/manual-review;
- `matched_risk` — для сопоставленных пар с `one_side_not_usable`/
  `low_token_overlap`/`*_mismatch`.

Coverage notes — это **НЕ дельты**; они нужны, чтобы UI/отчёт не трактовал
пустой diff как отсутствие изменений.

## Формат отчёта

`explain_entity_diff_report(entity_diff_report, graphic_descriptor_report=None,
options=None, llm_runner=None)` → `summary` (deltas/selected/explained/skipped/
failed/accepted/rejected/needs_human_review/possible_ocr_noise/
possible_weak_graphic totals + `by_risk_level`/`by_status`/`warnings_count`),
`selection` (strategy + selected_delta_ids), `explanations[]`, `coverage_notes[]`,
`warnings[]`. Запись — `write_delta_explanation_report` (атомарно).

## Что этап НЕ делает

- **НЕ** ищет отличия по всему тому и **НЕ** добавляет новые дельты;
- **НЕ** заменяет deterministic diff;
- **НЕ** скачивает `crop_url`, **НЕ** рендерит PDF, **НЕ** вызывает Qwen/Opus
  напрямую (только инъектированный runner);
- **НЕ** делает сетевых вызовов сам; импорты — только stdlib
  (`json/os/tempfile/pathlib/typing`).

## Интеграция в Dry Run (готово)

Delta Explanation уже подключён к
[Dry Run / Orchestrator](stage_comparison_pipeline_v2_dry_run.md): после entity
diff dry-run вызывает `explain_entity_diff_report(diff_report, {left/right/matched
graphic}, options["delta_explanation"], llm_runner)`, пишет
`delta_explanation_report.json`, добавляет секцию `delta_explanation` в
`pipeline_v2_summary.json` и раздел «Delta explanation / critic» в `.md`.
**По умолчанию `run_pipeline_v2_dry_run(..., llm_runner=None)` → `skipped_no_runner`
(offline, реальный LLM не вызывается).** `options["delta_explanation"].enabled=false`
→ этап `disabled`. Подключение fail-soft: падение не валит обязательные этапы.

## Как подключить дальше

- **Реальный runner:** тонкая обёртка вокруг существующего `claude -p`
  provider'а, инъектируется в `run_pipeline_v2_dry_run(..., llm_runner=...)`
  снаружи (controlled smoke, под флагом). Модуль её не создаёт.
- **UI:** показывать `explanations` рядом с дельтами + `coverage_notes` как
  предупреждение «по графике возможны пропуски, нужна дообработка».

## Тесты

[tests/test_stage_comparison_pipeline_v2_delta_explanation.py](../tests/test_stage_comparison_pipeline_v2_delta_explanation.py)
— synthetic diff/graphic отчёты + FAKE runner (без сети/Claude/subprocess):
выбор priority-дельт, пропуск high-confidence, `max_deltas`, prompt-контракт
(«не ищи новые отличия» + evidence), explained по fake JSON, fail-soft на битом
JSON, `skipped_no_runner` без runner'а, подсчёт `possible_ocr_noise`/
`needs_human_review`, `possible_weak_graphic` + coverage_notes по слабой графике
(в т.ч. без LLM), атомарная запись, отсутствие сети и provider-импортов,
интеграционный поток diff+graphic → explanation.

## Следующий блок

- **Controlled smoke** на одном маленьком реальном prepared package (без
  production/deploy, под флагом): сначала `llm_runner=None` (offline,
  `skipped_no_runner`), затем отдельным разрешённым запуском с fake/real runner —
  проверить качество объяснений/critic на живых дельтах и корректность
  `coverage_notes` по слабой графике.

## Связанные файлы

- [pipeline_v2_delta_explanation.py](../backend/app/services/stage_comparison/pipeline_v2_delta_explanation.py)
- [pipeline_v2_entity_diff.py](../backend/app/services/stage_comparison/pipeline_v2_entity_diff.py) — этап 4 (вход)
- [pipeline_v2_graphic_block_descriptor.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_block_descriptor.py) — graphic readiness
- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — оркестратор (точка будущей интеграции)
- [text_llm_provider.py](../backend/app/services/stage_comparison/text_llm_provider.py) — существующий `claude -p` provider (обёртка-runner подключается снаружи, не импортируется здесь)
