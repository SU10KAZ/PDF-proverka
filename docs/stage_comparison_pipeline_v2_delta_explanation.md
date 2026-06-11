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
  не задан `include_high_confidence`);
- `engineering_first` — квотированная выборка по selection-группам (см. ниже).

`max_deltas` (default 20) ограничивает выборку — чтобы не отправлять весь том.
Options: `{mode, selection_strategy, max_deltas, include_high_confidence,
high_confidence_threshold, engineering_first?}`.

### engineering_first — инженерное содержание раньше штампов

**Зачем.** На инженерных разделах (live-кейс ИОС1.1: 1014 дельт, из них
changed = stamp_field 326 / power_supply 16 / contents 2) `changed_only` по
confidence набирает почти одни штампы и не доходит до cable/equipment/power/
scheme. При этом инженерные сущности — атомарные: их изменение выглядит как
`removed`+`added`, а не `changed`. Deterministic diff НЕ меняется — меняется
только выборка для critic.

**Selection-группы** (`classify_selection_group`, по `entity_type`):

| группа | entity_type |
|---|---|
| `engineering` | cable, equipment, power_supply, scheme_component, scheme_connection_hint, table_row, requirement, norm_reference (+ нераспознанные не-weak типы, forward-compat) |
| `admin_stamp` | stamp_field |
| `navigation_contents` | contents_item, document_section, change_log_item |
| `weak_or_artifact` | unknown/пустой entity_type ИЛИ флаги possible_ocr_noise / low_match_score (one-sided evidence-флаги weak НЕ делают) |

**Правила:** кандидаты фильтруются как в `priority_only`
(`include_high_confidence` сохраняет семантику); внутри группы порядок
детерминированный — `changed` → `added` → `removed`, далее по убыванию
confidence, далее по delta_id; проход 1 — квоты групп; проход 2 — добор
остатка из leftovers в порядке приоритета групп (мало инженерных → слоты
достаются stamp/navigation, но не наоборот); проход 3 — overflow
per_subject_cap. `max_deltas` строгий.

**per_subject_cap** (`build_selection_group_key` =
`entity_type|subject|left_page|right_page`): композитная подпись штампа
дробится diff'ом на 4 атомарные дельты одного события (composite + role +
surname + date) — cap (default 2) не даёт одному событию занять квоту;
излишки уходят в конец выборки, не теряются.

**Пример включения (рекомендуемые опции):**

```json
{
  "selection_strategy": "engineering_first",
  "max_deltas": 20,
  "include_high_confidence": true,
  "engineering_first": {
    "engineering_quota": 12,
    "admin_stamp_quota": 4,
    "navigation_quota": 2,
    "weak_quota": 2,
    "per_subject_cap": 2
  }
}
```

Квоты/cap в примере = code-defaults (`_ENGINEERING_FIRST_DEFAULTS`), а вот
`include_high_confidence` в коде по умолчанию **false** — без явного `true`
high-confidence чистые `changed` (включая инженерные номиналы @0.9) выпадают
из кандидатов целиком (семантика как у `priority_only`). Для
engineering_first рекомендуется явно ставить `true` (так шёл smoke ИОС1.1).

Примечание к порядку: «weak в конец» относится к квотному проходу — добор
leftovers (проход 2) может поставить инженерные leftovers ПОСЛЕ weak-квоты,
это задокументированное перераспределение, а не нарушение приоритета.

Стратегия включается только явным указанием — default остаётся
`priority_only`, существующие стратегии не изменены.

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
- dict-ответ runner'а со `status`/`raw_status` = `skipped`/`disabled`
  (noop-runner из `pipeline_v2_llm_runner`) → тоже `skipped_no_runner`
  (сознательный пропуск, НЕ сбой — failed-метрики не инфлируются);
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

## Использование verdict в summary sections

Dry-run строит из вердиктов офлайн-секции `delta_sections` (см.
[dry run](stage_comparison_pipeline_v2_dry_run.md)): `accept`+show →
✅ confirmed; `needs_human_review` (и `possible_ocr_noise` с show=true и
risk≠none) → 🟡 needs_review; `possible_weak_graphic`/слабый graphic-контекст →
🟠 weak_graphic_review; `reject`, а также `possible_ocr_noise` с show=false
или risk=none → ⚪ noise (скрыто по умолчанию); failed/skipped/нечитаемый
ответ (`llm_response_parse_failed`) → 🔴. Сам этот модуль секций не строит —
только отдаёт explanations.

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

- **Реальный runner — ГОТОВ:**
  [pipeline_v2_llm_runner.py](../backend/app/services/stage_comparison/pipeline_v2_llm_runner.py)
  (`build_pipeline_v2_claude_runner()` поверх существующего `claude -p`
  provider'а) инъектируется в `run_pipeline_v2_dry_run(..., llm_runner=...)`
  явным вызовом — default dry-run остаётся `llm_runner=None`. Детали и
  smoke-runbook: [stage_comparison_pipeline_v2_llm_runner.md](stage_comparison_pipeline_v2_llm_runner.md).
  Этот модуль (delta_explanation) runner по-прежнему НЕ создаёт и НЕ
  импортирует.
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

## Grounded Vision Evidence в prompt (2026-06-11)

`build_delta_explanation_prompt`, `explain_single_delta` и
`explain_entity_diff_report` получили опциональный grounded-evidence вход:

* `build_delta_explanation_prompt(delta, graphic_context, options, grounded_evidence=None)`
  — если передана per-delta evidence-карточка (из
  `grounded_evidence_report.json`), в prompt добавляется секция
  `GROUNDED VISION EVIDENCE` + блок правил;
* `explain_single_delta(..., grounded_evidence=None)` — прокидывает карточку и
  фиксирует `grounded_evidence_level` / `grounded_evidence_used` в результате;
* `explain_entity_diff_report(..., grounded_evidence_report=None)` — строит
  индекс `delta_id → карточка` и подмешивает per-delta.

Правила в prompt'е (добавляются ТОЛЬКО при наличии evidence):

```text
Используй grounded evidence как ПОДТВЕРЖДАЮЩИЙ слой.
weak evidence трактуй как ПОДСКАЗКУ, требующую ручной проверки.
НЕ считай ungrounded/rejected vision-выводы фактами.
НЕ выдумывай изменений сверх переданной deterministic-дельты.
```

В секцию попадают только `confirmed` (как факт) и `weak` (помечены
`WEAK(hint)`). `conflict`/`rejected_only` показываются одной строкой-
предупреждением — rejected-якоря как факт НЕ всплывают. Без evidence prompt
полностью идентичен прежнему (backward-compat, старые тесты зелёные). Подробно —
[stage_comparison_pipeline_v2_grounded_evidence.md](stage_comparison_pipeline_v2_grounded_evidence.md).

## Связанные файлы

- [pipeline_v2_delta_explanation.py](../backend/app/services/stage_comparison/pipeline_v2_delta_explanation.py)
- [pipeline_v2_entity_diff.py](../backend/app/services/stage_comparison/pipeline_v2_entity_diff.py) — этап 4 (вход)
- [pipeline_v2_grounded_evidence.py](../backend/app/services/stage_comparison/pipeline_v2_grounded_evidence.py) — источник evidence-карточек
- [pipeline_v2_graphic_block_descriptor.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_block_descriptor.py) — graphic readiness
- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — оркестратор (этап [5b] grounded_evidence → [6] delta_explanation)
- [text_llm_provider.py](../backend/app/services/stage_comparison/text_llm_provider.py) — существующий `claude -p` provider (обёртка-runner подключается снаружи, не импортируется здесь)
