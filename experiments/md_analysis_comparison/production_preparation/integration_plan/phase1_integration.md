# Phase 1 Integration Plan — Completeness Lens + Document Type + Dedup

**Дата:** 2026-05-20
**Phase:** 1 (parallel Sonnet completeness lens + document_type routing + Phase 0 dedup)
**Готовность:** HOLD — требует remediation
([FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md),
[a1_v2_final_recommendation.md](../../algorithm_research/reports/a1_v2_final_recommendation.md))

---

## Архитектура Phase 1

```
Stage 01 (text_analysis)
  ├─► current_method (Opus, существующий) ──┐
  │                                          │
  └─► completeness_lens (Sonnet, NEW)  ──────┤
        ├─ DOCUMENT_TYPE routing             │
        └─ discipline checklist              │
                                             ▼
                                  findings_service.merge
                                  + class_dedup
                                  + fuzzy_dedup (опц.)
                                             │
                                             ▼
                                   03_findings.json (schema_version=2)
```

**Что проверено эмпирически** ([FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md)):
- document_type routing работает на `audit_comparison` (cross_01: 0 phantom findings).
- Graceful fallback на A0 при сбое лензы (`test_fallback_to_a0.py` PASS).
- Schema стабильна (`test_a1_v2_schema.py` PASS).
- Per-case missed-critical rate **halved** vs A0 на 16 кейсах.

**Что в gate-fail-status** ([_gating_evaluation.md](../../algorithm_research/reports/_gating_evaluation.md)):
- FP +44% на same-case-set (3 кейса) — НЕ noise, а beyond_gt_useful + wrong_severity
  + duplicate_of_gt по семантике (см.
  [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md): speculative noise = 0).
- strict_score -18% — артефакт scoring formula.
- Human review load +27%.

**Решение для production:** Phase 1 включается **opt-in только для
`audit_comparison`** + remediation (cap reduction для full_rd) до A/B.

---

## Сводка изменений

| Категория | Кол-во | Описание |
|---|---|---|
| NEW files | ~13 | lens runner, doc_type detector, prompt, 8 checklists, тесты |
| MODIFIED files | 6 | runner.py, prompt, task_builder, claude_runner, findings_service, config |
| DELETED | 0 | — |
| LOC delta (грубо) | +1100 / +145 | new / modified |

---

## NEW files

### `backend/app/pipeline/stages/text_analysis/completeness_lens.py` (~250 LOC)

Async-функция `run_completeness_lens(ctx) -> StageResult`. По схеме клона
существующего `text_analysis/runner.py`:

```python
async def run_completeness_lens(
    ctx: "PipelineStageContext",
    *,
    document_type: str,
    discipline_code: str,
    stage_label: str = "completeness_lens",
) -> StageResult:
    """Запустить параллельную Sonnet лензу completeness.

    Возвращает StageResult с findings в формате, совместимом с
    01_text_analysis.json. На любой сбой — возвращает empty findings и
    `meta.completeness_status='failed'` (graceful fallback).
    """
    if not STAGE01_COMPLETENESS_LENS_ENABLED:
        return StageResult.skipped(stage_label)
    if document_type not in STAGE01_COMPLETENESS_BY_DOC_TYPE:
        return StageResult.skipped(stage_label, reason="doc_type_not_routed")

    checklist_path = _load_checklist(discipline_code)
    prompt = _build_completeness_prompt(ctx, document_type, checklist_path)
    # вызов claude_runner.run_completeness_lens (см. ниже)
    ...
```

Точный список параметров `StageResult` — match existing pattern в `text_analysis/runner.py`.

### `backend/app/services/findings/document_type_detector.py` (~180 LOC)

Drop-in из `production_preparation/schemas/document_type_detection_rules.py`
(если файл создан) или новый файл, реализующий ту же логику.

```python
def detect_document_type(
    md_path: str,
    project_info: dict,
) -> tuple[str, float]:
    """
    Возвращает: (doc_type, confidence)
    doc_type ∈ {"full_rd", "audit_comparison", "tz_vs_rd", "specification_only"}
    confidence ∈ [0, 1]
    """
    # 1. Проверка project_info.document_type (manual override) → confidence=1.0
    # 2. Эвристика по содержимому MD:
    #    - audit_comparison → есть таблица типа "X vs Y" в первых 300 строк
    #    - tz_vs_rd → есть упоминание "ТЗ" + "РД" + явное противопоставление
    #    - specification_only → preferring spec-style: > 80% таблиц с маркой/типом
    #    - иначе full_rd
    # 3. Если confidence < STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN → default full_rd
    ...
```

### `prompts/pipeline/ru/completeness_lens_task.md` (~130 LOC)

Drop-in из `production_preparation/prompts/completeness_lens_production_prompt.md`.
Placeholder'ы: `{PROJECT_ID}`, `{MD_FILE_PATH}`, `{OUTPUT_PATH}`, `{DOCUMENT_TYPE}`,
`{DISCIPLINE_CHECKLIST}`, `{CHECKLIST_CONTENT}`.

### `prompts/pipeline/ru/text_analysis_task.md` (FULL REPLACEMENT, ~210 LOC)

Drop-in из `production_preparation/prompts/stage01_production_prompt.md`.
Это **полная замена** существующего шаблона (129 LOC → ~210 LOC).
Новые placeholder'ы: `{DOCUMENT_TYPE}`, `{DISCIPLINE_CHECKLIST}`,
`{DISCIPLINE_NORMS_FILE}` (последние два опциональны — если discipline registry
их не отдаёт, инжектится пустая строка).

### `backend/app/data/discipline_checklists/AR.md` (~120 LOC)
### `backend/app/data/discipline_checklists/KJ.md` (~120 LOC)
### `backend/app/data/discipline_checklists/KM.md` (~120 LOC)
### `backend/app/data/discipline_checklists/EOM.md` (~120 LOC)
### `backend/app/data/discipline_checklists/OV.md` (~120 LOC)
### `backend/app/data/discipline_checklists/VK.md` (~120 LOC)
### `backend/app/data/discipline_checklists/SS.md` (~120 LOC)
### `backend/app/data/discipline_checklists/MULTI.md` (~80 LOC)

Drop-in из `experiments/md_analysis_comparison/algorithm_research/prompt_optimization/checklists/*.md`
(qualified в [checklist_quality_report.md](../../algorithm_research/reports/checklist_quality_report.md)
+ [FINAL_SUMMARY.md §6](../../algorithm_research/reports/FINAL_SUMMARY.md)).

Loader: читается `completeness_lens.py` через `_load_checklist(discipline_code)`.

### `tests/findings/test_completeness_lens.py` (~100 LOC)
- Mock claude_runner.
- Graceful fallback на сбой (`completeness_status='failed'` → empty findings).
- `STAGE01_COMPLETENESS_LENS_ENABLED=false` → ранний return.
- `document_type` не в `STAGE01_COMPLETENESS_BY_DOC_TYPE` → skipped.

### `tests/findings/test_document_type_detector.py` (~80 LOC)
- 4 канонических кейса (по одному на каждый doc_type).
- Manual override через `project_info.document_type` (confidence=1.0).
- Low confidence → fallback `full_rd`.

---

## MODIFIED files

### `backend/app/pipeline/stages/text_analysis/runner.py` (152 → ~192 LOC; +~40 LOC)

В существующем `run_text_analysis()` (line 55+):

```python
import asyncio
from backend.app.pipeline.stages.text_analysis.completeness_lens import run_completeness_lens
from backend.app.services.findings.document_type_detector import detect_document_type
from backend.app.core.config import (
    STAGE01_COMPLETENESS_LENS_ENABLED, STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE,
)

async def run_text_analysis(ctx, *, stage_label="text_analysis", use_triage=False, ...):
    doc_type, conf = detect_document_type(ctx.md_path, ctx.project_info)
    ctx.meta["document_type"] = doc_type
    ctx.meta["document_type_confidence"] = conf

    # Параллельный запуск двух LLM-вызовов
    if STAGE01_COMPLETENESS_LENS_ENABLED:
        current_task = claude_runner.run_text_analysis(ctx, ...)
        lens_task = run_completeness_lens(ctx, document_type=doc_type, ...)
        current_result, lens_result = await asyncio.gather(
            current_task, lens_task, return_exceptions=True,
        )
        if isinstance(lens_result, Exception):
            if STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE:
                # log warning + продолжить только с current_result
                lens_result = StageResult.failed("completeness_lens", error=str(lens_result))
            else:
                raise
    else:
        current_result = await claude_runner.run_text_analysis(ctx, ...)
        lens_result = None

    return _merge_results(current_result, lens_result, ctx.meta)
```

`_merge_results()` — добавить как новую private-функцию (~25 LOC) или вынести
в findings_service.

### `prompts/pipeline/ru/text_analysis_task.md` (FULL REPLACEMENT)

Полная замена существующих 129 LOC на 210-LOC версию из
`production_preparation/prompts/stage01_production_prompt.md`.

Главные отличия от текущего:
- placeholder `{DOCUMENT_TYPE}` теперь обязателен;
- HARD RULE про routing по document_type;
- KILL-LIST на полноту, если `document_type != full_rd`;
- problem_class + affected_system требуются (для dedup);
- severity_reasoning для КРИТ ≤ 120 символов;
- `is_beyond_gt_useful` поле.

### `backend/app/pipeline/stages/prepare/task_builder.py` (+~15 LOC)

В `prepare_text_analysis_task()` (line 442) и `build_text_analysis_prompt()` (line 470):

```python
from backend.app.services.findings.document_type_detector import detect_document_type

# ... после _inject_discipline ...
doc_type, conf = detect_document_type(md_file_path, project_info)
task = (
    template
    .replace("{PROJECT_ID}", project_id)
    .replace("{OUTPUT_PATH}", output_path)
    .replace("{MD_FILE_PATH}", md_file_path)
    .replace("{DOCUMENT_TYPE}", doc_type)
)
return task
```

Также обновить `_inject_discipline()` (если она есть) — она должна теперь
инжектить `{DISCIPLINE_CHECKLIST}` (содержимое discipline checklist'а).

### `backend/app/services/llm/claude_runner.py` (line 307, +~25 LOC)

Добавить новую публичную функцию `run_completeness_lens()`:

```python
async def run_completeness_lens(
    project_id: str,
    output_path: str,
    md_path: str,
    document_type: str,
    discipline_code: str,
    ...
) -> dict:
    """Запустить Sonnet completeness lens.

    Модель: claude-sonnet-4-5 (по решению из FINAL_SUMMARY §6).
    Subprocess через `claude -p` (subscription, не API). См. MEMORY:
    'Только Claude Code subscription, не API'.
    """
    prompt = _load_completeness_prompt(project_id, ..., document_type, discipline_code)
    return await _run_claude_subprocess(prompt, model="sonnet", ...)
```

Имя функции и сигнатура — `match existing pattern` из `run_text_analysis()` на line 307.

### `backend/app/services/findings/findings_service.py` (1178 → ~1230 LOC; +~50 LOC)

Новый метод `_merge_completeness_into_findings()`:

```python
def _merge_completeness_into_findings(
    self,
    current_findings: list[dict],
    completeness_findings: list[dict],
    document_type: str,
) -> tuple[list[dict], dict]:
    """Объединить current_method + completeness, deduplicate."""
    from backend.app.services.findings.dedup import merge_across_methods

    method_map = {
        "current_method": current_findings,
        "completeness": completeness_findings,
    }
    merged, report = merge_across_methods(
        method_map,
        priority=["current_method", "completeness"],  # current_method выигрывает при tie
    )
    meta = {
        "completeness_applied": True,
        "completeness_findings_in": len(completeness_findings),
        "completeness_findings_kept": sum(
            1 for f in merged if "completeness" in (f.get("source_agents") or [])
        ),
        "dedup_report": report.to_dict(),
    }
    return merged, meta
```

Вызывается из существующего findings_merge-flow.

### `backend/app/core/config.py` (+~15 LOC)

```python
# ── Phase 1 completeness lens ──
STAGE01_COMPLETENESS_LENS_ENABLED: bool = os.getenv("STAGE01_COMPLETENESS_LENS_ENABLED", "false").lower() == "true"

# Маппинг doc_type → bool. Default: lens включена только для audit_comparison.
_dt_raw = os.getenv("STAGE01_COMPLETENESS_BY_DOC_TYPE", "audit_comparison")
STAGE01_COMPLETENESS_BY_DOC_TYPE: set[str] = {x.strip() for x in _dt_raw.split(",") if x.strip()}

STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD: int = int(os.getenv("STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD", "6"))
STAGE01_COMPLETENESS_MAX_FINDINGS_OTHER: int = int(os.getenv("STAGE01_COMPLETENESS_MAX_FINDINGS_OTHER", "10"))

STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN: float = float(os.getenv("STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN", "0.6"))
STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE: bool = os.getenv("STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE", "true").lower() == "true"
```

---

## Concurrency / cost

- 2 параллельных Claude CLI subprocess (асyncio.gather). Production уже делает
  параллельные claude calls в других местах (см. comparison/findings_merge).
- Subscription limits: лимит даёт примерный budget 5000 messages/8 часов. Phase 1
  на canary (1-2 проекта/сутки) укладывается в 5% бюджета.
- Wall-clock: +49% vs A0 (FINAL_SUMMARY §4 gating table). PASS.

## Output schema bump

- `meta.schema_version`: legacy = 1 (или отсутствует), Phase 1 = 2.
- Новые опциональные поля (см. `migration/migration_plan.md`):
  - в finding: `problem_class`, `affected_system`, `severity_reasoning`,
    `is_beyond_gt_useful`, `interface_type`, `discipline_pair`,
    `internal_duplicate_of`, `is_canonical`, `class_key`,
    `duplicate_count_in_cluster`, `source_agents`.
  - в meta: `document_type`, `document_type_confidence`,
    `completeness_applied`, `dedup_report`.
- Frontend читатели **должны** игнорировать неизвестные поля.

## Opt-in routing

`STAGE01_COMPLETENESS_BY_DOC_TYPE="audit_comparison"` (default):
- `full_rd` → completeness lens **не запускается**, только current_method.
- `audit_comparison` → lens запускается (proven good на cross_01/cross_02).
- `tz_vs_rd` → lens **не запускается** (требует remediation, см. FINAL_SUMMARY §4).
- `specification_only` → lens **не запускается** до accumulation 3+ кейсов.

Чтобы расширить — переопределить env var:
`STAGE01_COMPLETENESS_BY_DOC_TYPE="audit_comparison,specification_only"`.

## Тестовая стратегия

Полная стратегия — `tests/test_plan.md`. Краткое:

1. Unit: doc_type detector, completeness_lens (mock'нутый claude_runner).
2. Contract: schema_v2 outputs валидны через `text_analysis.json` (расширенный
   schema).
3. Integration: 1-3 кейса из datasets для каждой дисциплины. Сверка с GT.
4. Regression: 24-case golden. Сверка metric diffs с tolerance bands.
5. Stochasticity: 3-run на 6 informative cases. IQR/median ≤ 0.25.
6. Shadow mode: первые N production-проектов — lens работает, но output в logs
   только. Engineer review.

## Что НЕ делается на Phase 1

- НЕ меняется `manager.py`.
- НЕ меняется Stage 02, Stage 03b runner, norm_checks pipeline.
- НЕ добавляются дополнительные линзы (cross_discipline, normative, calculations) —
  они REJECTED ([final_verdict.md "What NOT to implement"](../../algorithm_research/reports/final_verdict.md)).
- НЕ меняется current_method prompt (Opus продолжает работать как сейчас).
  Caveat: Opus тоже не знает про document_type — это намечено в Phase 1 как
  "follow-up" ([FINAL_SUMMARY.md §4.4 (3)](../../algorithm_research/reports/FINAL_SUMMARY.md)).

## Развёртывание (rollout sequence)

1. Phase 0 deployed first (separate PR), `STAGE01_DEDUP_ENABLED=true`.
2. Phase 1 PR с дефолтами `*_ENABLED=false`.
3. Включить `STAGE01_COMPLETENESS_LENS_ENABLED=true` **только** через env override
   на canary-проекте (`audit_comparison`).
4. Сверить 5-10 canary-проектов с engineer review (production_validation_strategy.md).
5. При успехе — расширить `STAGE01_COMPLETENESS_BY_DOC_TYPE` (например, добавить
   `specification_only` после accumulation 3+ кейсов).
6. `full_rd` route — только после remediation (cap 14→6, см.
   [FINAL_SUMMARY §4.4](../../algorithm_research/reports/FINAL_SUMMARY.md)).
