# Phase 0 Integration Plan — Class + Fuzzy Dedup

**Дата:** 2026-05-20
**Phase:** 0 (dedup only, no LLM)
**Готовность:** APPROVED FOR DEPLOY ([FINAL_SUMMARY.md §3](../../algorithm_research/reports/FINAL_SUMMARY.md))

---

## Что меняется в production

Phase 0 — это **аддитивный пост-процессор** на хвосте findings merge. Он не трогает
Stage 01 prompt, не делает LLM-вызовов, не меняет схему. На A0 baseline он — no-op
(проверено `test_phase0_dedup_safety.py`, 8 кейсов × 3 варианта PASS, см.
[FINAL_SUMMARY.md §3](../../algorithm_research/reports/FINAL_SUMMARY.md)).

Реальную пользу даёт, когда впоследствии включится Phase 1 — там merge получит
findings из двух источников (current_method + completeness lens), и dedup отрежет
повторы. На текущем A0-pipeline просто стоит выключателем `OFF` или включается
без эффекта (provably no-op).

## Сводка изменений

| Категория | Кол-во | Описание |
|---|---|---|
| NEW files | 5 | dedup модули + тесты |
| MODIFIED files | 3 | findings_service, findings_merge/runner, config |
| DELETED | 0 | — |
| READ-ONLY (для контекста) | 2 | claude_runner.py, schemas/text_analysis.json |
| LOC delta (грубо) | +330 / +50 | new / modified |

---

## NEW files

### `backend/app/services/findings/dedup/__init__.py`
~5 LOC. Реэкспорт публичного API:
```python
from .class_dedup import collapse_to_canonical, mark_duplicates, merge_across_methods, DedupReport
from .fuzzy_dedup import fuzzy_collapse, DEFAULT_SIM_THRESHOLD
```

### `backend/app/services/findings/dedup/class_dedup.py`
~540 LOC. Drop-in копия `production_preparation/dedup/class_dedup.py`.
- Public API: `collapse_to_canonical(findings) -> (list, DedupReport)`, `mark_duplicates`, `merge_across_methods`.
- Stdlib-only, без зависимостей от parent stand.
- Severity weights уже сконфигурированы под production-формат (`КРИТИЧЕСКОЕ`, `ПРОВЕРИТЬ ПО СМЕЖНЫМ` с пробелом).

### `backend/app/services/findings/dedup/fuzzy_dedup.py`
~300 LOC. Drop-in копия `production_preparation/dedup/fuzzy_dedup.py`.
- Public API: `fuzzy_collapse(findings, sim_threshold=0.7) -> (list, DedupReport)`.
- Использует `difflib.SequenceMatcher.ratio()` — stdlib.
- КРИТИЧЕСКОЕ-protect: два КРИТ findings никогда не схлопываются.

### `tests/findings/dedup/test_class_dedup.py`
~150 LOC. Unit-тесты:
- Two findings same `(problem_class, affected_system)` → 1 канонический.
- Два КРИТ с одинаковым ключом → оба сохранены, `critical_collapsed_count > 0`.
- Output count ≤ input count.
- Source agents корректно агрегированы при `merge_across_methods`.

### `tests/findings/dedup/test_fuzzy_dedup.py`
~120 LOC. Unit-тесты:
- Похожие строки коллапсируются при threshold 0.7.
- КРИТ-protect работает аналогично class_dedup.
- На A0 baseline 8-кейсов — no-op (provably).

---

## MODIFIED files

### `backend/app/services/findings/findings_service.py` (+~30 LOC)

**Где:** в существующем методе, который записывает финальный `03_findings.json`.
По текущей структуре (1178 LOC) это, скорее всего, метод вида
`_finalize_findings(findings, project_id) -> list` или близкий — точный
function name надо подтвердить при имплементации.

**Что добавить (псевдокод):**
```python
from backend.app.core.config import STAGE01_DEDUP_ENABLED, STAGE01_DEDUP_FUZZY_THRESHOLD
from backend.app.services.findings.dedup import (
    collapse_to_canonical, fuzzy_collapse,
)

def _apply_dedup(self, findings: list, meta: dict) -> tuple[list, dict]:
    if not STAGE01_DEDUP_ENABLED:
        return findings, meta
    findings, class_report = collapse_to_canonical(findings)
    findings, fuzzy_report = fuzzy_collapse(
        findings, sim_threshold=STAGE01_DEDUP_FUZZY_THRESHOLD,
    )
    meta = {
        **meta,
        "dedup_report": {
            "class_dedup": class_report.to_dict(),
            "fuzzy_dedup": fuzzy_report.to_dict(),
        },
    }
    return findings, meta
```

**Вызов:** перед `_write_findings_json()` или эквивалентным записывающим методом.

### `backend/app/pipeline/stages/findings_merge/runner.py` (+~15 LOC)

**Где:** функция, которая после merge T+G→F пишет финальный `03_findings.json`
(существующий поток).

**Что добавить:**
- импорт `from backend.app.services.findings.dedup import collapse_to_canonical`;
- вызов `findings, report = collapse_to_canonical(merged_findings)` **после**
  существующего merge, но **до** `_validate_and_repair_json()`;
- запись `report.to_dict()` в `meta.dedup_report`.

**Feature flag:** wrapped in `if STAGE01_DEDUP_ENABLED:` (default `False`).

### `backend/app/core/config.py` (+~8 LOC)

Сразу после блока `TEXT_ANALYSIS_TASK_TEMPLATE = ...` (line 78) добавить:

```python
# ── Phase 0 dedup ──
STAGE01_DEDUP_ENABLED: bool = os.getenv("STAGE01_DEDUP_ENABLED", "false").lower() == "true"
STAGE01_DEDUP_FUZZY_THRESHOLD: float = float(os.getenv("STAGE01_DEDUP_FUZZY_THRESHOLD", "0.7"))
```

`STAGE01_DEDUP_ENABLED=false` по умолчанию — это и есть rollback-friendly default.

---

## READ-ONLY (для контекста, не модифицируются на Phase 0)

- `backend/app/services/llm/claude_runner.py` — Stage 01 LLM-вызов остаётся как есть.
- `backend/app/schemas/text_analysis.json` — схема не меняется на Phase 0 (поля
  `class_key`, `is_canonical`, `duplicate_count_in_cluster`, `source_agents`
  опциональны — старые читатели их проигнорируют).

---

## Feature flag поведение

| `STAGE01_DEDUP_ENABLED` | Поведение |
|---|---|
| `false` (default) | dedup-импорт не используется; merge runner ведёт себя как сейчас; никаких новых полей |
| `true` | После merge добавляется dedup-проход; `meta.dedup_report` появляется в `03_findings.json` |

Откат: установить `STAGE01_DEDUP_ENABLED=false` → следующий pipeline-run полностью
soft-skip'нет код. Никаких миграций данных не требуется.

---

## Migration impact

- Существующие `03_findings.json` **не трогаются** — Phase 0 действует только на
  новые выводы.
- Frontend / Excel-экспорт должны быть толерантны к новым опциональным полям
  (`class_key`, `is_canonical`, `duplicate_count_in_cluster`, `source_agents`,
  `meta.dedup_report`) — они уже игнорируют неизвестные поля по дизайну (см.
  CLAUDE.md §"Sheet vs Page" — frontend уже парсит legacy format).
- Cross-project запросы (`/api/optimization/summary/all`) не затрагиваются.

## Тестовая стратегия (см. также tests/test_plan.md)

1. Unit: `test_class_dedup.py`, `test_fuzzy_dedup.py` — 100% покрытие новых модулей.
2. Contract: на 8 кейсов из `experiments/.../datasets/` — `total_out <= total_in`,
   `missed_critical` не растёт. Эквивалент `test_phase0_dedup_safety.py` из
   experiments-стенда, перенесённый под production-импорты.
3. Regression: golden 24-case dataset (см. `tests/regression_strategy.md`).
4. Smoke: 1 реальный проект из `projects/EOM/...` с `STAGE01_DEDUP_ENABLED=true` —
   убедиться, что merge runner не падает и `03_findings.json` валиден.

## Что НЕ делается на Phase 0

- НЕ меняется Stage 01 prompt (`prompts/pipeline/ru/text_analysis_task.md`).
- НЕ добавляется completeness lens.
- НЕ добавляется document_type detection.
- НЕ меняется `claude_runner.py` / `text_analysis/runner.py`.
- НЕ затрагиваются Stage 02, Stage 03b, norm_checks.

## Развёртывание

1. PR содержит только NEW + MODIFIED файлы выше.
2. Default `STAGE01_DEDUP_ENABLED=false` → мерж безопасен.
3. Включить на canary-проект: `STAGE01_DEDUP_ENABLED=true` через env override.
4. Сверить: `meta.dedup_report.total_in == meta.dedup_report.total_out`
   (no-op на A0 baseline — это ожидаемое поведение, см.
   [FINAL_SUMMARY.md §3](../../algorithm_research/reports/FINAL_SUMMARY.md)).
5. Если Phase 1 далее включается — Phase 0 dedup автоматически начнёт сокращать
   повторы merge T+G+completeness.
