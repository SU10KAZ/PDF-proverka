# Files To Modify — Consolidated

**Дата:** 2026-05-20
**Связанные документы:** `phase0_integration.md`, `phase1_integration.md`.

Сводная таблица всех production-файлов, затрагиваемых Phase 0 + Phase 1.

---

## Сводка по action

| Action | Phase 0 | Phase 1 | Total |
|---|---|---|---|
| NEW | 5 | 13 | 18 |
| MODIFY | 3 | 6 | 6 (на Phase 1 пересекаются с Phase 0) |
| DELETE | 0 | 0 | 0 |
| READ-ONLY (контекст) | 2 | 5 | 7 |

---

## Полная таблица

| Файл | Action | LOC delta (~) | Phase | Risk | Заметка |
|---|---|---|---|---|---|
| `backend/app/services/findings/dedup/__init__.py` | NEW | +5 | 0 | LOW | пакет-импорт |
| `backend/app/services/findings/dedup/class_dedup.py` | NEW | +540 | 0 | LOW | drop-in копия |
| `backend/app/services/findings/dedup/fuzzy_dedup.py` | NEW | +300 | 0 | LOW | drop-in копия |
| `backend/app/services/findings/findings_service.py` | MODIFY | +30 (P0) +50 (P1) | 0+1 | MED | central merge point |
| `backend/app/pipeline/stages/findings_merge/runner.py` | MODIFY | +15 | 0 | LOW | вызов dedup после merge |
| `backend/app/core/config.py` | MODIFY | +8 (P0) +15 (P1) | 0+1 | LOW | env vars |
| `tests/findings/dedup/test_class_dedup.py` | NEW | +150 | 0 | LOW | unit |
| `tests/findings/dedup/test_fuzzy_dedup.py` | NEW | +120 | 0 | LOW | unit |
| `backend/app/pipeline/stages/text_analysis/completeness_lens.py` | NEW | +250 | 1 | MED | новый runner |
| `backend/app/services/findings/document_type_detector.py` | NEW | +180 | 1 | MED | эвристика |
| `prompts/pipeline/ru/completeness_lens_task.md` | NEW | +130 | 1 | LOW | drop-in |
| `prompts/pipeline/ru/text_analysis_task.md` | MODIFY (full replace) | 129 → 210 | 1 | **HIGH** | LLM-поведение меняется радикально |
| `backend/app/data/discipline_checklists/AR.md` | NEW | +120 | 1 | LOW | drop-in |
| `backend/app/data/discipline_checklists/KJ.md` | NEW | +120 | 1 | LOW | drop-in |
| `backend/app/data/discipline_checklists/KM.md` | NEW | +120 | 1 | LOW | drop-in (новый — KM checklist создан в этом исследовании) |
| `backend/app/data/discipline_checklists/EOM.md` | NEW | +120 | 1 | LOW | drop-in |
| `backend/app/data/discipline_checklists/OV.md` | NEW | +120 | 1 | LOW | drop-in |
| `backend/app/data/discipline_checklists/VK.md` | NEW | +120 | 1 | LOW | drop-in |
| `backend/app/data/discipline_checklists/SS.md` | NEW | +120 | 1 | LOW | drop-in |
| `backend/app/data/discipline_checklists/MULTI.md` | NEW | +80 | 1 | LOW | drop-in |
| `backend/app/pipeline/stages/text_analysis/runner.py` | MODIFY | 152 → ~192 (+40) | 1 | MED | asyncio.gather + fallback |
| `backend/app/pipeline/stages/prepare/task_builder.py` | MODIFY | +15 | 1 | MED | placeholder injection |
| `backend/app/services/llm/claude_runner.py` | MODIFY | +25 | 1 | MED | новая `run_completeness_lens` |
| `tests/findings/test_completeness_lens.py` | NEW | +100 | 1 | LOW | unit |
| `tests/findings/test_document_type_detector.py` | NEW | +80 | 1 | LOW | unit |
| `tests/integration/test_stage01_phase1.py` | NEW | +150 | 1 | MED | smoke integration |
| `tests/regression/test_24_case_golden.py` | NEW | +200 | 1 | MED | golden regression |
| `backend/app/schemas/text_analysis.json` | MODIFY (optional) | +25 | 1 | LOW | расширить дополнительными полями (опционально, не блокер) |

---

## READ-ONLY (контекст, не модифицируется)

| Файл | Зачем читать |
|---|---|
| `backend/app/pipeline/manager.py` | Понять как Stage 01 включается в pipeline (НЕ менять) |
| `backend/app/services/findings/finding_quality.py` | Понять как finding_quality относится к dedup (может быть synergy в будущем — не на Phase 0/1) |
| `backend/app/services/findings/migrated_findings_service.py` | Совместимость со старыми проектами |
| `backend/app/pipeline/context.py` | PipelineStageContext shape для completeness_lens |
| `backend/app/pipeline/stage_result.py` | StageResult shape |
| `frontend/static/js/app.js` | Убедиться, что schema_v2 поля игнорируются (frontend-side) |
| `backend/app/pipeline/stages/report/generate_excel_report.py` | Аналогично — schema_v2 поля не блокируют экспорт |

---

## Per-phase quick reference

### Phase 0 deployment (minimal scope)
NEW:
- `backend/app/services/findings/dedup/__init__.py`
- `backend/app/services/findings/dedup/class_dedup.py`
- `backend/app/services/findings/dedup/fuzzy_dedup.py`
- `tests/findings/dedup/test_class_dedup.py`
- `tests/findings/dedup/test_fuzzy_dedup.py`

MODIFY:
- `backend/app/services/findings/findings_service.py` (+30 LOC)
- `backend/app/pipeline/stages/findings_merge/runner.py` (+15 LOC)
- `backend/app/core/config.py` (+8 LOC)

### Phase 1 deployment (включая Phase 0)
Все из Phase 0, плюс:

NEW:
- `backend/app/pipeline/stages/text_analysis/completeness_lens.py`
- `backend/app/services/findings/document_type_detector.py`
- `prompts/pipeline/ru/completeness_lens_task.md`
- `backend/app/data/discipline_checklists/{AR,KJ,KM,EOM,OV,VK,SS,MULTI}.md`
- `tests/findings/test_completeness_lens.py`
- `tests/findings/test_document_type_detector.py`
- `tests/integration/test_stage01_phase1.py`
- `tests/regression/test_24_case_golden.py`

MODIFY:
- `prompts/pipeline/ru/text_analysis_task.md` (FULL REPLACE — 129 → 210 LOC)
- `backend/app/pipeline/stages/text_analysis/runner.py` (+40 LOC)
- `backend/app/pipeline/stages/prepare/task_builder.py` (+15 LOC)
- `backend/app/services/llm/claude_runner.py` (+25 LOC)
- `backend/app/services/findings/findings_service.py` (+50 LOC дополнительно к Phase 0)
- `backend/app/core/config.py` (+15 LOC дополнительно к Phase 0)
- `backend/app/schemas/text_analysis.json` (опционально, +25 LOC, для строгой схема-валидации новых полей)

---

## Risk-сводка (по файлам)

**HIGH risk:**
- `prompts/pipeline/ru/text_analysis_task.md` — полная замена шаблона, меняет
  поведение LLM. Mitigation: A/B на same project_id (см. tests/test_plan.md
  "production_shadow_tests").

**MED risk:**
- `findings_service.py` (1178 LOC) — large file, central merge point. Mitigation:
  90%+ coverage existing tests; новые методы изолированы.
- `runner.py` (text_analysis) — добавляет asyncio.gather + fallback path.
  Mitigation: `test_fallback_to_a0.py` уже валидирует graceful fallback.
- `task_builder.py` — placeholder injection. Mitigation: новый placeholder
  опциональный, default = `"full_rd"` если detector не сработал.
- `claude_runner.py` — новая subprocess function. Mitigation: уже есть проверенные
  паттерны для других стейджей.
- `document_type_detector.py` — эвристика. Mitigation: default fallback к
  `full_rd` (preserves status quo) + manual override через `project_info`.
- `completeness_lens.py` — новый runner. Mitigation: graceful fallback на сбой;
  feature-flagged.

**LOW risk:**
- Все NEW non-runner файлы (dedup, checklists, tests, prompts).
- `core/config.py` — только env vars.
- `findings_merge/runner.py` — простой вызов dedup, feature-flagged.

---

## Зависимости и порядок имплементации

```
1. config.py (env vars)
2. dedup/ package
3. tests/findings/dedup/
4. findings_service._apply_dedup
5. findings_merge/runner.py dedup hook
6. PHASE 0 PR ready ← merge here
─────────────────────────────────────
7. document_type_detector.py + tests
8. discipline_checklists/*.md (data drop)
9. completeness_lens_task.md (prompt drop)
10. claude_runner.run_completeness_lens
11. completeness_lens.py + tests
12. task_builder.py placeholder injection
13. text_analysis_task.md (FULL REPLACE) — после steps 7-12
14. text_analysis/runner.py (asyncio.gather)
15. findings_service._merge_completeness_into_findings
16. integration + regression tests
17. PHASE 1 PR ready ← merge here behind feature flag
```

Каждый PR не сломает текущее поведение, потому что:
- Phase 0: `STAGE01_DEDUP_ENABLED=false` default.
- Phase 1: `STAGE01_COMPLETENESS_LENS_ENABLED=false` default.
- Prompt replacement (step 13) — потенциальная regression. Mitigation: при
  `STAGE01_COMPLETENESS_LENS_ENABLED=false` старая лента сохраняется через
  conditional template loading (env-toggleable `TEXT_ANALYSIS_TASK_TEMPLATE`
  override) — это можно сделать так, чтобы новый prompt активировался только
  одновременно с Phase 1.
