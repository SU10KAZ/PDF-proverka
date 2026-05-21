# Estimated LOC Changes

**Дата:** 2026-05-20

LOC оценки округлены до десятков. Включают комментарии и docstrings (Python),
markdown заголовки (для .md prompts/checklists). Effective review surface
~70% от total LOC (типичная пропорция signal/comments).

---

## Phase 0 LOC сводка

| Слой | NEW | MODIFIED | TOTAL |
|---|---|---|---|
| dedup package | +845 | 0 | +845 |
| findings_service.py | 0 | +30 | +30 |
| findings_merge/runner.py | 0 | +15 | +15 |
| core/config.py | 0 | +8 | +8 |
| tests | +270 | 0 | +270 |
| **TOTAL Phase 0** | **+1115** | **+53** | **+1168** |

Effective review surface (~70%): ~820 LOC.

---

## Phase 1 LOC сводка (поверх Phase 0)

| Слой | NEW | MODIFIED | TOTAL |
|---|---|---|---|
| completeness_lens.py | +250 | — | +250 |
| document_type_detector.py | +180 | — | +180 |
| completeness_lens_task.md | +130 | — | +130 |
| text_analysis_task.md | — | +81 (210−129) | +81 |
| discipline_checklists/*.md (8) | +920 | — | +920 |
| text_analysis/runner.py | — | +40 | +40 |
| task_builder.py | — | +15 | +15 |
| claude_runner.py | — | +25 | +25 |
| findings_service.py (Phase 1 add'l) | — | +50 | +50 |
| core/config.py (Phase 1 add'l) | — | +15 | +15 |
| schemas/text_analysis.json | — | +25 | +25 |
| tests (unit) | +180 | — | +180 |
| tests (integration + regression) | +350 | — | +350 |
| **TOTAL Phase 1 (add'l)** | **+2010** | **+251** | **+2261** |

Effective review surface (~70%): ~1580 LOC.

---

## Cumulative (Phase 0 + Phase 1)

| Метрика | Значение |
|---|---|
| Total LOC new | +3125 |
| Total LOC modified | +304 |
| **Grand total LOC delta** | **+3429** |
| Effective review surface (~70%) | ~2400 LOC |
| Files NEW | 18 |
| Files MODIFIED | 6 |

---

## Per-file breakdown (Phase 0)

| Файл | LOC |
|---|---|
| `backend/app/services/findings/dedup/__init__.py` | 5 |
| `backend/app/services/findings/dedup/class_dedup.py` | 540 |
| `backend/app/services/findings/dedup/fuzzy_dedup.py` | 300 |
| `backend/app/services/findings/findings_service.py` (Δ) | +30 |
| `backend/app/pipeline/stages/findings_merge/runner.py` (Δ) | +15 |
| `backend/app/core/config.py` (Δ) | +8 |
| `tests/findings/dedup/test_class_dedup.py` | 150 |
| `tests/findings/dedup/test_fuzzy_dedup.py` | 120 |

---

## Per-file breakdown (Phase 1, дополнительно)

| Файл | LOC |
|---|---|
| `backend/app/pipeline/stages/text_analysis/completeness_lens.py` | 250 |
| `backend/app/services/findings/document_type_detector.py` | 180 |
| `prompts/pipeline/ru/completeness_lens_task.md` | 130 |
| `prompts/pipeline/ru/text_analysis_task.md` (Δ полная замена) | 210 (был 129) |
| `backend/app/data/discipline_checklists/AR.md` | 120 |
| `backend/app/data/discipline_checklists/KJ.md` | 120 |
| `backend/app/data/discipline_checklists/KM.md` | 120 |
| `backend/app/data/discipline_checklists/EOM.md` | 120 |
| `backend/app/data/discipline_checklists/OV.md` | 120 |
| `backend/app/data/discipline_checklists/VK.md` | 120 |
| `backend/app/data/discipline_checklists/SS.md` | 120 |
| `backend/app/data/discipline_checklists/MULTI.md` | 80 |
| `backend/app/pipeline/stages/text_analysis/runner.py` (Δ) | +40 |
| `backend/app/pipeline/stages/prepare/task_builder.py` (Δ) | +15 |
| `backend/app/services/llm/claude_runner.py` (Δ) | +25 |
| `backend/app/services/findings/findings_service.py` (Δ Phase 1 add'l) | +50 |
| `backend/app/core/config.py` (Δ Phase 1 add'l) | +15 |
| `backend/app/schemas/text_analysis.json` (Δ) | +25 |
| `tests/findings/test_completeness_lens.py` | 100 |
| `tests/findings/test_document_type_detector.py` | 80 |
| `tests/integration/test_stage01_phase1.py` | 150 |
| `tests/regression/test_24_case_golden.py` | 200 |

---

## Caveats

1. **LOC включает docstrings и комментарии.** Класс `class_dedup.py` имеет
   ~150 строк docstrings / комментариев из 540. Это норма для production-кода;
   review-surface оценен ~70%.
2. **Discipline checklists (8 × ~120 LOC = ~920 LOC) — это data, не code.**
   Review предполагает domain-expertise (инженер-проектировщик по дисциплине),
   не software review. Можно делить на отдельные PR по дисциплинам.
3. **Prompt replacement (text_analysis_task.md, 129 → 210 LOC) — самый
   важный с точки зрения risk.** LOC сам по себе мал, но behavioural impact
   огромный. Review должен включать prompt-engineering pass + A/B на сейф-кейсах.
4. **Test LOC высокий (+800), но это плюс**, потому что каждый новый код-путь
   покрыт.
5. **Schema JSON delta (+25 LOC) опциональна.** Можно отложить — JSON schema
   не валидируется production-движком в рантайме сейчас (она используется
   только в test_a1_v2_schema-подобных тестах и в IDE для autocompletion).
6. **NO LOC delta в:** `backend/app/pipeline/manager.py`, frontend, Stage 02,
   Stage 03b, norms pipeline. Это сознательно — изоляция Phase 0/1.
