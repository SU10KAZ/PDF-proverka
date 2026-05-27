# Estimated Risk Inventory

**Дата:** 2026-05-20
**Связанные документы:**
- [FINAL_SUMMARY §7 (Risks left)](../../algorithm_research/reports/FINAL_SUMMARY.md)
- [final_verdict.md (Risk summary)](../../algorithm_research/reports/final_verdict.md)
- [a1_v2_final_recommendation.md (Risks left)](../../algorithm_research/reports/a1_v2_final_recommendation.md)

Severity scale: LOW / MED / HIGH.
Probability scale: LOW (< 10%) / MED (10-40%) / HIGH (> 40%).

---

## Phase 0 risk inventory (5 пунктов)

### R0.1 — fuzzy_dedup схлопывает семантически разные findings
- **Severity:** MED
- **Probability:** LOW
- **Описание:** При threshold 0.7 SequenceMatcher может сматчить два разных
  finding'а с похожим лексическим naming.
- **Mitigation:**
  - КРИТИЧЕСКОЕ-protect никогда не позволит схлопнуть два КРИТ.
  - Threshold (`STAGE01_DEDUP_FUZZY_THRESHOLD=0.7`) tuneable через env.
  - На A0 8-кейсов validated — `same_class_drops = 0` (no-op).
- **Detection:** `DedupReport.same_class_drops` > 0 на A0 проектах → флаг для review.

### R0.2 — class_key derivation падает на новых вариациях категорий
- **Severity:** LOW
- **Probability:** LOW
- **Описание:** Если в finding отсутствует `problem_class`, ключ строится
  fallback'ом по `category + signature(problem)`. На редких категориях это
  может дать неожиданные ключи.
- **Mitigation:** все категории заранее заданы и проверены тестами; baseline
  фоллбэк не падает с exception, только даёт уникальный ключ → no collapse.
- **Detection:** unit-тесты + DedupReport.

### R0.3 — `meta.dedup_report` ломает старых читателей
- **Severity:** LOW
- **Probability:** LOW
- **Описание:** Frontend/Excel/Export могут не знать про новое поле.
- **Mitigation:** Frontend уже толерантен к unknown полям (см. CLAUDE.md
  "Sheet vs Page" — frontend парсит legacy format graceful). Verify через
  smoke test.
- **Detection:** smoke test после canary deploy.

### R0.4 — race condition при одновременной записи `03_findings.json`
- **Severity:** LOW
- **Probability:** LOW
- **Описание:** Dedup-проход выполняется после merge, до `_validate_and_repair_json`.
  Если другой код пишет файл одновременно — конфликт.
- **Mitigation:** дедуп проход чисто in-memory; запись остаётся за тем же
  существующим writer'ом → не появляется новой race condition vs текущий код.
- **Detection:** существующие тесты merge.

### R0.5 — assertion failure на pathological input (empty findings, deeply nested)
- **Severity:** LOW
- **Probability:** LOW
- **Описание:** Assertion `output ≤ input` может теоретически нарушиться при
  багах в clustering.
- **Mitigation:**
  - assertion на месте; тесты покрывают edge-кейсы (пустой list, дубликаты,
    КРИТ × 2 same key).
  - На любой `AssertionError` — feature flag можно выключить.
- **Detection:** assertion log в production = signal для отката.

**Aggregate Phase 0 risk: LOW.** Все 5 рисков либо имеют detection-механизм,
либо mitigated через feature flag.

---

## Phase 1 risk inventory (12 пунктов)

### R1.1 — Sonnet completeness lens fails (rate limit / network / overload)
- **Severity:** MED (downgraded к LOW из-за graceful fallback)
- **Probability:** MED
- **Описание:** Sonnet через `claude -p` subprocess может упасть, sub-limit'нуть.
- **Mitigation:** `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE=true` (default) →
  pipeline продолжает с current_method only.
- **Detection:** `meta.completeness_status='failed'` в `03_findings.json`.
- **Validation:** `test_fallback_to_a0.py` PASS ([a1_v2_final_recommendation.md "What is proven"](../../algorithm_research/reports/a1_v2_final_recommendation.md)).

### R1.2 — document_type detector mis-detects on edge cases
- **Severity:** MED
- **Probability:** MED
- **Описание:** Эвристика на MD-содержимом может ошибиться на нетипичных
  layout'ах. Например, проект-документ с ТЗ-fragment'ом в начале может быть
  ошибочно классифицирован как `tz_vs_rd`.
- **Mitigation:**
  - Manual override через `project_info.document_type` — confidence=1.0 bypass.
  - `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN=0.6` → ниже порога → fallback к
    `full_rd` (preserves status quo).
- **Detection:** `meta.document_type` + `meta.document_type_confidence` в выводе.
  Engineer review в canary periods.

### R1.3 — completeness lens too aggressive на `full_rd` → FP explosion
- **Severity:** HIGH (это уже наблюдалось, FINAL_SUMMARY §4)
- **Probability:** HIGH (если включить на full_rd) / LOW (route disabled)
- **Описание:** На 11 full_rd кейсах A1-v2 strict_score = 23.1 vs A0 = 49.8.
- **Mitigation:**
  - Default `STAGE01_COMPLETENESS_BY_DOC_TYPE="audit_comparison"` — full_rd НЕ
    включён.
  - `STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD=6` (cap reduced с 14, см.
    remediation in [FINAL_SUMMARY §4.4](../../algorithm_research/reports/FINAL_SUMMARY.md)).
- **Detection:** строгое разрешение route only после remediation valid'ируется
  на стенде.

### R1.4 — prompt replacement (text_analysis_task.md) regression
- **Severity:** HIGH
- **Probability:** MED
- **Описание:** Новый prompt меняет поведение Opus current_method. Может
  ухудшить performance даже без активной лензы.
- **Mitigation:**
  - A/B на 24-case golden — gate на missed_critical (must be ≤ A0).
  - При regression — env переменная `TEXT_ANALYSIS_TASK_TEMPLATE` может
    указать на legacy prompt path (Phase 1 PR должен оставить fallback path
    в код).
- **Detection:** regression suite (см. `tests/regression_strategy.md`).
- **Validation status:** **NEEDS TESTING на A1-v2 stand с only-current_method
  variant** — пока не сделано.

### R1.5 — checklist load failure (missing file, corrupt MD)
- **Severity:** LOW
- **Probability:** LOW
- **Описание:** `_load_checklist("EOM")` упадёт если `data/discipline_checklists/EOM.md`
  отсутствует.
- **Mitigation:**
  - Try/except в `completeness_lens._load_checklist()` → empty checklist →
    lens либо возвращает `applicability='not_applicable'`, либо skipped.
  - Тесты включают "missing checklist" сценарий.
- **Detection:** logger warning + `meta.completeness_status='no_checklist'`.

### R1.6 — schema v2 backwards compat (legacy projects)
- **Severity:** MED
- **Probability:** LOW
- **Описание:** Старые проекты (`schema_version=1` или отсутствует) могут
  быть прочитаны новыми consumer'ами и упасть на отсутствие новых полей.
- **Mitigation:**
  - Все новые поля **optional** (default None / empty list).
  - Migration plan: новые consumers default'ят missing fields (см.
    `migration/migration_plan.md`).
  - No data backfill required.
- **Detection:** legacy projects (любой пред-Phase-1) — нужна явная проверка
  на E2E test.

### R1.7 — concurrency cost (Sonnet + Opus parallel)
- **Severity:** LOW
- **Probability:** MED
- **Описание:** 2 параллельных Claude CLI subprocesses едят subscription budget
  быстрее.
- **Mitigation:**
  - FINAL_SUMMARY §4 gating: cost +49% (PASS, < 70% threshold).
  - Route disabled на full_rd → реально 0% impact для большинства проектов.
  - PAID_API_ENABLED + daily limit guard (см. MEMORY: paid_api_guard).
- **Detection:** paid_cost tracking.

### R1.8 — Opus current_method не знает про document_type
- **Severity:** MED
- **Probability:** HIGH
- **Описание:** Текущий prompt update даёт `{DOCUMENT_TYPE}` Opus'у, но
  поведение не валидировано на full corpus. На ar_03 наблюдался 1/15 phantom
  finding от current_method ([FINAL_SUMMARY §4.3](../../algorithm_research/reports/FINAL_SUMMARY.md)).
- **Mitigation:**
  - В новом prompt'е есть KILL-LIST на полноту-вопросы для non-full_rd.
  - Если phantom-rate растёт → Phase 1 follow-up: enhance current_method prompt
    тоже (отмечено как future iteration).
- **Detection:** regression на specification_only / tz_vs_rd кейсах.

### R1.9 — Stochasticity (3-run variance) НЕ измерена
- **Severity:** MED
- **Probability:** MED
- **Описание:** Phase 1 ни разу не запускалась 3 × на одном и том же кейсе.
  Variance unknown.
- **Mitigation:** stochasticity testing **обязателен до canary** (см.
  `tests/stochasticity_strategy.md`).
- **Detection:** 3-run IQR/median > 0.25 → block deploy.

### R1.10 — Discipline checklist drift vs norms_db
- **Severity:** LOW
- **Probability:** MED (со временем)
- **Описание:** Чек-листы цитируют конкретные пункты норм. При обновлении
  норматива чек-лист может стать stale.
- **Mitigation:** периодический CI-check, сравнивающий ссылки в чек-листах с
  `norms_db.json` (отмечено в FINAL_SUMMARY §7 Risks left).
- **Detection:** scheduled job, manual quarterly review.

### R1.11 — Frontend/Excel экспорт не обрабатывает `is_beyond_gt_useful`
- **Severity:** LOW
- **Probability:** LOW
- **Описание:** Поле `is_beyond_gt_useful=true` должно отображаться
  пользователю как "дополнительное замечание, не нарушение нормы".
- **Mitigation:** Frontend изменения — отдельный PR, можно отложить (поле
  optional, default skipped).
- **Detection:** UX feedback в canary.

### R1.12 — Sonnet completeness lens пишет в неправильный output path
- **Severity:** MED
- **Probability:** LOW
- **Описание:** Новый prompt должен писать в отдельный staging-файл (не
  в `01_text_analysis.json` напрямую) — иначе current_method перезапишет.
- **Mitigation:** `completeness_lens.py` пишет в
  `_output/01_text_analysis_completeness.json`; findings_service делает merge
  и пишет финальный `01_text_analysis.json`.
- **Detection:** integration test.

---

## Aggregate verdict

| Phase | Aggregate risk | Главные blockers |
|---|---|---|
| Phase 0 | **LOW** | нет |
| Phase 1 | **MED** | R1.3 (full_rd FP), R1.4 (prompt regression), R1.9 (stochasticity) |

**Phase 0 — ready to deploy.** Все риски либо detection-механизмы есть, либо
feature flag mitigated.

**Phase 1 — HOLD до remediation:**
1. R1.9 — обязательно run stochasticity до canary.
2. R1.3 — оставить `STAGE01_COMPLETENESS_BY_DOC_TYPE` только `audit_comparison`
   до отдельной remediation pass для full_rd.
3. R1.4 — добавить regression test на 24-case golden до merge.

После этих 3 пунктов — canary OK.

---

## Risk-mitigation matrix (одной таблицей)

| Risk | Severity | Mitigation kind | Status |
|---|---|---|---|
| R0.1 fuzzy collapse | MED | threshold tuning | mitigated |
| R0.2 class_key fallback | LOW | unit tests | mitigated |
| R0.3 schema-v2 readers | LOW | already tolerant | verified |
| R0.4 race condition | LOW | no new races | verified |
| R0.5 assertion | LOW | feature flag | mitigated |
| R1.1 lens failure | MED | graceful fallback | **proven** |
| R1.2 doc_type misdetect | MED | manual override + fallback | mitigated |
| R1.3 full_rd FP | HIGH | route disabled by default | mitigated |
| R1.4 prompt regression | HIGH | regression suite + fallback path | **TODO** |
| R1.5 checklist load | LOW | try/except | mitigated |
| R1.6 schema compat | MED | optional fields | mitigated |
| R1.7 cost | LOW | within budget | verified |
| R1.8 current_method ignorance | MED | KILL-LIST in prompt | partial |
| R1.9 stochasticity | MED | run 3× pre-canary | **TODO** |
| R1.10 checklist drift | LOW | scheduled CI check | TODO (future) |
| R1.11 frontend display | LOW | follow-up PR | accepted |
| R1.12 output path | MED | separate staging file | mitigated by design |
