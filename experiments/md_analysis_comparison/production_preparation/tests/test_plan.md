# Master Test Plan — Phase 0 / Phase 1

**Дата:** 2026-05-20
**Связанные документы:**
- `regression_strategy.md`, `golden_dataset_strategy.md`, `stochasticity_strategy.md`,
  `production_validation_strategy.md`.
- [FINAL_SUMMARY §10 (Files в session)](../../algorithm_research/reports/FINAL_SUMMARY.md)
  — testing infra уже создана в experiments-стенде.

---

## Уровни тестирования

```
1. Unit tests          — per module
2. Contract tests      — schemas & invariants
3. Integration tests   — single project smoke
4. Regression tests    — 24-case golden
5. Stochasticity tests — 3-run on 6 cases
6. Production shadow   — N canary projects, log-only
7. Production A/B      — same project_id, two versions
```

Каждый уровень должен PASS перед переходом к следующему.

---

## 1. Unit tests

### Phase 0

| Файл | Что тестируется | LOC |
|---|---|---|
| `tests/findings/dedup/test_class_dedup.py` | derive_class_key, cluster_findings, collapse_to_canonical, merge_across_methods, КРИТ-protect | 150 |
| `tests/findings/dedup/test_fuzzy_dedup.py` | fuzzy_collapse threshold behaviour, КРИТ-protect, output count invariant | 120 |

Gating: 100% line coverage на новые dedup модули.

### Phase 1

| Файл | Что тестируется | LOC |
|---|---|---|
| `tests/findings/test_document_type_detector.py` | 4 doc_types, manual override, low confidence fallback | 80 |
| `tests/findings/test_completeness_lens.py` | feature flag gating, doc_type routing, graceful fallback on lens failure | 100 |
| `tests/findings/test_findings_service_merge.py` | _merge_completeness_into_findings: priorities, source_agents, dedup_report | 80 |

Gating: 100% coverage на новые модули + 95% на изменённые функции.

---

## 2. Contract tests

Проверяют что output соответствует expected schema.

| Тест | Описание | Reference |
|---|---|---|
| `test_03_findings_v1_compat.py` | Старые `03_findings.json` (schema_version отсутствует) читаются consumer'ами | migration_plan.md |
| `test_03_findings_v2_schema.py` | Новые outputs с `schema_version=2` валидируются по расширенной схеме | schemas/text_analysis.json |
| `test_dedup_report_shape.py` | `meta.dedup_report` имеет ожидаемые поля (total_in, total_out, clusters, ...) | dedup/class_dedup.py DedupReport |
| `test_completeness_meta_shape.py` | `meta.completeness_applied`, `meta.completeness_status` корректны | completeness_lens.py |

Gating: все contract тесты PASS на 24-case golden outputs.

---

## 3. Integration tests (smoke)

Запуск pipeline end-to-end на одном проекте.

| Тест | Проект | Phase | Verify |
|---|---|---|---|
| `test_smoke_phase0_eom.py` | projects/EOM/<sample> | 0 | dedup runs without exception, meta.dedup_report корректен |
| `test_smoke_phase1_audit_comparison.py` | projects/EOM/<cross-sample> | 1 | doc_type detected as `audit_comparison`, lens called, merge OK |
| `test_smoke_phase1_full_rd.py` | projects/EOM/<full-rd-sample> | 1 | doc_type detected as `full_rd`, lens NOT called (default route), behaviour == A0 |
| `test_smoke_phase1_graceful_fallback.py` | mock'нутый Sonnet failure | 1 | pipeline продолжает, current_method outputs surfaced |

Gating: smoke-тесты pass перед canary deploy.

---

## 4. Regression tests

24-case golden dataset; полная стратегия — `regression_strategy.md`.

Краткое:
- Запуск Phase 0 / Phase 1 на 24 кейсах из `experiments/.../datasets/`.
- Сравнение с baseline snapshots `_baseline_A0.json` (надо создать).
- Tolerance bands:
  - `missed_critical` — must be ≤ A0 (no regression on critical recall).
  - `matched_gt` — within ±0 (no loss).
  - `FP` — within ±15% (Phase 0 dедуплицирующий ОК; Phase 1 на opt-in
    doc_types может быть ±15% от A0).
  - `strict_score` — within ±10% **на opt-in doc_types только** (full_rd
    остаётся как A0).

Gating: regression suite PASS → block merge при FAIL.

CI integration: pre-merge required check + nightly scheduled run.

---

## 5. Stochasticity tests

Полная стратегия — `stochasticity_strategy.md`.

Краткое:
- 6 informative cases (cross_01, ov_01, kj_01, eom_03, vk_03, ar_03/km_02).
- 3 runs per case.
- Metrics: median + IQR.
- Pass: `IQR/median ≤ 0.25` для (matched_gt, FP, missed_critical, strict_score)
  в каждом кейсе.

Gating: PASS перед canary.

Estimated cost: ~6 × 3 × 2 LLM = 36 calls; ~60 минут subscription time
(FINAL_SUMMARY §11).

---

## 6. Production shadow tests

Полная стратегия — `production_validation_strategy.md`.

Краткое:
- На canary deploy `STAGE01_COMPLETENESS_LENS_ENABLED=true` для `audit_comparison`.
- Phase 1 lens **запускается**, но output **не surfaced** в UI (только logs).
- N=10 проектов с `document_type=audit_comparison`.
- Engineer review каждого Sonnet output: correct? FP? missed?
- Структурированный feedback в `experiments/.../canary_feedback.jsonl`.

Pass criteria: ≥ 80% findings одобрены engineer review (correct / beyond_useful).

---

## 7. Production A/B tests

После shadow PASS — canary с visible output.

- Один и тот же `project_id` audit'ится дважды:
  - Version A: pre-Phase-1 (env: `STAGE01_COMPLETENESS_LENS_ENABLED=false`).
  - Version B: Phase 1 (env: `=true`).
- Через version_service obe версии доступны в UI.
- Engineer выбирает свою.

Pass criteria: ≥ 60% engineer'ов выбирают Version B (Phase 1) на
`audit_comparison`.

---

## Ramp-up gating checklist

Каждый этап имеет gate; переход возможен только при всех PASS.

### Gate 0 → Gate 1 (Unit → Contract)
- [ ] Все Phase 0 unit tests PASS (8 кейсов dедупа).
- [ ] Все Phase 1 unit tests PASS (mock'нутые claude calls).
- [ ] Coverage по новым модулям ≥ 95%.

### Gate 1 → Gate 2 (Contract → Integration)
- [ ] schema v1 + v2 contract tests PASS.
- [ ] meta.dedup_report shape валиден.
- [ ] meta.completeness_status shape валиден.

### Gate 2 → Gate 3 (Integration → Regression)
- [ ] Smoke-tests на 4 типах сценариев PASS.
- [ ] Graceful fallback verified.

### Gate 3 → Gate 4 (Regression → Stochasticity)
- [ ] 24-case golden regression PASS.
- [ ] Никаких missed_critical regressions.
- [ ] FP / strict_score в tolerance.

### Gate 4 → Gate 5 (Stochasticity → Canary)
- [ ] 6-case 3-run IQR/median ≤ 0.25.
- [ ] No outlier > 2× IQR.

### Gate 5 → Gate 6 (Canary shadow → Canary visible)
- [ ] 10 проектов shadow с ≥ 80% engineer approval.
- [ ] Zero data corruption / pipeline crashes.

### Gate 6 → Production
- [ ] A/B ≥ 60% engineer preference для Version B.
- [ ] Зафиксированный rollback playbook (см. `rollback_steps.md`).
- [ ] Документация / runbooks обновлены.

---

## Phase-specific test summary

### Phase 0 — minimal
1. Unit (dедуп) PASS
2. Contract (`meta.dedup_report`) PASS
3. Smoke (1 EOM проект) PASS
4. Regression (24-case, no-op expected) PASS

Очень узкий test pass → ready for deploy.

### Phase 1 — extended
Все Phase 0 PLUS:
1. Unit (doc_type, lens) PASS
2. Contract (schema v2) PASS
3. Smoke (4 сценария) PASS
4. Regression (24-case) PASS
5. Stochasticity (6×3) PASS
6. Canary shadow (10 проектов) PASS
7. Canary A/B (≥ 60% pref) PASS

Тяжёлая validation — это и есть HOLD причина из
[FINAL_SUMMARY §4](../../algorithm_research/reports/FINAL_SUMMARY.md).

---

## Test execution в CI

```yaml
# .github/workflows/test.yml (концептуально)
jobs:
  unit:
    runs: pytest tests/findings/ tests/integration/ -m "not slow"
  contract:
    runs: pytest tests/contract/
  regression:
    runs-on: scheduled (nightly) + on-PR-touching-stage01
    runs: python tests/regression/run_24_case_golden.py
  stochasticity:
    runs-on: manual trigger only (cost-sensitive)
    runs: python tests/stochasticity/run_6_case_3x.py
```

Pre-merge required: unit + contract + smoke.
Nightly required: regression.
Manual: stochasticity + canary.

---

## Existing test infrastructure (от experiments stand)

Можно переиспользовать (или mirror'ить в production tests/):

- `experiments/.../algorithm_research/tests/test_phase0_dedup_safety.py` —
  переезжает в `tests/findings/dedup/test_safety.py`.
- `test_document_type_routing.py` — переезжает в test_document_type_detector.py.
- `test_a1_v2_schema.py` — переезжает в `tests/contract/test_v2_schema.py`.
- `test_fallback_to_a0.py` — переезжает в integration tests.
- `test_completeness_not_applicable.py` — переезжает в lens tests.

Тесты `test_no_production_changes.py` устаревают после merge'а (но полезны
до merge'а — они защищают experiments-стенд от случайных production-правок).
