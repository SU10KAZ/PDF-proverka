# Regression Strategy — 24-case Golden

**Дата:** 2026-05-20
**Reference:** [FINAL_SUMMARY §1 (Cases tested)](../../algorithm_research/reports/FINAL_SUMMARY.md),
[dataset_expansion_report.md](../../algorithm_research/reports/dataset_expansion_report.md).

---

## Dataset

24 кейса из `experiments/md_analysis_comparison/datasets/`:

| Дисциплина | Кейсы (3 на каждую) |
|---|---|
| AR | ar_01_evacuation, ar_02_facade_thermal, ar_03_balcony_glazing |
| EOM | eom_01_cable_sizing, eom_02_grounding, eom_03_low_voltage_selectivity |
| KJ | kj_01_rebar, kj_02_slab_punching, kj_03_foundation_audit |
| KM | km_01_truss_design, km_02_metal_protection_spec, km_03_connections |
| OV | ov_01_ventilation, ov_02_smoke_protection, ov_03_heating_calc |
| VK | vk_01_water_flow, vk_02_sewage, vk_03_hot_water_tz |
| SS | ss_01_cabling, ss_02_fire_alarm, ss_03_access_integration |
| MULTI/CROSS | cross_01, cross_02, multi_01 |

**Document type распределение:** full_rd=17, audit_comparison=3, tz_vs_rd=2,
specification_only=3.

---

## Baseline snapshots

Каждый кейс получает `_baseline_A0.json` snapshot — текущий production-output
(pre-Phase-0). Эти snapshot'ы являются ground truth для regression.

**Создание baselines:**
```bash
# Запустить current production-pipeline на 24 кейсах через тест-harness:
python tests/regression/build_baselines.py --output tests/regression/_baselines/
# Каждый файл: tests/regression/_baselines/<case_id>_baseline_A0.json
```

Baselines **версионируются в git** под `tests/regression/_baselines/`.
Обновление baselines требует explicit PR с обоснованием.

---

## Сравнение

Для каждого кейса вычисляем metric diff между `current_run` и `_baseline_A0`:

```python
def evaluate_diff(baseline: dict, current: dict, gt: dict) -> dict:
    baseline_metrics = compute_metrics(baseline, gt)
    current_metrics = compute_metrics(current, gt)
    return {
        "matched_gt_delta": current_metrics["matched_gt"] - baseline_metrics["matched_gt"],
        "missed_critical_delta": current_metrics["missed_critical"] - baseline_metrics["missed_critical"],
        "fp_delta": current_metrics["fp"] - baseline_metrics["fp"],
        "strict_score_delta": current_metrics["strict_score"] - baseline_metrics["strict_score"],
    }
```

(Re-use `experiments/.../algorithm_research/metrics/compare_results.py` или
зеркалить его в `tests/regression/`.)

---

## Tolerance bands

| Metric | Phase 0 tolerance | Phase 1 tolerance (opt-in doc_type) | Phase 1 tolerance (full_rd) |
|---|---|---|---|
| matched_gt_delta | ≥ 0 | ≥ 0 | ≥ 0 |
| missed_critical_delta | ≤ 0 (must improve or equal) | ≤ 0 | ≤ 0 |
| fp_delta | within ±15% of A0 fp | within ±15% of A0 fp (можно +25% если is_beyond_gt_useful покрытие выше) | ±0 (route disabled by default) |
| strict_score_delta | within ±5% of A0 | within ±10% of A0 | ±0 |

**Rationale:**
- Phase 0 — no-op на A0 baseline → tolerance очень узкая.
- Phase 1 на opt-in doc_types — допускаем рост FP (это beyond_gt_useful) при
  сохранении critical recall.
- Phase 1 на full_rd — route disabled by default; должно быть как A0.

---

## Failure modes & actions

| Failure | Action |
|---|---|
| missed_critical_delta > 0 на ≥ 1 кейсе | **BLOCK MERGE.** Фикс обязателен. |
| matched_gt_delta < 0 на ≥ 2 кейсах | **BLOCK MERGE.** Investigate prompt regression. |
| fp_delta > tolerance | Investigate; либо tighten dedup threshold, либо tune prompt cap. Можно поднять tolerance с explicit PR-обоснованием. |
| strict_score_delta < -tolerance | Same as fp investigate. |

Если tolerance расширяется — это PR-комментарий с явным engineering rationale
+ approval от 2 senior reviewers.

---

## CI integration

```yaml
# .github/workflows/regression.yml
name: regression-stage01
on:
  pull_request:
    paths:
      - 'backend/app/pipeline/stages/text_analysis/**'
      - 'backend/app/services/findings/**'
      - 'prompts/pipeline/ru/text_analysis_task.md'
      - 'prompts/pipeline/ru/completeness_lens_task.md'
      - 'backend/app/data/discipline_checklists/**'
  schedule:
    - cron: '0 2 * * *'  # nightly 2:00 UTC

jobs:
  regression:
    runs-on: self-hosted  # need Claude CLI subscription
    steps:
      - checkout
      - run: python tests/regression/run_24_case_golden.py
      - run: python tests/regression/compare_to_baselines.py --tolerance-config tests/regression/tolerance.yaml
      - if: failure()
        run: cat tests/regression/_failed_cases.md  # human-readable report
```

Pre-merge required check; nightly catches drift independent of PRs.

---

## Cost

24 cases × Phase 1 (2 LLM legs each) = 48 LLM calls per regression run.
~30-40 минут wall-clock on self-hosted runner.

Nightly cost: ~50 calls × 30 days = 1500 calls/month. В budget Claude Code
subscription comfortable.

---

## Special handling

### "is_beyond_gt_useful" соединения
A1-v2 produces findings outside GT scope (38% beyond_gt_useful per
[a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md)).
Эти НЕ должны провалить regression:

```python
def filter_for_regression(findings: list) -> list:
    """Beyond-GT findings ignored для matched_gt; они считаются как 'extra OK'."""
    return [f for f in findings if not f.get("is_beyond_gt_useful")]
```

Применяется только для `matched_gt` calculation. FP остаётся inclusive
(beyond_gt_useful всё же считается в "human review load").

### Compare_results matching limitations
[FINAL_SUMMARY §4.4 (4)](../../algorithm_research/reports/FINAL_SUMMARY.md):
`compare_results.evaluate_case` matches GT by exact substring. Phase 1 prompts
rephrase abstractly → fewer substring matches → falsely shows missed_critical.

**Mitigation:** semantic / fuzzy GT match. Это **tooling task**, не блокер для
Phase 1. До тех пор regression может show false missed_critical on rephrased
findings — caveat в reviewer notes.

---

## Updating baselines

Baselines обновляются при:
1. Sanctioned prompt iteration (например, full_rd remediation pass).
2. New GT discovered (ground truth refinement).
3. Tolerance widening accepted by 2 reviewers.

**Не обновляются** при:
1. Regression "by accident" — пишем фикс, не обновление baseline.
2. Disagreement — open discussion, не сдвиг.

Baseline-update PR должен иметь:
- Diff baselines per case.
- Explicit rationale per case.
- Reviewer sign-off (2 senior).
