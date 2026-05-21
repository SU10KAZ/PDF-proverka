# Gating Evaluation — Phase 0 / Phase 1

## Phase 0 (dedup post-process)

### A0_phase0_classdedup__baseline: **pass**
- [PASS] critical_recall_not_worse — missed_crit A0=3 variant=3
- [PASS] matched_gt_not_worse — matched_gt A0=49 variant=49
- [PASS] duplicates_fp_reduced_or_equal — FP+dupes A0=73 variant=73
- [PASS] no_llm_cost — Phase 0 is pure Python post-process (verified by code review)
- [PASS] production_risk_low — Dedup runs after the pipeline; can be guarded by feature flag

### A0_phase0_fuzzydedup__baseline: **pass**
- [PASS] critical_recall_not_worse — missed_crit A0=3 variant=3
- [PASS] matched_gt_not_worse — matched_gt A0=49 variant=49
- [PASS] duplicates_fp_reduced_or_equal — FP+dupes A0=73 variant=73
- [PASS] no_llm_cost — Phase 0 is pure Python post-process (verified by code review)
- [PASS] production_risk_low — Dedup runs after the pipeline; can be guarded by feature flag

### A0_phase0_combined__baseline: **pass**
- [PASS] critical_recall_not_worse — missed_crit A0=3 variant=3
- [PASS] matched_gt_not_worse — matched_gt A0=49 variant=49
- [PASS] duplicates_fp_reduced_or_equal — FP+dupes A0=73 variant=73
- [PASS] no_llm_cost — Phase 0 is pure Python post-process (verified by code review)
- [PASS] production_risk_low — Dedup runs after the pipeline; can be guarded by feature flag

## Phase 1 (A1-v2 candidate) — same case set (fair comparison)

Cases compared head-to-head: **8**
  - ar_01_evacuation, cross_01_eom_ov_loads, eom_01_cable_sizing, kj_01_rebar, multi_01_tz_vs_rd, ov_01_ventilation, ss_01_cabling, vk_01_water_flow

Overall: **see_details**
- [PASS] missed_critical_not_worse — A0=3 A1v2=1
- [PASS] critical_recall_not_worse — A0=0.942 A1v2=0.981
- [FAIL] fp_within_15pct — A0_fp=73 A1v2_fp=119 threshold=83.9
- [FAIL] strict_score_at_least_plus_10pct — A0=50.5 A1v2=35.7 threshold=55.6
- [FAIL] human_review_load_within_20pct — A0_total=127 A1v2_total=173
- [PASS] document_type_routing_used — Verified by test_document_type_routing.py
- [SEE_REPORT] subset_stochasticity_reported — Requires repeated runs (see Stage 5 report)
- [PASS] sonnet_failure_graceful_fallback — Verified by test_fallback_to_a0.py
- [FAIL] avg_cost_increase_le_70pct — A0_avg_sec=158.7 A1v2_avg_sec=315.1 threshold=269.8
- [PASS] no_production_files_modified — Verified by test_no_production_changes.py

## Phase 1 (A1-v2 candidate) — FULL aggregates (uneven case sets)

_Note: A0 aggregates over its full case set; A1-v2 over its own._


Overall: **see_details**
- [PASS] missed_critical_not_worse — A0=3 A1v2=3
- [PASS] critical_recall_not_worse — A0=0.942 A1v2=0.973
- [FAIL] fp_within_15pct — A0_fp=73 A1v2_fp=326 threshold=83.9
- [FAIL] strict_score_at_least_plus_10pct — A0=50.5 A1v2=34.5 threshold=55.6
- [FAIL] human_review_load_within_20pct — A0_total=127 A1v2_total=444
- [PASS] document_type_routing_used — Verified by test_document_type_routing.py
- [SEE_REPORT] subset_stochasticity_reported — Requires repeated runs (see Stage 5 report)
- [PASS] sonnet_failure_graceful_fallback — Verified by test_fallback_to_a0.py
- [PASS] avg_cost_increase_le_70pct — A0_avg_sec=158.7 A1v2_avg_sec=244.1 threshold=269.8
- [PASS] no_production_files_modified — Verified by test_no_production_changes.py

## v1 vs v2 (informational)

| algo | cases | matched | missed_crit | FP | beyond | strict | balanced |
|---|---|---|---|---|---|---|---|
| A1_v1 | 2 | 12 | 0 | 10 | 0 | 72.8 | 82.8 |
| A1_v2 | 24 | 108 | 3 | 326 | 0 | 34.5 | 62.4 |
