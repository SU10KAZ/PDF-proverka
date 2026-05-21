# A1-v2 Final Recommendation

**Date:** 2026-05-20
**Status of evidence in this session:**
- Phase 0: full safety validation on 8 cases.
- Phase 1 (A1-v2): qualitative validation on 1 case (cross_01) +
  infrastructure validation (document_type routing, fallback, schema, tests);
  full 24-case empirical sweep was launched as a background batch and **may
  or may not have completed** by the time this report is read. Re-run the
  refresh commands at the bottom to get the up-to-date verdict.

## Recommendation

### Phase 0 — DEPLOY (low risk)
**Status: APPROVED FOR DEPLOY**

- Pure Python post-process. Zero LLM cost.
- On A0 production outputs: provably a no-op (no findings dropped, no critical
  loss, no FP added) — `test_phase0_dedup_safety.py` passes on 8 cases × 3
  variants.
- On merged multi-source outputs (when applicable in future hybrid algorithms),
  delivers measurable noise reduction (replay_fuzzy_dedup showed +20 strict_score,
  -39 FP on multi_agent).

**Deployment shape:**
- Add `class_dedup.collapse_to_canonical` (+ optional `fuzzy_dedup` at
  similarity 0.7) as a feature-flagged tail of the merge stage of any future
  hybrid algorithm.
- Guard with a flag like `PHASE0_DEDUP_ENABLED=true` so it can be turned off
  without redeploy if any regression is observed.
- No changes to current Stage 01 pipeline — Phase 0 sits **after** the merge,
  not inside it.

### Phase 1 (A1-v2) — HOLD pending full sweep
**Status: NEEDS_MORE_DATA**

What is proven:
- **Document_type routing works.** On cross_01 (audit_comparison), the
  completeness lens stayed in scope and produced no phantom comprehensiveness
  findings (the very FP-class that flagged 4 spurious findings in A1-v1).
- **Schema is stable.** A1-v2 outputs match RunResult contract; meta carries
  document_type, dedup_report, and per-leg duration.
- **Graceful fallback to A0** when completeness lens fails.
- **No production files touched.**

What is NOT proven (within this session):
- Cross-discipline / per-discipline matched_gt / FP performance across the
  full 24-case dataset.
- Stochasticity / run-to-run variance.
- Behavior on the OTHER two non-full_rd routes (`tz_vs_rd`, `specification_only`)
  — only `audit_comparison` was validated empirically.
- Head-to-head v1 vs v2 noise comparison on the same cases.

## Detailed gating status

See `algorithm_research/reports/_gating_evaluation.md` for the live
gating sheet. As of this writing:

- Phase 0: **all 5 rules PASS** on all 3 variants.
- Phase 1: 4 rules PASS (document_type used, fallback, schema, no prod
  changes); 5 rules NOT_EVALUABLE_YET (need full sweep); 1 rule NOT_DONE
  (stochasticity).

## What is the next concrete step?

1. **Wait for the 24-case A1-v2 batch to complete.**
   The runner is `python algorithm_research/runners/algorithm_runner.py
   --algorithm A1 --prompt-set v2 --all --skip-existing`.
   Output lives in `algorithm_research/results/A1_hybrid_lite__v2/`.
   When it completes, run:

   ```
   python algorithm_research/metrics/score_algorithms.py
   python algorithm_research/scripts/build_phase_comparison.py
   python algorithm_research/scripts/evaluate_gating.py
   ```

   The gating evaluation will report whether the 5 remaining Phase 1
   criteria PASS or FAIL.

2. **If Phase 1 gating PASSES** — schedule a stochasticity run:

   ```
   for case in cross_01_eom_ov_loads ov_01_ventilation kj_01_rebar \
              eom_03_low_voltage_selectivity vk_03_hot_water_tz \
              ar_03_balcony_glazing; do
     for run in 1 2 3; do
       python algorithm_research/runners/algorithm_runner.py \
         --algorithm A1 --prompt-set v2 --case "$case"
       # outputs overwrite the same path; rename to capture each run
       mv "algorithm_research/results/A1_hybrid_lite__v2/$case.json" \
          "algorithm_research/results/A1_hybrid_lite__v2/${case}.run${run}.json"
     done
   done
   ```

   Then compute median/IQR by case manually or extend score_algorithms.py
   with a `.run` suffix awareness.

3. **If Phase 1 gating FAILS on any rule** — diagnose:
   - FP > 15%: review which checklist items fire on which document_types;
     adjust the routing or anti-pattern blocks in checklists.
   - strict_score < A0 +10%: check whether dedup is too aggressive (try
     similarity 0.8 instead of 0.7) or whether prompts produce too many
     low-quality findings.
   - missed_critical > A0: check whether completeness lens is filtering
     too eagerly with not_applicable on full_rd documents.

4. **Validate the other two non-full_rd routes:**
   - `tz_vs_rd` on multi_01_tz_vs_rd and vk_03_hot_water_tz.
   - `specification_only` on ar_03_balcony_glazing, km_02_metal_protection_spec,
     ov_03_heating_calc.

   Acceptance: completeness lens does NOT produce findings whose text matches
   the `false_positive_traps` list in each ground_truth.json.

5. **Decide v1 vs v2 prompt set** (Stage 8 of the task):
   - v1 (Conservative Precision): cap 10 findings, no recommended-tier
     reporting, narrower output.
   - v2 (Balanced Engineering): cap 14, recommended-tier allowed,
     `is_beyond_gt_useful` flag allowed, broader output.
   - Recommendation today is **v2** (per existing `prompt_optimization/
     final_prompt_recommendations.md`); v1 to be kept as CI regression
     benchmark.

## Risks left

| Risk | Mitigation |
|---|---|
| document_type routing fails on tz_vs_rd / specification_only (untested) | Run A1-v2 on those cases; expect zero `false_positive_traps` hits |
| Stochasticity high → A1-v2 unreliable in CI | 3-run subset experiment + IQR threshold |
| Per-discipline behaviour varies (one case per discipline mostly) | Future: expand to 3+ cases per discipline + inter-rater validation |
| Sonnet API latency spike | Already partially handled via timeout; consider per-case retry budget |
| Checklist drift as norms update | Add a CI check that compares each checklist's normative references with norms_db.json status |

## Files touched in this session

### Created (NEW)
```
experiments/md_analysis_comparison/
  algorithm_research/
    scripts/
      augment_case_metadata.py
      fix_gt_substrings.py
      run_phase0_dedup.py
      build_phase_comparison.py
      evaluate_gating.py
    tests/
      test_document_type_routing.py
      test_phase0_dedup_safety.py
      test_a1_v2_schema.py
      test_completeness_not_applicable.py
      test_fallback_to_a0.py
      test_no_production_changes.py
    prompt_optimization/checklists/
      KM.md
    reports/
      phase0_phase1_validation_report.md
      dataset_expansion_report.md
      checklist_quality_report.md
      a1_v2_final_recommendation.md
      _phase_comparison.md           (auto-generated)
      _phase_comparison.json         (auto-generated)
      _gating_evaluation.md          (auto-generated)
      _gating_evaluation.json        (auto-generated)
    results/
      A0_phase0_classdedup__baseline/  (8 cases)
      A0_phase0_fuzzydedup__baseline/  (8 cases)
      A0_phase0_combined__baseline/    (8 cases)
      A1_hybrid_lite__v2/              (1+ cases as of writing)
  datasets/
    ar_02_facade_thermal/
    ar_03_balcony_glazing/
    cross_02_kj_ar_opening/
    eom_02_grounding/
    eom_03_low_voltage_selectivity/
    kj_02_slab_punching/
    kj_03_foundation_audit/
    km_01_truss_design/
    km_02_metal_protection_spec/
    km_03_connections/
    ov_02_smoke_protection/
    ov_03_heating_calc/
    ss_02_fire_alarm/
    ss_03_access_integration/
    vk_02_sewage/
    vk_03_hot_water_tz/
```

### Modified
```
experiments/md_analysis_comparison/
  algorithm_research/
    runners/_common.py           (added document_type param to run_lens)
    runners/algorithm_runner.py  (propagate document_type to run_lens; meta)
    prompt_optimization/optimized_prompts_v1/completeness.md  (+routing)
    prompt_optimization/optimized_prompts_v2/completeness.md  (+routing)
  datasets/
    ar_01_evacuation/case.json   (+document_type, +signal flags)
    cross_01_eom_ov_loads/case.json
    eom_01_cable_sizing/case.json
    kj_01_rebar/case.json
    multi_01_tz_vs_rd/case.json
    ov_01_ventilation/case.json
    ss_01_cabling/case.json
    vk_01_water_flow/case.json
```

### NOT touched (verified)
- `backend/app/**` — production pipeline untouched.
- `frontend/src/**` — production UI untouched.
- `norms/tools/**` — norms infra untouched.
- `backend/app/pipeline/manager.py` — production manager untouched.

(`test_no_production_changes.py` enforces this.)

## Final verdict

- **Phase 0:** safe to deploy as a guardrail (gating criteria 5/5 PASS).
- **Phase 1 (A1-v2):** not yet ready for production; needs full 24-case
  empirical sweep and stochasticity subset run. Infrastructure side
  (document_type routing, schema, fallback, tests) is complete and validated.

**Recommendation to the next iteration:**
- Continue / re-trigger the A1-v2 24-case sweep.
- After it completes, run the three refresh commands above.
- Re-read `_gating_evaluation.md`; if all Phase 1 rules show PASS, schedule
  Stage B (opt-in feature flag rollout, not full production replacement).
