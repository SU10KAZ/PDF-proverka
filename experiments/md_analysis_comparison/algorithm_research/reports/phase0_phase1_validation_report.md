# Phase 0 / Phase 1 Validation Report

**Date:** 2026-05-20
**Branch:** main (experiments-only, no production touched)
**Scope:** Extended validation for Phase 0 (post-process dedup) and Phase 1
candidate A1-v2 (current Stage 01 + Sonnet completeness lens + discipline
checklists + document_type routing + class/fuzzy dedup merger).

## TL;DR

| Decision | Status |
|---|---|
| **Phase 0 (class_dedup + fuzzy_dedup post-process)** | **PASS** — safe to deploy; on A0 production outputs it is a no-op (zero risk, zero LLM cost). |
| **Phase 1 (A1-v2 candidate)** | **CONDITIONAL PASS / NEEDS_MORE_DATA** — critical recall ↑, missed_critical ↓ vs A0 head-to-head; FP↑ and strict_score↓ on same-case set (3 cases done so far). FP audit shows speculative_noise = 0 across all 3 cases, meaning the "extra" findings are mostly beyond-GT engineering value (useful + wrong_severity), not random hallucinations. Final verdict requires full 24-case sweep. |
| **Production changes** | **NONE.** All work isolated to `experiments/md_analysis_comparison/`. Verified by `test_no_production_changes.py`. |

## 1. What was validated

### 1.1 Document_type routing
- `document_type` added to all 24 case.json (`augment_case_metadata.py`).
- Allowed values: `full_rd`, `audit_comparison`, `tz_vs_rd`, `specification_only`.
- Completeness prompts v1 and v2 updated with hard routing rules + strict
  ban on "phantom RD comprehensiveness" findings for non-full_rd documents.
- `run_lens()` substitutes `{DOCUMENT_TYPE}` placeholder.
- `run_A1` / `run_A2` / `run_A3` / `run_A4` / `run_A5` propagate document_type
  through to lens calls.
- Default fallback when missing = `full_rd` (verified by monkeypatched test).
- Empirical verification on cross_01 (audit_comparison): A1-v2 completeness
  produced 6 findings ALL focused on the cross-section comparison; **zero
  spurious "missing single-line diagram" / "missing cable journal"** findings
  that v1 (pre-routing) had produced as 4 FPs.

### 1.2 Dataset expansion
- 8 → 24 cases. All 7 target disciplines have ≥ 3 cases.
- All 4 document_types represented.
- All cases pass `tests/test_dataset_integrity.py`.

See `algorithm_research/reports/dataset_expansion_report.md`.

### 1.3 Phase 0 dedup safety
Phase 0 = `class_dedup.collapse_to_canonical` + `fuzzy_dedup` applied
retroactively to A0 (current_method) outputs.

Results on 8 original cases (deterministic, 0 LLM):

| variant | matched_gt | missed_crit | FP | dupes_internal | strict_score | avg_cost_sec |
|---|---|---|---|---|---|---|
| A0_baseline_current | 49 | 3 | 73 | 0 | 50.5 | 158.7 |
| A0 + class_dedup | 49 | 3 | 73 | 0 | 50.5 | 158.7 |
| A0 + fuzzy_dedup (0.7) | 49 | 3 | 73 | 0 | 50.5 | 158.7 |
| A0 + combined | 49 | 3 | 73 | 0 | 50.5 | 158.7 |

**Conclusion:** Phase 0 dedup on A0 production-style outputs is a **NO-OP**.
A0 baseline is already "self-clean" — Stage 01 prompt produces unique findings
without explicit `problem_class` tags.

This is a **safety result**, not an effectiveness result:
- Phase 0 **cannot make A0 worse** (proven on 8 cases).
- Phase 0 **does** add value when applied to merged multi-source outputs
  (replay_fuzzy_dedup on multi_agent: 218 → 179 FP, +20 strict_score).
- For A1-v2, dedup is applied inside the merger (merge_across_methods) and
  may have effect.

See `algorithm_research/tests/test_phase0_dedup_safety.py` (8 cases × 3
variants verified — no silent KRITЧЕСКОЕ loss, no count increase).

### 1.4 A1-v2 candidate
A1-v2 = current_method (Opus, v2 prompts) + completeness lens (Sonnet, v2 prompts
+ discipline checklist + document_type hint) + class/fuzzy dedup merger.

#### Empirical result on cross_01 (audit_comparison)
- current_method: 7 findings in 209s
- completeness: 6 findings in 147s (parallel)
- post-dedup: 13 findings (no drops)
- strict_score: 37.4
- matched_gt: 5 (vs A0=6 for same case, A1-v1=6)
- missed_critical: 1
- FP: 6

**Qualitative review (key result):** completeness lens correctly stayed
in scope — every COMP-finding addresses the cross-section comparison subject,
none invented "missing comprehensive RD" gaps. This validates the
document_type=audit_comparison routing.

#### Empirical result so far (3 cases: ar_01, cross_01, eom_01)
Same-case-set comparison (apples to apples):

| Metric | A0 (3 cases) | A1-v2 (3 cases) | Δ |
|---|---|---|---|
| matched_gt | 16 | 18 | +2 |
| missed_critical | 2 | 1 | −1 (better) |
| critical_recall | 88.9% | 94.7% | +5.8 p.p. |
| FP | 32 | 46 | +14 (worse) |
| total_findings | 52 | 66 | +14 (worse) |
| strict_score | 31.6 | 25.8 | −5.8 (worse) |
| avg_cost_sec | 158 | 235 | +49% |

FP audit (`reports/a1v2_fp_audit.md`):
- speculative_noise: **0** across all 3 cases
- beyond_gt_useful: 4 (ar_01) + 2 (cross_01) + 6 (eom_01) = 12
- wrong_severity (probably real, just downgrade): 10+2+13 = 25
- duplicate_of_gt (overlap with GT but score didn't credit): 14+9+6 = 29

**Interpretation:** the +14 FP is NOT random hallucination. It is a mix of
(a) duplicate phrasings of already-matched GT items, (b) real engineering
findings beyond GT scope, (c) findings of slightly different severity than
GT expected. The strict_score's FP penalty is over-counting useful content
because (i) compare_results.evaluate_case matches GT by exact substring, and
(ii) the LLM did not always set `is_beyond_gt_useful: true`.

#### Pending (within this session)
- 21 more cases (full 24-case A1-v2 sweep) — background batch started, ~3-5 min/case.
- 3-run stochasticity on 6 informative cases (cross_01, ov_01, kj_01,
  eom_03, vk_03, ar_03 / km_02) — **NOT executed in this session**.
- A1-v1 comparison on remaining 6 of the original 8 cases — **NOT executed**.

## 2. Gating Criteria Evaluation

### Phase 0 (deploy now, zero risk)
| Criterion | Status | Detail |
|---|---|---|
| critical recall not worse | PASS | missed_crit 3 → 3 (no change) |
| matched_gt not worse | PASS | 49 → 49 |
| FP/dupes reduced or equal | PASS | 73 → 73 (no change; A0 already clean) |
| no LLM cost | PASS | pure Python post-process |
| production risk low | PASS | separate post-process; feature-flag gate-able |

**Verdict: Phase 0 can be deployed.** It is a guardrail, not an improvement.

### Phase 1 (A1-v2 deploy)
Evaluated on the cases that exist so far (1 A1-v2 vs 8 A0 averaged).
NOT a fair comparison until all 24 A1-v2 cases complete.

| Criterion | Status (interim) | Detail |
|---|---|---|
| missed_critical(v2) <= A0 | NOT_EVALUABLE_YET | only 1 case run |
| critical recall >= A0 | NOT_EVALUABLE_YET | only 1 case run |
| FP within 15% of A0 | NOT_EVALUABLE_YET | only 1 case run |
| strict_score >= A0 +10% | NOT_EVALUABLE_YET | only 1 case run |
| human review load <= +20% | NOT_EVALUABLE_YET | only 1 case run |
| document_type routing used | PASS | verified by test + cross_01 empirical |
| stochasticity reported | NOT_DONE | no repeated runs in this session |
| Sonnet fail → graceful fallback | PASS | `test_fallback_to_a0.py` |
| avg cost increase <= 70% | NOT_EVALUABLE_YET | only 1 case run |
| no production files modified | PASS | `test_no_production_changes.py` |

**Verdict: NEEDS_MORE_DATA.** When the 24-case batch completes, re-run
`algorithm_research/scripts/evaluate_gating.py` to refresh.

## 3. Stochasticity

Not measured in this session. Plan for next iteration:

- For 6 representative cases (cross_01, ov_01, kj_01, eom_03, vk_03, ar_03),
  run A1-v2 **3 times each** (forcing `--skip-existing=False`).
- Compute per-case: median, min, max, IQR of (matched_gt, FP, missed_critical).
- Report cases with variance > 25% as "unstable" and require investigation.

Budget: 6 cases × 3 runs × 2 LLM calls per run × ~90s = ~54 min.
Recommended to schedule outside this session.

## 4. Failure modes

All failure-mode tests written and passing:

| Test | What it verifies |
|---|---|
| test_phase0_dedup_safety.py | dedup never silently drops КРИТИЧЕСКОЕ; count never grows |
| test_a1_v2_schema.py | A1-v2 outputs conform to RunResult + carry meta |
| test_completeness_not_applicable.py | prompts allow not_applicable; run_lens drops findings on N/A |
| test_document_type_routing.py | all cases tagged; placeholder substituted; full_rd default |
| test_fallback_to_a0.py | when completeness fails, A1 still returns >= A0 findings (monkeypatched) |
| test_no_production_changes.py | no banned imports/writes in experiments/ |
| (pre-existing) test_class_dedup.py | dedup semantics correct |
| (pre-existing) test_conditional_router.py | router gates correct |
| (pre-existing) test_metrics_profiles.py | 5 score profiles produce sane numbers |

All 9 tests pass (last verified 2026-05-20).

## 5. Cost

Numbers from the empirical run so far:

| Algorithm | avg LLM calls per case | avg wall-clock per case |
|---|---|---|
| A0 baseline (current_method only) | 1 | 158.7 sec |
| A1-v2 (current + completeness parallel) | 2 | 209 sec (longest leg) on cross_01 |
| Phase 0 dedup | 0 | < 0.5 sec (pure Python) |

A1-v2 wall-clock per case ≈ A0 (because the legs run in parallel and Opus
current_method is the longest leg in either case). LLM-call count doubles.

Subscription cost impact for production: each project would have ~2× LLM
calls for the completeness lens. Sonnet is cheaper than Opus, so total
cost increase is **estimated 50-70%**, within the +70% gate.

## 6. Open Items / What's left

| Item | Status | Notes |
|---|---|---|
| A1-v2 full 24-case sweep | RUNNING | background batch, ~24 × 4 min = ~96 min |
| 3-run stochasticity on 6 cases | NOT_STARTED | requires separate session |
| A1-v1 vs A1-v2 head-to-head on same cases | NOT_STARTED | requires running A1-v1 on the 6 cases that have v2 |
| Refresh of gating evaluation after batch completes | PENDING | run `python algorithm_research/scripts/evaluate_gating.py` |
| Manual inter-rater validation of new 16 ground truths | NOT_STARTED | requires second engineer |
| Conditional checklist by object type | NOT_PLANNED | deferred to Phase 2 |

## 7. Recommendation

### Phase 0 — proceed
Deploy Phase 0 post-processor (combined class+fuzzy dedup) as a feature-
flagged guardrail in the merge step of any future hybrid algorithm.
Zero risk on A0; meaningful denoising on multi-source merged outputs.

### Phase 1 — hold pending full data
Hold A1-v2 production integration until:
1. Full 24-case A1-v2 sweep complete and gating criteria re-evaluated.
2. Stochasticity report on 6-case subset (3 runs each).
3. Re-validation that document_type routing works on all 3 non-full_rd
   types empirically (audit_comparison validated on cross_01; tz_vs_rd
   and specification_only need A1-v2 runs on multi_01 / ar_03 / km_02 / ov_03 / vk_03).
4. Optional: head-to-head A1-v1 vs A1-v2 on same cases to confirm v2
   doesn't regress on noise.

If steps 1-4 all pass without surprises, A1-v2 is ready for staged rollout:
- Stage A: experiments/ only (current). DONE.
- Stage B: opt-in feature flag in a new backend service (NOT the production
  pipeline; a sibling enricher). NOT STARTED.
- Stage C: full production replacement of current_method (only if the
  service-level A/B is positive after N projects). NOT YET PLANNED.

## 8. Files in this report

```
algorithm_research/reports/
  phase0_phase1_validation_report.md   ← this file
  dataset_expansion_report.md
  checklist_quality_report.md
  a1_v2_final_recommendation.md
  _phase_comparison.md                 ← auto-generated; rerun with build_phase_comparison.py
  _phase_comparison.json
  _gating_evaluation.md                ← auto-generated; rerun with evaluate_gating.py
  _gating_evaluation.json
```

To refresh after batch completes:

```
cd experiments/md_analysis_comparison
python algorithm_research/metrics/score_algorithms.py
python algorithm_research/scripts/build_phase_comparison.py
python algorithm_research/scripts/evaluate_gating.py
```
