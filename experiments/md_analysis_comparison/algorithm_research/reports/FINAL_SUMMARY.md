# Final Summary — Phase 0 / Phase 1 Extended Validation

**Date:** 2026-05-20
**Session goal:** Extended testing of Phase 0 (dedup) and Phase 1 candidate
(A1-v2 = current Stage 01 + Sonnet completeness lens + checklists +
`document_type` routing + class/fuzzy dedup) on an expanded dataset.

**Production status:** **NOTHING changed in production.** All work isolated
to `experiments/md_analysis_comparison/`. Enforced by `test_no_production_changes.py`.

---

## 1. Cases tested

- **24 cases total** (8 original + 16 new synthetic-but-realistic).
- 7 disciplines covered with ≥ 3 cases each: AR, EOM (4 incl. cross_01),
  KJ, KM, OV, VK, SS.
- 4 document_types covered: full_rd (17), audit_comparison (3),
  tz_vs_rd (2), specification_only (3).
- Every case has FP-trap markers and signal flags for diagnostic use.

See `dataset_expansion_report.md` for details.

## 2. Runs executed

- **A0 baseline (current_method, Opus, baseline prompt):** 8 cases (pre-existing).
- **A0 + Phase 0 dedup** (class_dedup / fuzzy_dedup / combined): 8 cases (new,
  pure Python, no LLM).
- **A1-v2 (current + Sonnet completeness + checklists + doc_type + dedup):**
  in progress at time of writing. **Coverage will increase as the background
  batch completes.** Refresh with `bash algorithm_research/scripts/refresh_reports.sh`.
- **A1-v1:** 2 cases (pre-existing, cross_01 + ov_01).
- **3-run stochasticity on 6 cases:** **NOT DONE** in this session. Deferred.

## 3. Phase 0 verdict — can deploy?

**YES.** All 5 gating rules PASS on all 3 variants (class, fuzzy, combined):

| Rule | A0 → A0+Phase0 |
|---|---|
| critical_recall_not_worse | PASS (missed_crit 3 → 3) |
| matched_gt_not_worse | PASS (49 → 49) |
| FP/duplicates reduced or equal | PASS (73 → 73) |
| no LLM cost | PASS (pure Python) |
| production risk low | PASS (separate post-process, feature-flag-gate-able) |

**On A0 production outputs Phase 0 is a no-op** (A0 baseline is already
self-clean — Stage 01 prompt produces unique findings).
Value only manifests when applied to merged multi-source outputs (e.g. the
merge step inside A1-v2, A4, multi_agent). On legacy multi_agent outputs,
fuzzy_dedup reduces FP by 18% (+20 strict_score).

**Recommendation: deploy Phase 0 as a feature-flagged guardrail.** It cannot
make A0 worse and may help any future merged algorithm.

## 4. Phase 1 (A1-v2) verdict — can deploy?

**HOLD — needs targeted remediation. A1-v2 is _better_ for some document
types (audit_comparison) and _worse_ for others (full_rd) on the strict
scoring formula. Refined recommendation per document_type below.**

### Updated finding (16 cases, refresh @ batch midpoint)
Per-document_type aggregates (from `_doc_type_analysis.md`):

| doc_type | A0 cases | A0 avg_strict | A1-v2 cases | A1-v2 avg_strict | Verdict |
|---|---|---|---|---|---|
| full_rd | 6 | **49.8** | 11 | **23.1** | A1-v2 worse (+114 FP, completeness lens too aggressive) |
| audit_comparison | 1 | 25.1 | 3 | **51.4** | A1-v2 **better** (+26 strict; routing helps) |
| tz_vs_rd | 1 | **80.0** | 1 | 36.0 | A1-v2 worse on 1 case (+11 FP; lens adds duplicate findings) |
| specification_only | 0 | n/a | 1 | 22.0 | no A0 baseline; A1-v2 caught 3/3 real GT |

Critical recall (all 16 v2 cases): 13/16 cases caught ALL critical;
3 cases missed 1 critical each. A0 (8 cases): 5/8 caught all, 3/8 missed
at least 1 → **A1-v2 per-case missed-critical RATE is HALVED**.

### Refined recommendation
- Phase 1 NOT ready as a blanket A0 replacement.
- Phase 1 _is_ ready for **opt-in on audit_comparison documents** —
  routing works, FP reduction matters, score is better.
- For full_rd: completeness cap should drop (current 14 → ~6) or run
  completeness only on cases with low current_method finding count.
- For tz_vs_rd: needs more cases (vk_03 still pending in batch).

### What works (qualitative)
- **document_type routing works correctly on audit_comparison** —
  cross_01 (5 cases) and cross_02 (5 cases) completeness output is fully in
  scope. NO phantom RD comprehensiveness findings emitted by the Sonnet lens.
- **document_type routing works on specification_only** — ar_03 (verified):
  Sonnet completeness focused on parameter-level findings (Б4/Б5 missing
  profile/глазурование). Only Opus current_method (which has no
  document_type awareness) produced 1/15 phantom finding.
- **Graceful fallback** to A0 when Sonnet lens fails (verified by
  `test_fallback_to_a0.py`).
- **Schema is stable.** All A1-v2 outputs carry meta.document_type,
  meta.dedup_report, per-leg duration. `test_a1_v2_schema.py` passes.
- **Speculative noise is 0** across the 6 cases audited (heuristic from
  `audit_a1v2_fp.py`). The extra findings are NOT random hallucinations;
  they are mostly (a) duplicate phrasings of GT, (b) real beyond-GT findings,
  (c) wrong-severity variants.

### What fails the gates (quantitative)

Same-case-set head-to-head (3 cases where both A0 and A1-v2 exist):

| Gate | A0 | A1-v2 | Verdict |
|---|---|---|---|
| missed_critical | 2 | 1 | PASS (A1-v2 better) |
| critical_recall | 88.9% | 94.7% | PASS |
| FP within 15% of A0 | 32 | 46 | **FAIL** (+44%) |
| strict_score ≥ A0 +10% | 31.6 | 25.8 | **FAIL** (-18%) |
| human review load ≤ A0 +20% | 52 findings | 66 findings | **FAIL** (+27%) |
| document_type routing used | — | — | PASS |
| Sonnet failure → graceful | — | — | PASS |
| avg cost increase ≤ 70% | 157s | 235s | PASS (+49%) |
| no production files modified | — | — | PASS |
| subset stochasticity | — | — | NOT_DONE |

**Net: 6 PASS, 3 FAIL, 1 NOT_DONE.** The 3 failures are all
"too many findings, score-formula-overweighted-FP" failures. Critical
findings get BETTER, not worse.

### Root cause analysis of the FAILs
The 3 failing gates are all driven by A1-v2 producing more findings than A0.
Of the extra findings (per `a1v2_fp_audit.md`):

- 0 are speculative noise.
- ~38% are beyond_gt_useful (real engineering value not in GT).
- ~38% are wrong_severity (could be downgraded but not removed).
- ~24% are duplicate_of_gt (matching GT semantically but not by substring).

This is a **scoring artifact + label-mismatch problem**, not a real noise
regression:
- `compare_results.evaluate_case` matches GT by exact substring; semantically
  equivalent findings may not credit toward matched_gt.
- LLM does not always set `is_beyond_gt_useful: true` (lost on
  `balanced_engineering` profile).
- Ground truths are themselves narrower than what a thorough engineering
  audit would produce.

### Recommendations for remediation before production
1. **Tune v2 prompt** to encourage `is_beyond_gt_useful` tagging on findings
   that go beyond the obvious problem space.
2. **Lower completeness cap** from 14 to ~8 findings for full_rd cases.
3. **Add document_type awareness to current_method prompt too** (Opus produced
   1 phantom finding on ar_03; this leaks through current_method).
4. **Improve `compare_results` matching** with semantic / fuzzy GT match
   (would credit "3 receivers not in table" against GT-02 "тепловые завесы
   не учтены"). This is a tooling fix, not a model change.

## 5. Prompts v1 vs v2

Head-to-head on cross_01 (only case with both):

| Metric | v1 | v2 |
|---|---|---|
| matched_gt | 6 | 5 |
| missed_critical | 0 | 1 |
| FP | 7 | 6 |
| total findings | 14 | 13 |
| strict_score | 57.7 | 37.4 |

v1 wins by strict_score on this case. But v1 had 4 spurious "phantom RD
comprehensiveness" findings from its completeness lens (Кабельный журнал
отсутствует / Спецификация не представлена / Тип системы заземления /
Координация с АПС). v2's routing eliminates those, but v2 also rephrased
critical findings more abstractly ("3 receivers" instead of "П2, В2, тепловые
завесы"), causing missed substring match.

**Conclusion on v1 vs v2:**
- v1 has the artifact-driven score advantage on this 1 case.
- v2 is semantically more accurate but rephrases more abstractly → fewer
  substring-based GT matches.
- Need 5+ cases with both to draw a firm conclusion.
- Current recommendation (per pre-existing
  `prompt_optimization/final_prompt_recommendations.md`): use v2 in
  production for the engineering-value benefits, keep v1 as CI regression
  benchmark.

## 6. Checklists requiring tweaks

See `checklist_quality_report.md`. Key items:

- **KM checklist created** (was missing) — mirrors EOM structure.
- All checklists have anti-pattern blocks suppressing common FP (vendor names,
  Э42/Э50A welding rods).
- `document_type` routing in completeness prompt makes M-tier items
  effectively conditional — no checklist edit needed at this time.
- Future Phase 2: conditional pieces by object type (отдельно стоящий vs МКД vs
  офисный центр) for items like молниезащита, антикоррозионная защита.

## 7. Risks left

| Risk | Mitigation |
|---|---|
| FP penalty in scoring formula over-counts useful beyond-GT findings | Improve compare_results or prompt v2 to set `is_beyond_gt_useful` |
| current_method (Opus) doesn't know about document_type | Future iteration: add to current_method prompt |
| Stochasticity unknown | Run 3× on 6-case subset in next session |
| Synthetic ground truths may miss valid findings | Inter-rater review with engineering team |
| Discipline-specific behaviour variance (1 case per discipline averaged) | Continue expanding dataset to 5+ cases per discipline |
| Cost growth ~50% (Sonnet calls) | Acceptable per gating (< 70% threshold); can be reduced by skipping completeness when current_method has > N findings already |

## 8. Production-facing files (possibly touched on future iterations)

If/when Phase 1 is approved for production:
- `backend/app/services/findings/findings_service.py` — would add a
  completeness-merger step.
- `backend/app/pipeline/stages/findings_merge.py` — would call the merger.
- `backend/app/pipeline/stages/text_analysis.py` — would optionally call
  the Sonnet completeness lens in parallel with current_method.
- `backend/app/data/discipline_checklists/*.md` — new directory for checklists
  (mirror of `experiments/md_analysis_comparison/algorithm_research/prompt_optimization/checklists/`).

**None of these have been touched in this session.**

## 9. Can we move to the implementation task?

**Phase 0:** YES — separate task can implement Phase 0 dedup as a
feature-flagged post-process in the merge stage. Low risk, no code change
to current Stage 01 or current_method runner.

**Phase 1 (A1-v2):** NO — not yet ready. Required before implementation task:
1. Finish 24-case A1-v2 batch (running in background).
2. Refresh gating with full data.
3. Run 3× stochasticity on 6-case subset.
4. Address the FP-volume gate failure (one of: tune v2 prompt cap,
   add is_beyond_gt_useful tagging, or relax the gate to a more engineering-
   appropriate metric like balanced_engineering).
5. (Optional) Add document_type awareness to current_method prompt.

Once 1-4 are addressed and same-case-set gates all PASS, a separate
implementation task can:
- Add the Sonnet completeness lens as opt-in feature flag.
- Wire document_type tagging into project metadata.
- Add Phase 0 dedup at merge tail.
- Run A/B at staging level before production cutover.

## 10. Files in this session

### Created
```
algorithm_research/scripts/
  augment_case_metadata.py         (added document_type to 8 original cases)
  fix_gt_substrings.py             (substring fixes for new cases)
  run_phase0_dedup.py              (Phase 0 batch runner)
  build_phase_comparison.py        (per-algorithm comparison)
  evaluate_gating.py               (gating criteria check)
  build_per_case_delta.py          (per-case A0 vs A1-v2)
  audit_a1v2_fp.py                 (FP heuristic classifier)
  refresh_reports.sh               (one-button refresh)

algorithm_research/tests/
  test_document_type_routing.py    (document_type plumbing)
  test_phase0_dedup_safety.py      (no critical loss)
  test_a1_v2_schema.py             (output schema)
  test_completeness_not_applicable.py
  test_fallback_to_a0.py           (Sonnet failure → A0 fallback)
  test_no_production_changes.py    (production guardrail)

algorithm_research/prompt_optimization/checklists/
  KM.md                            (new)

algorithm_research/reports/
  FINAL_SUMMARY.md                 (this file)
  phase0_phase1_validation_report.md
  dataset_expansion_report.md
  checklist_quality_report.md
  a1_v2_final_recommendation.md
  a1v2_fp_audit.md                 (auto)
  _phase_comparison.md             (auto)
  _gating_evaluation.md            (auto)
  _per_case_delta.md               (auto)

algorithm_research/results/
  A0_phase0_classdedup__baseline/  (8 cases — no LLM)
  A0_phase0_fuzzydedup__baseline/  (8 cases — no LLM)
  A0_phase0_combined__baseline/    (8 cases — no LLM)
  A1_hybrid_lite__v2/              (cases as completed; refresh to see latest)

datasets/                          (16 new cases × 3 files = 48 new files)
```

### Modified
- `algorithm_research/runners/_common.py` — `run_lens` accepts `document_type`.
- `algorithm_research/runners/algorithm_runner.py` — A1/A2/A3/A4/A5 propagate
  document_type to lens; ALL_CASES discovers dynamically.
- `algorithm_research/prompt_optimization/optimized_prompts_v1/completeness.md` — added routing.
- `algorithm_research/prompt_optimization/optimized_prompts_v2/completeness.md` — added routing + strict ban.
- `datasets/*/case.json` for 8 original cases — added document_type + signal flags.

### NOT touched (verified)
- `backend/app/**`
- `frontend/src/**`
- `norms/tools/**`
- `backend/app/pipeline/manager.py`

## 11. How to continue this work in a future session

```bash
cd experiments/md_analysis_comparison

# If A1-v2 batch is still running:
tail -f /tmp/a1v2_run.log
tail -f /tmp/a1v2_new16.log

# Or kick off again with --skip-existing:
python algorithm_research/runners/algorithm_runner.py --algorithm A1 --prompt-set v2 --all --skip-existing

# When done, refresh all reports:
bash algorithm_research/scripts/refresh_reports.sh

# For stochasticity (recommended next):
for case in cross_01_eom_ov_loads ov_01_ventilation kj_01_rebar \
           eom_03_low_voltage_selectivity vk_03_hot_water_tz \
           ar_03_balcony_glazing; do
  for run in 1 2 3; do
    python algorithm_research/runners/algorithm_runner.py \
      --algorithm A1 --prompt-set v2 --case "$case"
    mv "algorithm_research/results/A1_hybrid_lite__v2/${case}.json" \
       "algorithm_research/results/A1_hybrid_lite__v2/${case}.run${run}.json"
  done
done

# Re-read FINAL_SUMMARY.md, _gating_evaluation.md, _per_case_delta.md to
# see the updated verdict.
```
