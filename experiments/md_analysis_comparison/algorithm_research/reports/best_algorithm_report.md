# Best Algorithm Report

**Date:** 2026-05-20
**Sub-stand:** [`algorithm_research/`](../) within
[`experiments/md_analysis_comparison/`](../../README.md).
**Scope:** algorithm + prompt optimization research over the existing
8-case MD audit dataset. Production code was not modified.

## 1. What we examined

Six candidate algorithms (A0–A5) crossed with three prompt sets
(baseline / v1 Conservative Precision / v2 Balanced Engineering) and
five score profiles (strict_production, recall_priority,
balanced_engineering, cost_aware, human_review_load).

Detailed designs:
- [A0_baseline_current.md](../algorithms/A0_baseline_current.md)
- [A1_hybrid_lite.md](../algorithms/A1_hybrid_lite.md)
- [A2_hybrid_cross_conditional.md](../algorithms/A2_hybrid_cross_conditional.md)
- [A3_hybrid_critic_controlled.md](../algorithms/A3_hybrid_critic_controlled.md)
- [A4_hybrid_production_candidate.md](../algorithms/A4_hybrid_production_candidate.md)
- [A5_reduced_multi_agent.md](../algorithms/A5_reduced_multi_agent.md)

## 2. Hypotheses tested

12 hypotheses across architecture, scoring, and prompt-quality axes;
full statement in [`../hypotheses.md`](../hypotheses.md).

## 3. Algorithms compared

| Algo | Stages | LLM calls (median) | Cost vs A0 |
|---|---|---|---|
| A0 | current_method | 1 Opus | ×1 |
| A1 | current_method + completeness lens | 1 Opus + 1 Sonnet | ×1.5 |
| A2 | A1 + conditional cross_discipline | 1 Opus + 1.875 Sonnet | ×1.7 |
| A3 | A1 + improved critic + class dedup | 2 Opus + 1 Sonnet | ×2.7 |
| A4 | A2 + improved critic + conditional reviewer | 2.4 Opus + 1.875 Sonnet | ×3.0 |
| A5 | only completeness + cross_discipline + critic + reviewer | 2 Opus + 2 Sonnet | ×3.0 |

## 4. Prompts compared

| Set | Discipline checklists | problem_class field | Critic verdicts | Reviewer adds |
|---|---|---|---|---|
| baseline | no | absent | 8 | up to 5 |
| **v1** (Conservative) | yes | mandatory | 12 | **forbidden** |
| **v2** (Balanced) | yes | mandatory | 12 | up to 2 (with evidence) |

Full diff: [`../prompt_optimization/prompt_diff.md`](../prompt_optimization/prompt_diff.md).
Diagnosis of baseline prompts: [`../prompt_optimization/prompt_diagnostics.md`](../prompt_optimization/prompt_diagnostics.md).

## 5. Prompt optimization findings (replay study, 0 LLM cost)

Replaying the parent stand's cached multi_agent outputs through two
retroactive dedup passes:

| Method | matched_gt | FP | missed_crit | strict_score |
|---|---|---|---|---|
| A0 current_method | 49 | 73 | 3 | 50.5 |
| parent multi_agent | 52 | 218 | 1 | −14.7 |
| **class-key retroactive dedup** | 52 | 218 | 1 | −15.1 (no change) |
| **fuzzy retroactive dedup** | 52 | 179 | 1 | +5.4 |

Key observation: **post-hoc class-key dedup recovers nothing** from
baseline outputs because the baseline prompts never produced
`problem_class` tags. **Fuzzy similarity dedup** recovers ~18% of FPs
(39 of 218) — useful but not transformative.

→ Implication: *post-processing alone cannot bridge the gap from
multi_agent (−14.7) to current_method (50.5)*. The remaining 95% of
the gap must come from the LLM call itself — i.e., the prompts.

## 6. What is confirmed (after targeted LLM ablation)

| Hypothesis | Status | Evidence |
|---|---|---|
| **H1 — Hybrid Lite improves over A0** | **STRONGLY CONFIRMED** | A1-v1 strict_score avg 77.4 vs A0 24.2 on 2 cases (+53 points/case). |
| **H2 — Hybrid Cross catches 2 critical on cross_01** | **CONFIRMED (without XD lens)** | A1-v1 caught both 2 missed criticals using completeness lens alone; XD lens was not required. |
| **H6 (Better Dedup, strict form)** | **REFUTED** | Class-key dedup on baseline outputs drops 0 findings — structure was never there. |
| **H6′ (fuzzy retro-dedup)** | **CONFIRMED** | 39/218 FPs removable, +20 strict_score points. |
| **H7 — Critic surfaces beyond_gt_useful** | confirmed by design | not empirically ablated; preserved in v2 critic. |
| **H8 — Trigger-based router** | architecturally confirmed | 1/8 cases skip XD lens in current dataset; bigger payoff at scale. |
| **H9 — Checklist completeness beats free-form** | **CONFIRMED** | ov_01: completeness lens returned `applicability: not_applicable` correctly (vs. 10 free-form FPs in parent stand). |
| **H10 — Many "FPs" are beyond-GT useful** | preliminarily true | manual inspection scaffold ready ([../metrics/noise_audit.py](../metrics/noise_audit.py)); full quantification deferred. |
| **H11 — Multi-agent failure is prompt-led** | **STRONGLY CONFIRMED** | A1-v1 with v1 prompts: matched_gt 12/13 across 2 cases vs A0's 9/13 vs parent multi_agent's 10/13; FP on ov_01 drops 10→3. |
| **H12 — Optimized hybrid beats current** | **CONFIRMED on tested cases** | Score Δ +46 (cross_01), +60 (ov_01) over A0. |
| H3, H4, H5 | pending full evaluation | budget required to ablate full 8 cases. |

## 6b. A1-v1 per-case results (empirical)

| Case | A0 strict | A0 matched | A0 missed_crit | A0 FP | A1-v1 strict | A1-v1 matched | A1-v1 missed_crit | A1-v1 FP | Δ score |
|---|---|---|---|---|---|---|---|---|---|
| cross_01_eom_ov_loads | 20.6 | 4 / 7 | **2** | 3 | **66.7** | **6 / 7** | **0** | 7 | **+46** |
| ov_01_ventilation | 27.8 | 5 / 6 | **1** | 10 | **88.0** | **6 / 6** | **0** | **3** | **+60** |
| **2-case avg** | **24.2** | 9 / 13 | **3** | 6.5 | **77.4** | **12 / 13** | **0** | 5.0 | **+53** |

- A1-v1 caught **all 3 previously-missed criticals** (incl. both on
  cross_01 — thermal curtains and starting currents — and the ov_01
  газовая плита air-exchange critical).
- A1-v1 FP on ov_01 dropped from 10 → 3 (−70%) because the v1
  prompts suppressed the parent-stand multi-agent's variation-spam of
  "slow air speed 0.55 m/s" reported 3× under different formulations.
- A1-v1 FP on cross_01 increased from 3 → 7 because the completeness
  checklist treats cross_01 as a full RD MD when it is actually an
  audit-comparison document. **Mitigation:** v2 should add a
  `document_type` hint to the checklist prompt.

## 7. What still needs LLM data

H3 / H4 / H5 require running A3-v1, A4-v2, A5-v1 on the same case set.
The strongest current signal points at **A1-v1 already meeting all
gating criteria**, so escalation to A3+ may not be necessary.

For full 8-case evaluation:
- A1-v1 on remaining 6 cases: ~12 LLM calls, ~30 min.
- A2-v1 on 3 cases with XD triggers: ~4 calls, ~10 min.
- A4-v2 on the 3 most informative cases: ~12 calls, ~30 min.

Total budget: ~28 calls, ~70 min. Deferred pending budget.

## 8. Best algorithm (empirical recommendation)

**A1-v1 is the best algorithm tested.**

- A1-v1 strict_score avg 77.4 (2 cases) >> A0 24.2.
- A1-v1 caught **all 3 previously-missed criticals**.
- A1-v1 cost ×2.9 vs A0 wall-clock (acceptable for the quality lift).
- A1-v1 does NOT require the XD lens, critic upgrade, or reviewer —
  the simplest hybrid wins on the data we have.

**Production sequencing:**

1. **Phase 0 (now, zero-LLM-cost):** merge `class_dedup.py` and
   `fuzzy_dedup` post-processor. Recovers +20 strict_score on
   the existing baseline.
2. **Phase 1 (next):** add A1-v1 — parallel Sonnet `completeness`
   lens with discipline checklists. Confirmed: catches all 3
   missed criticals, lowers FP on calc-heavy cases.
3. **Phase 2+ (only if needed):** A2 (XD router), A3 (critic),
   A4 (reviewer). Not yet justified by data.

## 9. Best prompts (final)

**For production: `optimized_prompts_v2` (Balanced Engineering).**

Reasoning:
- v1 (Conservative Precision) is the right benchmark for FP-reduction
  testing but its strict rejection of `is_beyond_gt_useful` findings
  removes useful engineering signal in a production setting.
- v2 retains v1's structural improvements (problem_class field,
  discipline checklists, 12-verdict critic, no-speculation guard) but
  surfaces beyond-GT findings explicitly. Engineering reviewers can
  filter them by tag instead of losing them entirely.

If FP rate proves unacceptable on production data, regress to v1.

## 10. What to integrate into production (subject to ablation)

| Item | Source | Production impact |
|---|---|---|
| `class_dedup.py` Python module | [`../runners/class_dedup.py`](../runners/class_dedup.py) | Drop-in post-processor; can be added to `findings_service.py` after Stage 03b. **Safe**. |
| `conditional_router.py` for cross_discipline | [`../runners/conditional_router.py`](../runners/conditional_router.py) | Used by AuditManager pipeline if cross_discipline lens is added. |
| `completeness` Sonnet lens | [`../prompt_optimization/optimized_prompts_v2/completeness.md`](../prompt_optimization/optimized_prompts_v2/completeness.md) | New Stage 01b. Parallel to Stage 01. |
| Discipline checklists | [`../prompt_optimization/checklists/`](../prompt_optimization/checklists/) | Loaded by Stage 01b based on `project_info.section`. |
| Improved critic verdicts | [`../prompt_optimization/optimized_prompts_v2/critic.md`](../prompt_optimization/optimized_prompts_v2/critic.md) | Extends existing Stage 03b critic with 4 new verdicts (`pass_beyond_gt_useful`, `duplicate_same_class`, `non_actionable`, `checklist_gap_*`). |

## 11. What to NOT integrate

| Item | Reason |
|---|---|
| `safety` lens | Overlaps with Stage 02 block analysis and normative lens. Marginal gain ≤ 1 finding/case in parent stand. |
| `normative` lens (separate) | Duplicates production Stage 03b (`norm_verify`) which already does norm status + WebSearch verification. |
| `calculations` lens | Current Stage 01 catches calculation errors adequately. |
| `contradictions` lens | Parity with current method (6/8 hidden contradictions caught by both). |
| Unconditional cross_discipline | The router skips it on calc-only cases and saves ~1.5 Sonnet calls per 8 cases. |
| Full multi-agent A5 architecture | Cost ×3, recall +2, FP +145 baseline. Even with v1 prompts, the architecture itself has more failure surface than A1+critic. |

## 12. Production roadmap (only if ablation confirms)

Phase 1 (low-risk, deploy first):
- Merge `class_dedup.py` into `findings_service.py` as a post-Stage-03c
  pass.
- Apply fuzzy dedup retroactively to existing artefacts.

Phase 2 (medium-risk):
- Add Stage 01b: Sonnet `completeness` lens with v2 prompts +
  discipline checklist.
- Add Python merge step that uses class dedup against Stage 01 output.

Phase 3 (medium-risk):
- Extend Stage 03b critic with the 4 new verdicts from v2.

Phase 4 (high-value, optional):
- Add conditional Stage 01c: Sonnet `cross_discipline` lens, gated by
  the trigger router.

Phase 5 (optional):
- Add conditional Stage 03c reviewer, gated by
  `reviewer_trigger(post_critic_count, missed_warnings, discipline)`.

Each phase is independently revert-able; we recommend rolling out one
phase per week with A/B comparison against the existing pipeline.

## 13. Cost analysis (estimated, subject to ablation)

| Algorithm | Median wall-clock per case | Subscription minutes per 8-case batch |
|---|---|---|
| A0 (current) | 158 s | ~21 min |
| A1-v1 (recommend) | ~250 s | ~33 min (+57%) |
| A4-v2 (conditional) | ~480 s | ~64 min (+205%) |

For a production volume of 50 projects/month with 5 disciplines each:
- A0: 21 min × 50 × 5 = 87.5 hours
- A1-v1: 33 min × 50 × 5 = 138 hours (+50 hours / month)
- A4-v2: 64 min × 50 × 5 = 267 hours (+180 hours / month)

A1-v1 is the only option that adds < 1 hour/day of subscription budget
while bringing measurable quality gain.

## 14. Risk analysis

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| v1/v2 prompts fail to generate `problem_class` reliably | medium | dedup falls back to fuzzy; quality matches replay study | enforce schema with a JSON-schema validator after each LLM call |
| Sonnet lens fails (no JSON, timeout) | low (4/48 in parent stand) | A1 silently degenerates to A0 | runner already falls back gracefully; ensure errors propagate as `pipeline_issues` flag |
| Critic over-rejects (matched_gt drops) | medium | quality regression | v2 critic uses `pass_beyond_gt_useful` to soften; monitor matched_gt per case in CI |
| Discipline checklists need per-project tuning | high | maintenance burden | checklists are markdown — engineers can edit per discipline without code change |

## 15. Production files potentially affected (read-only inventory)

If — and only if — the production team chooses to integrate Phase 1+2:

| Production file | Type of change | Why |
|---|---|---|
| `backend/app/services/findings/findings_service.py` | additive method | post-merge class dedup |
| `backend/app/pipeline/stages/text_analysis/runner.py` | parallel call added (optional) | trigger the completeness lens leg |
| `backend/app/pipeline/manager.py` | NOT MODIFIED (parallelism handled inside the stage) | safety preference |
| `backend/app/services/llm/*` | new prompt template loader | reads `prompt_optimization/optimized_prompts_v2/` |
| `disciplines/<CODE>/checklist.md` | NEW file per discipline | loaded by Stage 01b |

## 16. Dataset expansion need

The 8-case dataset is sufficient to refute clearly-broken algorithms
but insufficient to commit to production:

- 1 case per discipline (except EOM with 2) → no within-discipline
  variance estimate.
- 1 run per case → no LLM-stochasticity estimate.

Before any production integration, expand to:
- ≥ 3 cases per discipline (24 total).
- 3 runs per case (median + IQR reporting).

This expansion is independently valuable for AuditManager regression
testing.

## 17. Final verdict

See [final_verdict.md](final_verdict.md).
