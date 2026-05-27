# Hypotheses — what we are actually testing

Each hypothesis has a short statement, what evidence would confirm/refute it,
and where it is tested in this sub-stand.

## Architecture hypotheses

### H1. Hybrid Lite (A1)

> **Current Stage 01 + parallel `completeness` lens** is better than current
> alone, with cost overhead < +60%.

- **Confirm:** A1 matches or exceeds current GT recall on cases with
  significant `completeness_gain` (KJ, VK, SS, EOM) without raising FP > 2×.
- **Refute:** A1 raises FP > 2× current AND does not gain GT.
- **Where:** [algorithms/A1_hybrid_lite.md](algorithms/A1_hybrid_lite.md),
  ablation rows §10 in [reports/best_algorithm_report.md](reports/best_algorithm_report.md).

### H2. Hybrid Cross (A2)

> **Current + `completeness` + conditional `cross_discipline`** is the
> minimum viable hybrid for the EOM↔OV class of cases.

- **Confirm:** On `cross_01_eom_ov_loads`, A2 recovers the 2 missed critical
  findings that current misses, with FP no more than +5 over current.
- **Refute:** A2 fails to catch the 2 critical OR raises FP > 10 over current.

### H3. Hybrid Critic (A3)

> **Current + `completeness` + improved critic + class-level dedup** removes
> ≥ 50% of multi-agent FPs in a re-merge.

- **Confirm:** When the existing multi-agent agent outputs are re-merged via
  the improved critic, the post-critic FP drops to < 110 (down from 218) with
  matched_gt ≥ 49.
- **Refute:** FP stays > 150 OR matched_gt drops below 45.

### H4. Hybrid Full Controlled (A4)

> **A3 + optional reviewer + class-level dedup + conditional router** is
> the production candidate.

- **Confirm:** Composite score ≥ current_method on ≥ 6/8 cases, GT recall
  ≥ 52, FP ≤ 110.
- **Refute:** Either score < current on > 2 cases or FP > 140.

### H5. Reduced Multi-Agent (A5)

> **Only `completeness` + `cross_discipline` + improved critic + reviewer**
> (no normative/calculations/contradictions/safety lenses) matches or
> exceeds full multi-agent quality at ~50% cost.

- **Confirm:** A5 GT recall ≥ 50, FP ≤ 130, wall-clock ≤ 450 s/case.
- **Refute:** A5 loses ≥ 3 GT vs full multi-agent.

### H6. Better Dedup

> **The dominant source of multi-agent FP excess is missing class-level
> dedup; problem-class collapsing alone (no other changes) drops FP by
> ≥ 80.**

- **Confirm:** Replaying cached multi-agent agent JSON through `class_dedup.py`
  (no LLM re-call) reduces total findings from 272 to ≤ 190 with matched_gt
  unchanged (52) and missed critical unchanged (1).
- **Refute:** Class-level dedup either drops below 200 with significant GT
  loss (matched_gt < 49) or fails to drop below 220.

## Quality / scoring hypotheses

### H7. Severity Calibration

> **Critic should only adjust severity / dedup / strip speculation; it
> should not block engineering-useful findings beyond GT.**

- **Confirm:** When critic is allowed to keep findings tagged
  `pass_beyond_gt_useful`, human review effort drops AND no critical GT
  is lost.
- **Refute:** A "permissive critic" policy raises FP without engineering
  value.

### H8. Conditional Routing

> **`cross_discipline` lens is only useful when the MD contains
> cross-discipline trigger markers.** Routing it conditionally saves
> ~50% of `cross_discipline` calls without losing recall.

- **Confirm:** A trigger-based router skips `cross_discipline` on ≥ 3 of
  8 cases without losing the corresponding GT findings.
- **Refute:** Skipping `cross_discipline` on any case with a GT critical
  cross-discipline finding loses recall.

### H9. Checklist Completeness

> **Discipline-specific checklist-based `completeness` is better than
> free-form `completeness`.**

- **Confirm:** Checklist-based v1/v2 prompts reduce `completeness` FPs by
  ≥ 30% while preserving or increasing matched GT.
- **Refute:** Checklist either produces fewer findings overall (under-detection)
  or the same FP rate.

### H10. Ground-truth Bias

> **A non-trivial fraction of multi-agent "FPs" are real engineering
> findings beyond GT — not noise.**

- **Confirm:** Manual inspection of a stratified sample of 30 multi-agent
  FPs finds ≥ 30% to be substantive, evidence-quoted, actionable.
- **Refute:** ≥ 80% of inspected FPs are duplicates, speculation, or
  out-of-scope.

## Prompt-quality hypotheses

### H11. Prompt Quality Hypothesis (the headline)

> **The primary failure mode of multi-agent is the *prompts*, not the
> architecture.** Replacing the lens / critic prompts with optimized v1
> (Conservative Precision) on the same multi-agent architecture cuts FP
> by ≥ 50% without losing GT.

- **Confirm:** Running A5 (reduced multi-agent) with `optimized_prompts_v1`
  produces matched_gt ≥ 50 and FP ≤ 110 — comparable to current_method on
  noise but with multi-agent's recall.
- **Refute:** Even with v1 prompts, FP stays > 150.

### H12. Optimized Hybrid Hypothesis

> **A4 with `optimized_prompts_v2` (Balanced Engineering) beats current
> on composite score across ≥ 6/8 cases.**

- **Confirm:** A4-v2 composite score median > current on at least 6 cases,
  matched_gt ≥ 53, FP ≤ 100, missed critical ≤ 1.
- **Refute:** Tied or worse on > 3 cases, OR cost > +100% over current.

## Hypothesis-by-test matrix

| Hypothesis | What we run | Re-use cached | New LLM cost |
|---|---|---|---|
| H1 | A1 on 8 cases × 1 prompt set | existing current.json | 8 × Sonnet completeness |
| H2 | A2 on 8 cases | existing current.json | 8 × Sonnet completeness + ~4 × Sonnet cross-discipline (router) |
| H3 | A3 reuses cached multi-agent agent JSON, re-runs critic | reuse 6×8=48 agent outputs | 8 × Opus critic |
| H4 | A4 on 8 cases | partial reuse | 8 × Sonnet ×2 + 8 × Opus critic + conditional Opus reviewer |
| H5 | A5 on 8 cases | none directly | 8 × Sonnet ×2 + 8 × Opus critic + 8 × Opus reviewer |
| H6 | class-level dedup re-merge of cached multi-agent outputs | full reuse | 0 |
| H7 | offline label re-counting using extended verdict set | full reuse | 0 |
| H8 | trigger-based router applied retroactively to cached outputs | full reuse | 0 |
| H9 | A1 with baseline vs v1 vs v2 completeness on selected cases | partial reuse | 3-6 × Sonnet on 3 cases |
| H10 | manual inspection script + sample report | full reuse | 0 |
| H11 | A5-v1 on selected ablation cases | partial reuse | 5 × (Sonnet ×2 + Opus ×2) |
| H12 | A4-v2 on all 8 cases | partial reuse | 8 × (Sonnet ×2 + Opus ×1) + conditional Opus |

The dominant new cost is H4 / H5 / H11 / H12 (each adds 16–32 Sonnet calls and
8–16 Opus calls). H3, H6, H7, H8, H10 are purely re-runs over cached
artifacts and add zero LLM cost.

For a tractable end-to-end pass we prioritise: H3, H6, H8, H10 (no new LLM
calls), then H1, H9, H11 (limited new LLM calls on the 2–3 most informative
cases: `cross_01_eom_ov_loads`, `ov_01_ventilation`, `kj_01_rebar`).
