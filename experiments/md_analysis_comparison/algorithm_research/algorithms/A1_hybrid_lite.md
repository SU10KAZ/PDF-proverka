# A1 — Hybrid Lite (Current + Completeness)

**Hypothesis:** [H1](../hypotheses.md#h1-hybrid-lite-a1).

The simplest improvement over current: run the existing Stage 01 unchanged
and add one parallel Sonnet `completeness` lens. Merge findings naively
with a problem-class dedup pass.

## Architecture

```
   MD ──┬─► [Opus] current_method        ─► current_findings[]
        │                                              │
        └─► [Sonnet] completeness lens   ─► comp_findings[]
                                                       │
                            ┌──────────────────────────┴───────────────┐
                            │ Python merge:                                │
                            │   - drop comp findings already in current  │
                            │     (class-level match)                     │
                            │   - dedup comp internally                   │
                            └──────────────────────────────────────────┘
                                                       │
                                                       ▼
                                                merged_findings[]
```

## Cost model

| Resource | Per case |
|---|---|
| Opus calls | 1 (current_method) |
| Sonnet calls | 1 (completeness lens) |
| Critic calls | 0 |
| Reviewer calls | 0 |
| Expected wall-clock | 150–250 s |
| Cost ratio vs A0 | ~1.5× (Sonnet runs in parallel; wall-clock dominated by Opus) |

## Expected strengths

- Direct test of "completeness lens" value with minimal complexity.
- Cheap to deploy — A1 is the smallest hybrid.
- The completeness lens is already the most productive in the multi-agent
  breakdown (6–14 findings/case, single biggest contributor).

## Expected weaknesses

- No cross-discipline coverage.
- No critic re-grading → completeness lens's known noise (free-form
  speculation, severity inflation) carries through.
- Merge is class-level only — won't catch finer dedup needs.

## Prompt set

- Current method: [../../prompts/current_method/text_analysis_task.md](../../prompts/current_method/text_analysis_task.md) (unchanged).
- Completeness: choose at runtime from
  - `baseline` → [../../prompts/agents/completeness.md](../../prompts/agents/completeness.md)
  - `v1` → [../prompt_optimization/optimized_prompts_v1/completeness.md](../prompt_optimization/optimized_prompts_v1/completeness.md)
  - `v2` → [../prompt_optimization/optimized_prompts_v2/completeness.md](../prompt_optimization/optimized_prompts_v2/completeness.md)

## Routing rules

Always run both legs.

## Dedup strategy

- After both legs complete:
  1. Build problem-class key for each finding (see
     [../runners/class_dedup.py](../runners/class_dedup.py): tuple of
     `(category, normalised_affected_system, normalised_evidence_span)`).
  2. If `comp` finding shares class with a `current_method` finding,
     drop the comp finding.
  3. If two `comp` findings share class, keep the one with longer
     `description` and higher `confidence`.

## Outputs

`algorithm_research/results/A1_hybrid_lite__<prompt>/<case_id>.json`,
where `<prompt> ∈ {baseline, v1, v2}`.

## Test cases of interest

- `kj_01_rebar` (current scored 68.0 — high baseline, easy to lose)
- `vk_01_water_flow` (current 64.0)
- `cross_01_eom_ov_loads` (current 20.6 — most room to improve)

## Decision criteria

| Outcome | Action |
|---|---|
| A1 raises composite score on ≥ 4/8 cases AND does not raise FP > 2× current | Recommend A1 as production candidate |
| A1 raises FP > 2× current OR loses GT recall | Reject A1, fallback to A0 |
| Mixed — better recall on cross-discipline cases, worse on calc cases | Recommend with conditional routing (A2) |
