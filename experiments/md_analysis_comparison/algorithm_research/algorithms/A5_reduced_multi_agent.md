# A5 — Reduced Multi-Agent (completeness + cross_discipline + improved critic + reviewer)

**Hypothesis:** [H5](../hypotheses.md#h5-reduced-multi-agent-a5),
[H11](../hypotheses.md#h11-prompt-quality-hypothesis-the-headline).

The control for the prompt-quality hypothesis. Drops the lenses that the
parent stand identified as redundant (normative, calculations,
contradictions, safety) and keeps only the two that contributed
uniquely. With improved prompts, this is the cleanest test of whether
the multi-agent architecture itself has any value or whether the
*architecture* was carrying noise that the *prompts* introduced.

## Architecture

```
   MD ──┬─► [Sonnet] completeness      ─► comp[]
        │
        ├─► [Sonnet] cross_discipline  ─► xd[]
        │
        └─► (no other lenses)
                       │
                       ▼
                  class_dedup pre-critic
                       │
                       ▼
                  [Opus] improved critic ─► verdicts[]
                       │
                       ▼
                  Apply verdicts + post-critic dedup
                       │
                       ▼
                  [Opus] reviewer ─► final_findings[]
```

The current_method Opus call is **not** part of A5. This is the test of
whether two lenses + critic + reviewer alone are enough.

## Cost model

| Resource | Per case |
|---|---|
| Opus calls | 2 (critic + reviewer) |
| Sonnet calls | 2 |
| Expected wall-clock | 350–500 s |

## Expected strengths

- Cleanest test of H11: if A5 with v1 prompts matches multi-agent's
  recall with current_method's FP, the prompts (not the architecture)
  were the problem.
- Cheaper than full multi-agent (4 fewer Sonnet calls).
- Forces the critic to surface intra-discipline findings that a full
  multi-agent setup hides because they duplicate cross-discipline /
  completeness output.

## Expected weaknesses

- Without current_method's intra-section reasoning, A5 may miss the
  arithmetic-in-tables and contradiction findings that the
  `calculations` and `contradictions` lenses caught in full multi-agent.
- If A5 fails on KJ/VK (calc-heavy cases), it confirms current_method
  has irreplaceable single-pass value.

## Prompt set

- Completeness, cross_discipline: prefer
  [`../prompt_optimization/optimized_prompts_v1/`](../prompt_optimization/optimized_prompts_v1/)
  for the prompt-quality test; `optimized_prompts_v2` for the
  reduced-multi-agent comparator.
- Critic: v1 (Conservative).
- Reviewer: existing parent-stand reviewer prompt + "no speculative
  additions" rule.

## Routing rules

Both lenses always run in A5 (no conditional cross_discipline). This
keeps the architecture honest as a control.

## Dedup strategy

Same as A3/A4 (pre-critic + post-critic class-level dedup).

## Outputs

`algorithm_research/results/A5_reduced_multi_agent__<prompt>/<case_id>.json`.

## Decision criteria

| Outcome | Conclusion about H11 |
|---|---|
| A5-v1: matched_gt ≥ 50, FP ≤ 110 | H11 confirmed (prompts were the problem) |
| A5-v1: matched_gt < 47 | A5 loses GT — current_method's reasoning is irreplaceable |
| A5-v1: FP > 150 | H11 refuted; architecture itself is noisy |
| A5-v1: matched_gt ≥ 50 but FP between 110–150 | Partial confirmation (prompts help but not enough) |
