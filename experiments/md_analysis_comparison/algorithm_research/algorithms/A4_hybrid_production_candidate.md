# A4 — Hybrid Production Candidate (Current + Completeness + Conditional cross_discipline + Improved Critic + Conditional Reviewer)

**Hypothesis:** [H4](../hypotheses.md#h4-hybrid-full-controlled-a4),
[H12](../hypotheses.md#h12-optimized-hybrid-hypothesis).

The full hybrid stack. This is the candidate we would recommend taking
to production *if* the data supports it.

## Architecture

```
                ┌────────────────────────┐
                │ [Opus] current_method  │──► current[]
   MD ────────┤
                │ [Sonnet] completeness  │──► comp[]
                │  (v1 / v2)              │
                └─────────────────────────┘
                                                       │
                                                       │  Trigger router (Python)
                                                       │  ┌────────────┐
                                                       │  │ check MD   │
                                                       │  │ for XD     │
                                                       │  │ markers    │
                                                       │  └────┬───────┘
                                                       │       │
                                                       │       ├── no trigger → []
                                                       │       │
                                                       │       └── trigger → [Sonnet] cross_discipline (v1/v2) ──► xd[]
                                                       │                                                            │
                                                       └────────────────────────────────────────────────────────────┘
                                                       │
                                                       ▼
                                                       class_dedup pre-critic
                                                       │
                                                       ▼
                                                       ┌─────────────────────┐
                                                       │ [Opus] improved     │──► verdicts[]
                                                       │ critic (12 verdicts)│
                                                       └─────────────────────┘
                                                       │
                                                       ▼
                                                       Apply verdicts; class_dedup post-critic
                                                       │
                                                       ▼
                                                       Reviewer trigger:
                                                         IF critic flagged ≥ 2 substantive missed_findings_warning
                                                         AND post-critic count < 12
                                                       │
                                                       ▼ (conditional)
                                                       [Opus] reviewer → final[]
                                                       │
                                                       ▼
                                                       final_findings[]
```

## Cost model

| Scenario | Opus calls | Sonnet calls | Wall-clock |
|---|---|---|---|
| no XD trigger, no reviewer | 2 | 1 | 350–450 s |
| XD trigger, no reviewer | 2 | 2 | 400–550 s |
| XD trigger, reviewer | 3 | 2 | 500–700 s |
| Estimated mean over 8 cases | ~2.4 Opus, ~1.875 Sonnet | ~480 s | |

Cost overhead vs A0 (mean): wall-clock ~3.0×, Opus calls ~2.4×.

## Expected strengths

- Conditional execution keeps cost on uninteresting cases close to A1.
- Two-layer dedup (class-level + critic) compresses the agent variations.
- Conditional reviewer adds 2–5 missed findings on the cases where it
  matters without paying on the rest.

## Expected weaknesses

- Most complex algorithm — more failure surface (router heuristic, critic
  prompt, reviewer trigger logic).
- Reviewer can re-introduce speculative findings (the parent stand showed
  reviewer added 2–5 per case, some of which were beyond-GT noise).

## Prompt set

- Current method (unchanged).
- Completeness, cross_discipline — `optimized_prompts_v2` by default
  (Balanced Engineering; v1 too strict for production).
- Critic — v2 (Balanced).
- Reviewer — only invoked when triggered; prompt is the existing reviewer
  prompt augmented with explicit "no speculative additions" rule.

## Routing rules

1. **cross_discipline** — same as A2 (trigger-based).
2. **reviewer** — fires only when:
   - critic reported `missed_findings_warning` ≥ 2, AND
   - post-critic count < 12, AND
   - case discipline is not in {AR, KJ} (these benefit less from reviewer
     synthesis per the parent stand data).

## Dedup strategy

Same as A3 (pre-critic + post-critic class-level dedup).

## Outputs

`algorithm_research/results/A4_hybrid_production_candidate__<prompt>/<case_id>.json`.

Meta block includes router decisions:
```json
{
  "xd_triggered": true,
  "xd_triggers_hit": ["смеж", "ОВ", "тепловая нагрузка"],
  "reviewer_triggered": false,
  "reviewer_reason": "post-critic count >= 12"
}
```

## Decision criteria (production go/no-go)

| Outcome | Action |
|---|---|
| matched_gt ≥ 53, FP ≤ 100, missed crit ≤ 1, composite score ≥ A0 on ≥ 6/8 | RECOMMEND for production integration |
| matched_gt ≥ 50, FP ≤ 110 | Recommend with caveat — collect more data |
| matched_gt < 49 or FP > 130 | Reject — fall back to A1 or A0 |
