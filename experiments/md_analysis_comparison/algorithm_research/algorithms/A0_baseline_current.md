# A0 — Baseline (Current AuditManager Stage 01)

**Role in research:** ground-truth comparator. Re-uses the parent stand's
`current.json` outputs verbatim. No new prompts, no new LLM calls.

## Architecture

```
                              ┌──────────────────────────┐
   MD (full Russian RD)  ─► │ claude -p --model opus-4-7│ ─► findings[] (JSON)
                              │ text_analysis_task.md      │
                              └──────────────────────────┘
```

One prompt, one model, one pass. Mirrors the production
`backend/app/pipeline/stages/text_analysis/runner.py` semantics.

## Cost model

| Resource | Per case |
|---|---|
| Opus calls | 1 |
| Sonnet calls | 0 |
| Critic calls | 0 |
| Reviewer calls | 0 |
| Avg wall-clock (8-case mean) | ~158 s |

## Expected strengths

- Single-pass Opus has the full MD in one context → catches intra-document
  contradictions and complex calculations naturally.
- No inter-agent variations of the same finding → low FP rate.
- Severity calibration is biased to КРИТИЧЕСКОЕ but consistent.

## Expected weaknesses

- Does not actively *search for absence* (completeness gaps). Reacts to
  what is present rather than what is missing.
- Misses cross-discipline coordination issues that require multi-step
  reasoning across sections (`cross_01_eom_ov_loads` case).

## Prompt set

Re-uses [../../prompts/current_method/text_analysis_task.md](../../prompts/current_method/text_analysis_task.md) verbatim. No modification.

## Routing rules

None. Always run.

## Dedup strategy

None at the algorithm level (single LLM produces a single list; internal
duplicates were 0 across 8 cases in the prior run).

## Where to find outputs

`../results/<case_id>/current.json` (parent stand). The algorithm runner
symlinks or copies these into
`algorithm_research/results/A0_baseline_current/<case_id>.json` for
unified scoring.
