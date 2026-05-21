# A3 — Hybrid Critic Controlled (Current + Completeness + Improved Critic + Class Dedup)

**Hypothesis:** [H3](../hypotheses.md#h3-hybrid-critic-a3).

A1 plus a beefed-up Opus critic with 12 verdicts (vs the prior 8) and a
mandatory class-level dedup step before/after the critic.

## Architecture

```
                ┌────────────────────────┐
                │ [Opus] current_method  │──► current_findings[]
   MD ─────────┤
                │ [Sonnet] completeness  │──► comp_findings[]
                └─────┬──────────────────┘
                      │
                      ▼
                  class_dedup pre-critic (Python, lossless: only marks dupes)
                      │
                      ▼
                  ┌─────────────────────────┐
                  │ [Opus] improved critic  │──► verdicts[]
                  │   (12 verdicts)         │
                  └─────────────────────────┘
                      │
                      ▼
                  Apply verdicts (Python):
                    - drop {no_evidence, speculation, out_of_scope,
                            duplicate_same_class, weak_evidence}
                    - keep {pass, pass_beyond_gt_useful, checklist_gap_valid}
                    - severity adjust where critic suggests
                      │
                      ▼
                  final_findings[]
```

## Cost model

| Resource | Per case |
|---|---|
| Opus calls | 2 (current + critic) |
| Sonnet calls | 1 (completeness) |
| Critic calls | 1 (Opus) |
| Reviewer calls | 0 |
| Expected wall-clock | 350–500 s |

## Expected strengths

- The critic with class-level verdicts catches the kind of
  variation-spam that broke `ov_01_ventilation` (3× slow-air-speed) and
  `cross_01_eom_ov_loads` (8× C-curve breaker).
- Improved verdicts let the critic distinguish
  *engineering-useful-beyond-GT* findings from *speculative* ones,
  preserving signal that the parent stand's critic rejected.

## Expected weaknesses

- Critic adds ~150–250 s/case wall-clock.
- Critic still has a single Opus context — if the input list is > 80
  findings, judgement may degrade. (Mitigation: pre-critic dedup cuts the
  list by ~20–30%.)

## Prompt set

- Current method, completeness — same as A1.
- Critic — new `optimized_prompts_v1/critic_v2.md` (12 verdicts, class-level
  dedup, beyond-GT category).

## Routing rules

No conditional routing in A3 (left for A4).

## Dedup strategy

Two-pass:

1. **Pre-critic Python dedup** (`class_dedup.py`):
   - Build a `problem_class_key` per finding:
     ```
     (category, affected_system_norm, evidence_span_norm,
      discipline_pair_or_none, interface_type_or_none)
     ```
   - For each cluster, mark all but the canonical one with
     `internal_duplicate_of = <canonical_id>`.
   - Pass the full list with markers to the critic.

2. **Critic** is told to verify each cluster and either confirm
   (`duplicate_same_class`) or split (`pass`).

3. **Post-critic Python dedup**:
   - Final pass over the surviving findings — class-collapse again to
     catch anything the critic confirmed.

## Outputs

`algorithm_research/results/A3_hybrid_critic_controlled__<prompt>/<case_id>.json`.

The `meta` block carries:
```json
{
  "pre_critic_count": 25,
  "after_pre_dedup_count": 19,
  "critic_verdicts": {"pass": 12, "duplicate_same_class": 5, "no_evidence": 2, ...},
  "post_critic_count": 12
}
```

## Decision criteria

| Outcome | Action |
|---|---|
| A3 matched_gt ≥ 50, FP ≤ 90, missed critical ≤ 1 | Recommend A3 |
| A3 matched_gt drops below 47 (critic over-rejected) | Soften v1 critic → v2 |
| A3 FP > 100 | Class-dedup logic needs tightening |
