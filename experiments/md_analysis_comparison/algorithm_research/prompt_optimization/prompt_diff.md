# Prompt diff — baseline → v1 → v2

Quick reference for the changes between prompt families. See per-file
diffs in [baseline_prompts/](baseline_prompts/),
[optimized_prompts_v1/](optimized_prompts_v1/),
[optimized_prompts_v2/](optimized_prompts_v2/).

## High-level shape

| Aspect | baseline | v1 (Conservative Precision) | v2 (Balanced Engineering) |
|---|---|---|---|
| `problem_class` field | absent | mandatory | mandatory |
| `affected_system` field | absent | mandatory | mandatory |
| `discipline_pair` for cross_discipline | absent | mandatory | mandatory |
| `interface_type` for cross_discipline | absent | mandatory | mandatory |
| `severity_reasoning` for КРИТИЧЕСКОЕ | absent | mandatory ≤ 120 chars | mandatory |
| `is_beyond_gt_useful` field | absent | absent (rejected) | present (allowed for engineering value-adds) |
| Per-lens output cap | 20 | 4–10 per lens | 4–14 per lens |
| Discipline checklists | none | mandatory; passed in prompt | mandatory; passed in prompt |
| Anti-spam ("do not report" enumeration) | implicit | explicit | explicit |
| Critic verdict count | 8 | 12 | 12 |
| Critic `pass_beyond_gt_useful` | absent | rare | preferred over rejection |
| Reviewer-added findings | up to 5/case | **forbidden** | up to 2 if critical + evidence |
| Severity calibration ceiling for КРИТ % | none | ≤ 30% | ≤ 35% |
| Naming-inconsistency findings | reported | dropped | dropped unless creates ambiguity |
| Speculation guard | implicit | hard rule | hard rule |
| Completeness "speculative absence" | allowed | requires positive evidence | requires positive evidence |

## What v1 and v2 share

- All structural fields (`problem_class`, `affected_system`, etc.)
- Hard evidence rule (verbatim `evidence_quote` mandatory).
- Discipline checklist for completeness.
- 12-verdict critic.
- Class-level dedup pre-critic and post-critic.
- "DO NOT REPORT" enumeration in base.

## Where v2 is more permissive

- Reviewer may add up to 2 findings if missed critical with evidence.
- `is_beyond_gt_useful` findings are kept and surfaced.
- Caps per lens are slightly higher.
- Critic prefers `pass_beyond_gt_useful` over outright rejection.
- Slight severity ceiling difference.

## When to choose which

- **A1 (Hybrid Lite)** → v1 by default (this is the minimal hybrid;
  noise minimisation is the priority).
- **A2 (Hybrid Cross)** → v1.
- **A3 (Hybrid Critic Controlled)** → v1.
- **A4 (Hybrid Production Candidate)** → **v2** by default (production
  benefits from engineering value-adds).
- **A5 (Reduced Multi-Agent)** → both, in ablation: v1 tests the
  pure-prompt hypothesis (H11), v2 tests the production hypothesis
  (H12).

## Headline expected effect

If H11 holds:
- A5-v1 should produce: matched_gt ≈ 50, FP ≈ 90, missed critical ≤ 1.
- That is comparable to current_method on FP, with multi-agent's recall.

If H12 holds:
- A4-v2 should produce: matched_gt ≈ 53, FP ≈ 100, missed critical ≤ 1,
  beyond_gt_useful kept ~10–15.

If H1 holds:
- A1-v1 should produce: matched_gt ≈ 53, FP ≈ 85, missed critical ≤ 2.
