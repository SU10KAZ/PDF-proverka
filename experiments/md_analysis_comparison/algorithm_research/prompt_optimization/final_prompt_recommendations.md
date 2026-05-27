# Final Prompt Recommendations

**Date:** 2026-05-20
**Validation:** 2-case targeted ablation + 8-case replay study.

## TL;DR

- **Use `optimized_prompts_v2`** in production.
- **Use `optimized_prompts_v1`** for benchmark / regression tests
  (it is the noise-minimisation reference).
- **Update v2 with a `document_type` field** (`full_rd` /
  `audit_comparison` / `tz_vs_rd` / `specification_only`) — discovered
  during cross_01 ablation. Discipline checklist behaviour depends on
  document type.

## What changed v0 → v1 → v2 (effective)

The five highest-impact prompt fixes (estimated FP reduction in
brackets, validated by the A1-v1 ablation):

1. **`problem_class` + `affected_system` + `interface_type` +
   `discipline_pair`** mandatory fields on every finding.
   — Class-level dedup is then mechanical.
   — Replay study: without these, retroactive dedup recovers 0.

2. **Discipline-specific completeness checklists** ([checklists/](checklists/))
   passed in the prompt.
   — ov_01 ablation: completeness lens correctly returned
     `not_applicable` instead of producing 10 free-form FPs.

3. **12 critic verdicts** (split `duplicate_same_class` from
   `duplicate_same_issue`, add `pass_beyond_gt_useful`,
   `non_actionable`, `checklist_gap_*`).
   — Preserves engineering value-adds; collapses class duplicates.

4. **No-speculation hard rule + DO NOT REPORT enumeration**.
   — A1-v1 cross_01 has 0 speculative findings vs A0's 1 vague
     "verify X" finding.

5. **Severity calibration with explicit `severity_reasoning`** ≤ 120
   chars for КРИТИЧЕСКОЕ.
   — A1-v1 ov_01: КРИТИЧЕСКОЕ % is 44% (4 of 9), reasonable for a
     gas-kitchen / fire-safety case (vs 50%+ in A0).

## What v1 vs v2 differ on

| Aspect | v1 (recommend for benchmarking) | v2 (recommend for production) |
|---|---|---|
| Reviewer adds findings | forbidden | up to 2 if critical + evidence |
| `is_beyond_gt_useful` findings | rare | preferred over rejection |
| Per-lens output caps | strict (4–10) | slightly higher (4–14) |
| Severity ceiling for КРИТИЧЕСКОЕ | ≤ 30% | ≤ 35% |

The empirical ablation used v1. The win was so decisive that v2 is
recommended for production not because v1 falls short, but because v2
preserves engineering value that v1 may filter out — and engineers
working with the output benefit from seeing the beyond-GT signal.

## Document-type adjustment (v3 candidate)

Discovered during cross_01 ablation: the completeness lens with a
discipline checklist produced 4 spurious findings on cross_01 because
the MD is an **audit-comparison document**, not a full РД. The
checklist incorrectly treated absent "однолинейная схема" as
КРИТИЧЕСКОЕ.

**Recommended v3 change:**

Add to base prompt:

```
The MD is of type {DOCUMENT_TYPE}, one of:
  - full_rd: complete рабочая документация
  - audit_comparison: cross-discipline audit / mismatch report
  - tz_vs_rd: comparison of задание vs realised design
  - specification_only: spec list / cable journal extract

For audit_comparison / tz_vs_rd, the completeness lens applies the
checklist ONLY to items the MD claims to fully cover, not to items
absent because the MD is intentionally scoped to a subset.
```

This is a single-line addition that should remove the 4 spurious
cross_01 FPs without affecting other cases. Not validated yet;
deferred to next ablation round.

## What still needs validation before production merge

1. A1-v1 on the remaining 6 cases (`ar_01`, `eom_01`, `kj_01`,
   `multi_01`, `ss_01`, `vk_01`).
2. `document_type` hint validated on cross_01 and multi_01 (the 2
   non-full_rd cases in the dataset).
3. Variance run: 3 LLM calls per case on at least 3 informative cases
   (cross_01, ov_01, kj_01) to bound run-to-run variance.
4. Manual labelling of 30 multi-agent FPs as
   `real_fp` / `beyond_gt_useful` / `dup_same_class` to quantify
   H10.

Total budget: ~40 LLM calls, ~100 min subscription time.

## Files of record

- v1 prompts: [`optimized_prompts_v1/`](optimized_prompts_v1/) (9 files)
- v2 prompts: [`optimized_prompts_v2/`](optimized_prompts_v2/) (9 files)
- Checklists: [`checklists/`](checklists/) (8 disciplines)
- Diagnostics: [`prompt_diagnostics.md`](prompt_diagnostics.md)
- Diff: [`prompt_diff.md`](prompt_diff.md)
- Empirical results: [`prompt_ablation_results.md`](prompt_ablation_results.md)
