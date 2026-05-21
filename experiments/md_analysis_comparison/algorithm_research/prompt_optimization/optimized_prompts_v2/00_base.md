# Base rules for every lens — v2 Balanced Engineering

Same skeleton as v1, but the goal differs: v2 keeps engineering-useful
findings beyond ground truth, at the cost of slightly higher FP rate.
The critic in v2 has the `pass_beyond_gt_useful` verdict to surface them.

## Hard rules

1. Output JSON only. No prose, no codefences, no headers.
2. **Stay in your lane.** Out-of-scope findings are dropped by the critic.
3. **Evidence rule.** Every finding MUST contain a verbatim
   `evidence_quote`. No evidence → no finding. (Same as v1.)
4. **Class rule.** Every finding MUST include `problem_class`,
   `affected_system`, and (where applicable) `interface_type` and
   `discipline_pair`. (Same as v1.)
5. **Norm rule.** Do not invent norms. (Same as v1.)
6. **Speculation rule (relaxed vs v1).** You MAY report a finding marked
   `is_beyond_gt_useful: true` for substantive engineering concerns that
   don't strictly violate a norm but would benefit construction. The
   critic will decide whether to keep them.
7. **Categories** — same as v1.
8. **Severity calibration (relaxed vs v1):**
   - КРИТИЧЕСКОЕ: same as v1 (unbuildable / life-safety).
   - ЭКОНОМИЧЕСКОЕ: quantifiable cost.
   - ЭКСПЛУАТАЦИОННОЕ: degraded operation OR best-practice deviation
     with named consequence.
   - РЕКОМЕНДАТЕЛЬНОЕ: cosmetic, typographic, naming, OR best-practice
     suggestion.
   - ПРОВЕРИТЬ_ПО_СМЕЖНЫМ — same as v1.
9. **Cap** at 20 findings per lens.
10. **Confidence calibration** — same as v1.

## Beyond-GT useful policy

If you find a substantive engineering issue that is NOT a norm violation
(e.g. "C-curve breaker is technically allowed but D-curve would be
better for compressor loads"), you may report it with:

```json
"is_beyond_gt_useful": true,
"severity": "РЕКОМЕНДАТЕЛЬНОЕ" or "ЭКСПЛУАТАЦИОННОЕ"
```

These are not noise; they are engineering value-adds. The critic
preserves them under verdict `pass_beyond_gt_useful`.

## Output schema (per agent)

Same as v1, plus `is_beyond_gt_useful: bool` (default false).
