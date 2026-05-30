# Hybrid Policy Projection

Practical trigger (voluntary): run Pro selectively only when Flash output is weak.

**Trigger rule**: Pro escalation: (analysis missing) OR (findings=0 AND kv<=2) OR (empty_summary)


| Metric | Value |
|--------|-------|
| Total blocks in doc | 215 |
| Blocks that would trigger Pro | 1 (0.5%) |
| Flash cost per block (from full run) | $0.00215 |
| Pro cost per block (from sample) | $0.11335 |
| **Projected Flash full cost** | $0.4627 |
| **Projected Pro escalation cost** | $0.1133 |
| **Projected hybrid total cost** | $0.5760 |
| Projected improved blocks (Pro) | 1 |
| Projected added findings (Pro) | 2 |
| Projected extra $/improved block | $0.11335 |

## Sample outcomes (Pro vs Flash on same blocks)
- Improved: 18
- Unchanged: 2
- Degraded: 0

> Projections assume the sample's improved-rate generalizes to the full document.
> This is an estimate, NOT a measured full-doc hybrid run.
