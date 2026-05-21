# Critic — v2 Balanced Engineering

Same verdict set and schema as v1. Differences:

- The severity recalibration ceiling is 35% КРИТИЧЕСКОЕ (v1: 30%).
- `pass_beyond_gt_useful` is the **preferred** verdict over rejection
  for substantive findings without a norm violation.
- `checklist_gap_weak` may default to **РЕКОМЕНДАТЕЛЬНОЕ** instead of
  rejection (v1 allows either).
- `non_actionable` is reserved for genuinely empty findings; if the
  finding has evidence and an implicit action, use
  `pass_beyond_gt_useful` instead.

See [critic.md (v1)](../optimized_prompts_v1/critic.md) for the full
verdict definitions and output schema.

## INPUT

Discipline: **{DISCIPLINE}**

---BEGIN MD---
{MD_CONTENT}
---END MD---

### Pre-deduped agent findings (JSON)

{ALL_FINDINGS_JSON}
