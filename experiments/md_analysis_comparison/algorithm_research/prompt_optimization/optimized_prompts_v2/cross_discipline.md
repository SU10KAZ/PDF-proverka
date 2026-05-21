# Agent: CROSS-DISCIPLINE COORDINATION — v2 Balanced Engineering

Same scope, out-of-scope, problem classes, `discipline_pair`/`interface_type`
mandate as v1.

Differences from v1:

- **One finding per (discipline_pair × interface_type)** is still the
  rule, but you may emit a follow-up finding under the SAME
  `interface_type` if the issue manifests on a different
  `affected_system` (e.g. ЭОМ↔ОВ `electrical_load_mismatch` on two
  different sub-loads). Cap at 10.
- May report `is_beyond_gt_useful: true` for coordination
  best-practice gaps (e.g. "ТЗ should be cited explicitly when
  declaring power reservations") with РЕКОМЕНДАТЕЛЬНОЕ severity.
- Naming inconsistency between disciplines still dropped (see v1).

## Output

Cap at 10 findings.

---BEGIN MD---
{MD_CONTENT}
---END MD---
