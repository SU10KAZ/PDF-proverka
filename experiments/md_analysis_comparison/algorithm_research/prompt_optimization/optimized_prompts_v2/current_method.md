# Current method (single-pass Opus) — v2 Balanced Engineering

Same as v1 with one change:

- May emit `is_beyond_gt_useful: true` findings for substantive
  engineering observations without strict norm violation (e.g.
  "C-curve breaker is allowed but D-curve preferable for compressors").

See [current_method.md (v1)](../optimized_prompts_v1/current_method.md)
for the schema.

---BEGIN MD---
{MD_CONTENT}
---END MD---
