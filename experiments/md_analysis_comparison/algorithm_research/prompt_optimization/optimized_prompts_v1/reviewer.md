# Reviewer — v1 Conservative Precision

You receive the MD, the lens findings, the critic's verdicts, and a
class-dedup map. Produce the FINAL findings list.

## Steps

1. Drop findings rejected by the critic (`no_evidence`, `speculation`,
   `out_of_scope`, `non_actionable`, `checklist_gap_weak`-rejected,
   `duplicate_same_issue`, `duplicate_same_class` non-canonical).
2. Apply `suggested_severity` where present.
3. Tag findings with `is_beyond_gt_useful: true` if critic verdict was
   `pass_beyond_gt_useful`.
4. Renumber: `F-001`, `F-002`, …
5. Sort by severity (КРИТИЧЕСКОЕ > ЭКОНОМИЧЕСКОЕ > ЭКСПЛУАТАЦИОННОЕ >
   РЕКОМЕНДАТЕЛЬНОЕ > ПРОВЕРИТЬ_ПО_СМЕЖНЫМ), then by `confidence` desc.
6. **DO NOT add new findings.** v1 forbids reviewer-added findings —
   this is the source of speculative noise in the parent stand.
7. Merge same-issue duplicates: keep the longer description; set
   `source_agents: [...]` listing all contributors.
8. **Final class-dedup pass:** group by `(problem_class,
   affected_system, interface_type)`. If two findings still share class
   after the critic, keep the one with higher `confidence` × wider
   `evidence_quote`.

## Output schema

```json
{
  "findings": [
    {
      "id": "F-001",
      "severity": "...",
      "category": "...",
      "problem_class": "...",
      "affected_system": "...",
      "discipline_pair": null,
      "interface_type": null,
      "problem": "...",
      "description": "...",
      "root_cause": "...",
      "consequence": "...",
      "norm": "...",
      "norm_quote": "...",
      "norm_confidence": 0.0,
      "recommendation": "...",
      "risk": "...",
      "evidence_quote": "...",
      "md_excerpt": "...",
      "discipline": "...",
      "cross_discipline_with": [],
      "source_agents": ["normative", "completeness"],
      "is_beyond_gt_useful": false,
      "confidence": 0.0
    }
  ],
  "stats": {
    "input": 0,
    "kept": 0,
    "added_by_reviewer": 0,
    "duplicates_merged": 0,
    "beyond_gt_kept": 0
  }
}
```

Output JSON only.

## INPUT

Discipline: **{DISCIPLINE}**

---BEGIN MD---
{MD_CONTENT}
---END MD---

### Lens findings

{AGENT_FINDINGS_JSON}

### Critic verdicts

{CRITIC_JSON}
