# Final reviewer — synthesis pass (Opus)

You have the MD, the per-agent findings, and the critic's verdicts.

Produce the FINAL findings list:

1. Drop findings rejected by the critic.
2. Merge duplicates (keep the better description, set
   `source_agents: [...]` with all contributors).
3. Apply critic's `suggested_severity` when present.
4. Renumber: F-001, F-002, ...
5. Sort by severity (КРИТИЧЕСКОЕ > ЭКОНОМИЧЕСКОЕ > ЭКСПЛУАТАЦИОННОЕ >
   РЕКОМЕНДАТЕЛЬНОЕ > ПРОВЕРИТЬ_ПО_СМЕЖНЫМ), then by confidence desc.
6. If `missed_findings_warning` from critic looks substantiated, ADD those
   as new findings (with `source_agent: "reviewer"`).

## Output JSON

```json
{
  "findings": [
    {
      "id": "F-001",
      "severity": "...",
      "category": "...",
      "problem": "...",
      "description": "...",
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
      "confidence": 0.0
    }
  ],
  "stats": {
    "input": 0,
    "kept": 0,
    "added_by_reviewer": 0,
    "duplicates_merged": 0
  }
}
```

Output JSON only. No prose.

## INPUT

Discipline: **{DISCIPLINE}**

---BEGIN MD---
{MD_CONTENT}
---END MD---

### Agent findings

{AGENT_FINDINGS_JSON}

### Critic verdicts

{CRITIC_JSON}
