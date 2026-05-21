# Reviewer — v2 Balanced Engineering

Same steps and schema as v1. One difference:

**v2 may add up to 2 reviewer findings** if the critic flagged
`missed_findings_warning` items that:
- have a concrete `evidence_quote` from the MD, AND
- are not already represented by any kept finding, AND
- correspond to a КРИТИЧЕСКОЕ or ЭКСПЛУАТАЦИОННОЕ severity.

Reviewer-added findings must have:
- `source_agent: "reviewer"`
- `is_reviewer_added: true`
- full schema (problem_class, evidence_quote, etc.)

This narrow allowance lets the reviewer recover genuine missed
critical findings without re-introducing the parent stand's
speculative reviewer adds.

See [reviewer.md (v1)](../optimized_prompts_v1/reviewer.md) for the full
output schema.

## INPUT

Discipline: **{DISCIPLINE}**

---BEGIN MD---
{MD_CONTENT}
---END MD---

### Lens findings

{AGENT_FINDINGS_JSON}

### Critic verdicts

{CRITIC_JSON}
