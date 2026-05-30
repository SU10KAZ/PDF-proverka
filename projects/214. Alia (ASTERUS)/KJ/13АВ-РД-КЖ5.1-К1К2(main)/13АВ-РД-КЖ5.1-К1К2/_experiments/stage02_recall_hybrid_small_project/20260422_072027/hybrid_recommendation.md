# Hybrid Recommendation — Recall-Oriented Policy

## Summary

This experiment evaluated Flash single-block as first pass + selective second pass on a small KJ project (25 blocks) with recall-first escalation logic.

**Design goal**: minimize missed issues, not minimize cost.


## Flash First Pass

- Coverage: 100.0% (missing 0, dup 0, extra 0)
- Total findings: 0 on 0 blocks
- Cost: $0.0000
- Flash ✓ complete

## Escalation Tiers (Recall-First)

- Tier 1 mandatory: 25 blocks (100%) — escalated regardless of cost
- Tier 2 recommended: 0 blocks (0%) — included (small project, budget not limiting)
- **Total second-pass: 25 blocks (100%)**

## Second-Pass Comparison

### Pro (gemini-3.1-pro-preview) on 25 blocks:
  - Improved: 0 | Unchanged: 25 | Degraded: 0
  - Added findings: +0
  - Cost: $0.0000
### Claude (claude-opus-4-7) on 25 blocks:
  - Improved: 0 | Unchanged: 25 | Degraded: 0
  - Added findings: +0
  - Cost: $0.0000

## Winner

**Second-pass engine winner**: Pro (gemini-3.1-pro-preview)

## Second-pass winner selection (recall-first criteria)

| Criterion | Pro (gemini-3.1-pro-preview) | Claude (claude-opus-4-7) |
|-----------|---|---|
| Completeness | ✓ | ✓ |
| Coverage | 100.0% | 100.0% |
| Improved blocks | 0 | 0 |
| Added findings | +0 | +0 |
| Degraded blocks | 0 | 0 |
| Cost USD | $0.0000 | $0.0000 |
| Elapsed s | 0.0 | 0.0 |

**Winner**: Pro (gemini-3.1-pro-preview)
**Rationale**: Equal quality; Pro (gemini-3.1-pro-preview) preferred (lower cost $9999.0000 vs $9999.0000).


## Practical Policy Recommendation


### Mode A: Flash only
  - Coverage: 100.0%
  - Findings: 0
  - Cost: ~$0.0000

### Mode B: Flash + Pro (gemini-3.1-pro-preview) on escalation set
  - Total findings: 0 (+0 from second pass)
  - Improved blocks: 0
  - Combined cost: ~$0.0000

### Mode C: Flash + Claude (claude-opus-4-7) on escalation set
  - Total findings: 0 (+0 from second pass)
  - Improved blocks: 0
  - Combined cost: ~$0.0000

## Escalation Policy Assessment

- 25/25 blocks (100%) sent to second pass.
- Policy is recall-oriented: tier1 includes ALL blocks with any finding, heavy, merged, high/medium issue potential or miss_risk.
- For a KJ project (reinforced concrete drawings), engineering density is high — escalation rate reflects appropriate caution.

## Constraints honored

- Flash single-block (no batch mode)
- No full-document Pro or Claude runs
- Production stage_models.json UNCHANGED
- Claude CLI production path untouched
- No recrop of existing blocks
- Stage 03+ not touched
