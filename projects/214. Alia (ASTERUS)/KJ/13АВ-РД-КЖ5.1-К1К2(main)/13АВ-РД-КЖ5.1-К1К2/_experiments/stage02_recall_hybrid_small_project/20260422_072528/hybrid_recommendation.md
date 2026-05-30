# Hybrid Recommendation — Recall-Oriented Policy

## Summary

This experiment evaluated Flash single-block as first pass + selective second pass on a small KJ project (25 blocks) with recall-first escalation logic.

**Design goal**: minimize missed issues, not minimize cost.


## Flash First Pass

- Coverage: 100.0% (missing 0, dup 0, extra 0)
- Total findings: 11 on 5 blocks
- Cost: $0.0565
- Flash ✓ complete

## Escalation Tiers (Recall-First)

- Tier 1 mandatory: 15 blocks (60%) — escalated regardless of cost
- Tier 2 recommended: 0 blocks (0%) — included (small project, budget not limiting)
- **Total second-pass: 15 blocks (60%)**

## Second-Pass Comparison

### Pro (gemini-3.1-pro-preview) on 15 blocks:
  - Improved: 9 | Unchanged: 2 | Degraded: 4
  - Added findings: +15
  - Cost: $0.8564
### Claude (claude-opus-4-7) on 15 blocks:
  - Improved: 0 | Unchanged: 0 | Degraded: 15
  - Added findings: +0
  - Cost: $0.0000

## Winner

**Second-pass engine winner**: none

## Second-pass winner selection (recall-first criteria)

| Criterion | Pro (gemini-3.1-pro-preview) | Claude (claude-opus-4-7) |
|-----------|---|---|
| Completeness | ✗ | ✗ |
| Coverage | 93.3% | 0.0% |
| Improved blocks | 9 | 0 |
| Added findings | +15 | +0 |
| Degraded blocks | 4 | 15 |
| Cost USD | $0.8564 | $0.0000 |
| Elapsed s | 396.2 | 0.5 |

**Winner**: none
**Rationale**: Both engines had completeness issues — no clear winner.


## Practical Policy Recommendation


### Mode A: Flash only
  - Coverage: 100.0%
  - Findings: 11
  - Cost: ~$0.0565

### Mode B: Flash + Pro (gemini-3.1-pro-preview) on escalation set
  - Total findings: 26 (+15 from second pass)
  - Improved blocks: 9
  - Combined cost: ~$0.9128

### Mode C: Flash + Claude (claude-opus-4-7) on escalation set
  - Total findings: 11 (+0 from second pass)
  - Improved blocks: 0
  - Combined cost: ~$0.0565

## Escalation Policy Assessment

- 15/25 blocks (60%) sent to second pass.
- Policy is recall-oriented: tier1 includes ALL blocks with any finding, heavy, merged, high/medium issue potential or miss_risk.
- For a KJ project (reinforced concrete drawings), engineering density is high — escalation rate reflects appropriate caution.

## Constraints honored

- Flash single-block (no batch mode)
- No full-document Pro or Claude runs
- Production stage_models.json UNCHANGED
- Claude CLI production path untouched
- No recrop of existing blocks
- Stage 03+ not touched
