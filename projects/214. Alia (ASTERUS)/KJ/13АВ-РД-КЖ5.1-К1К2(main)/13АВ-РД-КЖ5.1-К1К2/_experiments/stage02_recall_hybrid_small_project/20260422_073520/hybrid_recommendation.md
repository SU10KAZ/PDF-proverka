# Hybrid Recommendation — Recall-Oriented Policy

## Summary

This experiment evaluated Flash single-block as first pass + selective second pass on a small KJ project (25 blocks) with recall-first escalation logic.

**Design goal**: minimize missed issues, not minimize cost.


## Flash First Pass

- Coverage: 100.0% (missing 0, dup 0, extra 0)
- Total findings: 21 on 6 blocks
- Cost: $0.1394
- Flash ✓ complete

## Escalation Tiers (Recall-First)

- Tier 1 mandatory: 17 blocks (68%) — escalated regardless of cost
- Tier 2 recommended: 0 blocks (0%) — included (small project, budget not limiting)
- **Total second-pass: 17 blocks (68%)**

## Second-Pass Comparison

### Pro (gemini-3.1-pro-preview) on 17 blocks:
  - Improved: 10 | Unchanged: 1 | Degraded: 6
  - Added findings: +21
  - Cost: $0.8846
### Claude (claude-opus-4-7) on 17 blocks:
  - Improved: 15 | Unchanged: 1 | Degraded: 1
  - Added findings: +46
  - Cost: $0.5446

## Winner

**Second-pass engine winner**: Claude (claude-opus-4-7)

## Second-pass winner selection (recall-first criteria)

| Criterion | Pro (gemini-3.1-pro-preview) | Claude (claude-opus-4-7) |
|-----------|---|---|
| Completeness | ✗ | ✓ |
| Coverage | 88.2% | 100.0% |
| Improved blocks | 10 | 15 |
| Added findings | +21 | +46 |
| Degraded blocks | 6 | 1 |
| Cost USD | $0.8846 | $0.5446 |
| Elapsed s | 458.3 | 264.2 |

**Winner**: Claude (claude-opus-4-7)
**Rationale**: Pro (gemini-3.1-pro-preview) had completeness issues; Claude (claude-opus-4-7) wins by default.


## Practical Policy Recommendation


### Mode A: Flash only
  - Coverage: 100.0%
  - Findings: 21
  - Cost: ~$0.1394

### Mode B: Flash + Pro (gemini-3.1-pro-preview) on escalation set
  - Total findings: 42 (+21 from second pass)
  - Improved blocks: 10
  - Combined cost: ~$1.0239

### Mode C: Flash + Claude (claude-opus-4-7) on escalation set
  - Total findings: 67 (+46 from second pass)
  - Improved blocks: 15
  - Combined cost: ~$0.6840

## Escalation Policy Assessment

- 17/25 blocks (68%) sent to second pass.
- Policy is recall-oriented: tier1 includes ALL blocks with any finding, heavy, merged, high/medium issue potential or miss_risk.
- For a KJ project (reinforced concrete drawings), engineering density is high — escalation rate reflects appropriate caution.

## Constraints honored

- Flash single-block (no batch mode)
- No full-document Pro or Claude runs
- Production stage_models.json UNCHANGED
- Claude CLI production path untouched
- No recrop of existing blocks
- Stage 03+ not touched
