# Winner Recommendation — Recall-Hybrid Stage 02

## Practical answer

**Recommended second-pass engine**: Pro (gemini-3.1-pro-preview)


## Decision criteria (recall-first)

1. Completeness (coverage=100%, no missing/dup/extra) — gates all else
2. Improved blocks count
3. Added findings
4. Degraded blocks count (minimize)
5. Cost/elapsed — tiebreaker only

## Second-pass engine comparison

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


## Escalation summary

- Second-pass blocks: **25**
  - Tier 1 mandatory: 25
  - Tier 2 recommended: 0
  - Tier 3 (Flash trusted): 0

## Flash first-pass results

- Coverage: 100.0%
- Total findings: 0 on 0 blocks
- Cost: $0.0000

## Hybrid total cost estimate

- Flash: $0.0000
- Pro second pass (25 blocks): $0.0000
- **Hybrid total: $0.0000**
- Total findings after hybrid: 0

## Constraints honored

- Flash single-block (no batch mode for Flash)
- No full-document Pro or Claude second pass
- No recrop of existing blocks
- Production defaults unchanged
- Stage 03+ not touched
