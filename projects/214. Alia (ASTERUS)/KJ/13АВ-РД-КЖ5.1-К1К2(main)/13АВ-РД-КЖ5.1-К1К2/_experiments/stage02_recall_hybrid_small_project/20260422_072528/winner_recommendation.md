# Winner Recommendation — Recall-Hybrid Stage 02

## Practical answer

**Recommended second-pass engine**: none


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
| Completeness | ✗ | ✗ |
| Coverage | 93.3% | 0.0% |
| Improved blocks | 9 | 0 |
| Added findings | +15 | +0 |
| Degraded blocks | 4 | 15 |
| Cost USD | $0.8564 | $0.0000 |
| Elapsed s | 396.2 | 0.5 |

**Winner**: none
**Rationale**: Both engines had completeness issues — no clear winner.


## Escalation summary

- Second-pass blocks: **15**
  - Tier 1 mandatory: 15
  - Tier 2 recommended: 0
  - Tier 3 (Flash trusted): 10

## Flash first-pass results

- Coverage: 100.0%
- Total findings: 11 on 5 blocks
- Cost: $0.0565

## Hybrid total cost estimate


## Constraints honored

- Flash single-block (no batch mode for Flash)
- No full-document Pro or Claude second pass
- No recrop of existing blocks
- Production defaults unchanged
- Stage 03+ not touched
