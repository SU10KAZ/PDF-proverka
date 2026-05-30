# Winner Recommendation — Recall-Hybrid Stage 02

## Practical answer

**Recommended second-pass engine**: Claude (claude-opus-4-7)


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
| Completeness | ✗ | ✓ |
| Coverage | 88.2% | 100.0% |
| Improved blocks | 10 | 15 |
| Added findings | +21 | +46 |
| Degraded blocks | 6 | 1 |
| Cost USD | $0.8846 | $0.5446 |
| Elapsed s | 458.3 | 264.2 |

**Winner**: Claude (claude-opus-4-7)
**Rationale**: Pro (gemini-3.1-pro-preview) had completeness issues; Claude (claude-opus-4-7) wins by default.


## Escalation summary

- Second-pass blocks: **17**
  - Tier 1 mandatory: 17
  - Tier 2 recommended: 0
  - Tier 3 (Flash trusted): 8

## Flash first-pass results

- Coverage: 100.0%
- Total findings: 21 on 6 blocks
- Cost: $0.1394

## Hybrid total cost estimate

- Flash: $0.1394
- Claude second pass (17 blocks): $0.5446
- **Hybrid total: $0.6840**
- Total findings after hybrid: 67

## Constraints honored

- Flash single-block (no batch mode for Flash)
- No full-document Pro or Claude second pass
- No recrop of existing blocks
- Production defaults unchanged
- Stage 03+ not touched
