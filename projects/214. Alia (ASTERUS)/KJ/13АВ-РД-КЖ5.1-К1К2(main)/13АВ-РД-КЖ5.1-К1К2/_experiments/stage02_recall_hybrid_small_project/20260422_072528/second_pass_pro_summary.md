# Second Pass — Pro (gemini-3.1-pro-preview)
Escalation set: 15 blocks
## Completeness
| Metric | Value |
|--------|-------|
| Coverage | 93.3% |
| Missing / Duplicate / Extra | 1 / 0 / 0 |
| Unreadable | 0 |

## Quality Deltas vs Flash (same escalation blocks)
| Metric | Value |
|--------|-------|
| Improved blocks | 9 / 15 |
| Unchanged blocks | 2 |
| Degraded blocks | 4 |
| Unreadable recovery | 0 |
| Additional findings | +15 (Flash 11 → engine 21) |
| Additional KV | +5 (Flash 509 → engine 202) |

## Top disagreements (Flash vs engine)
### Rescued blocks (engine found more):  - 9LPD-VX9H-YHK: Flash 0 → engine 3 findings (+3), KV +-4  - RELE-MX3A-MEN: Flash 0 → engine 2 findings (+2), KV +-27  - 46LP-7CN7-GG6: Flash 0 → engine 2 findings (+2), KV +-20  - DTGF-MYHX-PPD: Flash 0 → engine 2 findings (+2), KV +-4  - 7QUU-QDA4-N7D: Flash 0 → engine 2 findings (+2), KV +-3
### Degraded blocks (engine found less):  - 4UTW-PPGP-VEN: Flash 2 → engine 1 findings  - 4WTD-JFKA-JLE: Flash 3 → engine 1 findings  - 4MQJ-6NXP-4YH: Flash 3 → engine 1 findings
### Missing in engine output:  - 9GNP-D7CE-RYM
## Runtime & Cost
| Metric | Value |
|--------|-------|
| Elapsed | 396.2s |
| Avg / median / p95 per-block | 49.00s / 37.33s / 147.97s |
| Total cost USD | $0.8564 |
| Cost/valid block | $0.06117 |
| Cost source actual/est | 14/1 |
