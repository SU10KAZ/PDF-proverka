# Second Pass — Claude (claude-opus-4-7)
Escalation set: 17 blocks
## Completeness
| Metric | Value |
|--------|-------|
| Coverage | 100.0% |
| Missing / Duplicate / Extra | 0 / 0 / 0 |
| Unreadable | 0 |

## Quality Deltas vs Flash (same escalation blocks)
| Metric | Value |
|--------|-------|
| Improved blocks | 15 / 17 |
| Unchanged blocks | 1 |
| Degraded blocks | 1 |
| Unreadable recovery | 0 |
| Additional findings | +46 (Flash 21 → engine 64) |
| Additional KV | +21 (Flash 5207 → engine 345) |

## Top disagreements (Flash vs engine)
### Rescued blocks (engine found more):  - RELE-MX3A-MEN: Flash 0 → engine 5 findings (+5), KV +-155  - 46LP-7CN7-GG6: Flash 0 → engine 4 findings (+4), KV +-77  - 9GNP-D7CE-RYM: Flash 0 → engine 4 findings (+4), KV +2  - 43L7-P9UL-VYD: Flash 0 → engine 4 findings (+4), KV +0  - 4MQJ-6NXP-4YH: Flash 0 → engine 4 findings (+4), KV +1
### Degraded blocks (engine found less):  - 9J9X-DXHT-6GJ: Flash 6 → engine 4 findings
## Runtime & Cost
| Metric | Value |
|--------|-------|
| Elapsed | 264.2s |
| Avg / median / p95 per-block | 30.57s / 32.73s / 46.84s |
| Total cost USD | $0.5446 |
| Cost/valid block | $0.03204 |
| Cost source actual/est | 17/0 |
