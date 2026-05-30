# Phase R1 — Flash Single-Block Full Project (Recall-Oriented)

Model: **google/gemini-2.5-flash** | Mode: **single_block** | Parallelism: 3

## Coverage & Completeness
| Metric | Value |
|--------|-------|
| Total input blocks | 25 |
| Coverage | 100.0% |
| Missing / Duplicate / Extra | 0 / 0 / 0 |
| Inferred block_id | 0 |
| Unreadable | 0 |

## Quality Signals
| Metric | Value |
|--------|-------|
| Risk heavy/normal/light | 6/8/11 |
| Empty summary | 0 |
| Empty key_values | 0 |
| Blocks with findings | 6 |
| Total findings | 21 |
| Findings/100 blocks | 84.0 |
| Total / median KV | 5277 / 13.0 |

## Escalation Tiers (Recall-Oriented)
| Tier | Count | Pct |
|------|-------|-----|
| tier1 mandatory second pass | 17 | 68% |
| tier2 recommended second pass | 0 | 0% |
| tier3 flash-only ok | 8 | 32% |
| **Total for second pass** | **17** | **68%** |

## Runtime & Cost
| Metric | Value |
|--------|-------|
| Elapsed | 125.1s |
| Avg / median / p95 per-block | 11.51s / 5.83s / 18.58s |
| Prompt / completion / reasoning / cached tokens | 53200 / 49401 / 0 / 401 |
| Total cost USD | $0.1394 |
| Cost/valid block | $0.00557 |
| Cost/finding | $0.00664 |
| Cost source actual/est | 25/0 |
