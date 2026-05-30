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
| Blocks with findings | 5 |
| Total findings | 11 |
| Findings/100 blocks | 44.0 |
| Total / median KV | 597 / 12.0 |

## Escalation Tiers (Recall-Oriented)
| Tier | Count | Pct |
|------|-------|-----|
| tier1 mandatory second pass | 15 | 60% |
| tier2 recommended second pass | 0 | 0% |
| tier3 flash-only ok | 10 | 40% |
| **Total for second pass** | **15** | **60%** |

## Runtime & Cost
| Metric | Value |
|--------|-------|
| Elapsed | 66.4s |
| Avg / median / p95 per-block | 7.72s / 5.54s / 23.24s |
| Prompt / completion / reasoning / cached tokens | 64534 / 14844 / 0 / 0 |
| Total cost USD | $0.0565 |
| Cost/valid block | $0.00226 |
| Cost/finding | $0.00513 |
| Cost source actual/est | 25/0 |
