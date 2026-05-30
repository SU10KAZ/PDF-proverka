# Phase A — Model Quality Side-by-Side (OpenRouter)

| Metric | 2.5 Flash | 3.1 Pro |
|--------|-----------|---------|
| Model | google/gemini-2.5-flash | google/gemini-3.1-pro-preview |
| Coverage % | 100.0% | 98.3% |
| Missing blocks | 0 | 1 |
| Duplicate blocks | 0 | 0 |
| Extra blocks | 0 | 0 |
| Inferred block_ids | 0 | 0 |
| Unreadable blocks | 0 | 0 |
| Empty summary | 0 | 0 |
| Blocks w/ findings | 12 | 49 |
| Total findings | 38 | 92 |
| Findings/100 blocks | 63.3 | 153.3 |
| Median KV count | 19.0 | 12.0 |
| Total KV count | 1869 | 806 |
| Prompt tokens | 181124 | 148268 |
| Output tokens | 41268 | 292392 |
| Reasoning tokens | 0 | 249878 |
| Cached tokens | 0 | 0 |
| Total cost USD | $0.1575 | $3.8052 |
| Cost/valid block | $0.00263 | $0.06450 |
| Cost/finding | $0.00415 | $0.04136 |
| Elapsed (s) | 158.1 | 1091.3 |
| Avg batch dur (s) | 7.75 | 53.83 |
| Retry count | 0 | 0 |
