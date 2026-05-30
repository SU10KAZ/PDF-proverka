# Phase A — Model Quality Summary (OpenRouter single-block subset)

Chosen mainline: **google/gemini-3.1-pro-preview**

| Metric | google/gemini-2.5-flash | google/gemini-3.1-pro-preview |
|--------|-------------------------|-------------------------------|
| Coverage | 100.0% | 98.33% |
| Missing | 0 | 1 |
| Duplicates | 0 | 0 |
| Extra | 0 | 0 |
| Inferred block_id | 0 | 0 |
| Unreadable | 0 | 0 |
| Blocks with findings | 12 | 49 |
| Total findings | 38 | 92 |
| Findings/100 | 63.33 | 153.33 |
| Median KV | 19.0 | 12.0 |
| Total KV | 1869 | 806 |
| Prompt tokens | 181124 | 148268 |
| Output tokens | 41268 | 292392 |
| Reasoning tokens | 0 | 249878 |
| Cached tokens | 0 | 0 |
| Total cost USD | $0.1575 | $3.8052 |
| Cost/valid block | $0.00263 | $0.06450 |
| Cost/finding | $0.00415 | $0.04136 |
| Elapsed (s) | 158.1 | 1091.3 |
| Avg batch dur (s) | 7.75 | 53.83 |
| P95 batch dur (s) | 22.76 | 115.41 |
| Retry count | 0 | 0 |
| Provider errors | 0 | 0 |
| Cost source actual | 60/60 | 59/60 |
