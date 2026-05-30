# Chandra Stage02 Reference Compare

- Model: `qwen/qwen3.6-35b-a3b`
- Image max side: `1024`
- Blocks: `7`
- Avg elapsed: `9.57` sec/block
- Verdict counts: `{'fail_parse': 3, 'equivalent': 4}`

| Block | Verdict | Ref findings | Cand findings | Ref KV | Cand KV | Notes |
|---|---|---:|---:|---:|---:|---|
| `3TFL-TNRF-7G6` | `fail_parse` | 1 | 0 | 55 | 0 | candidate JSON parse failed |
| `947C-9UJT-RYU` | `fail_parse` | 1 | 0 | 23 | 0 | candidate JSON parse failed |
| `9PPJ-YP6U-HV6` | `fail_parse` | 3 | 0 | 14 | 0 | candidate JSON parse failed |
| `97AH-VUFP-LJJ` | `equivalent` | 5 | 3 | 24 | 13 | candidate preserved drawing type and findings presence |
| `6R7N-7LRD-AUR` | `equivalent` | 2 | 3 | 27 | 15 | candidate preserved drawing type and findings presence |
| `7DJ9-EQQ3-QMK` | `equivalent` | 3 | 3 | 38 | 20 | candidate preserved drawing type and findings presence |
| `6P9T-Q7GT-N9J` | `equivalent` | 4 | 3 | 32 | 15 | candidate preserved drawing type and findings presence |
