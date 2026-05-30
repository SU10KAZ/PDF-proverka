# Chandra Stage02 Reference Compare

- Model: `qwen/qwen3.5-35b-a3b`
- Image max side: `1024`
- Blocks: `12`
- Avg elapsed: `7.9` sec/block
- Verdict counts: `{'likely_degraded': 8, 'equivalent': 4}`

| Block | Verdict | Ref findings | Cand findings | Ref KV | Cand KV | Notes |
|---|---|---:|---:|---:|---:|---|
| `3TFL-TNRF-7G6` | `likely_degraded` | 1 | 3 | 55 | 9 | candidate key_values_read much shorter than reference |
| `66NE-7DYY-GQN` | `likely_degraded` | 1 | 0 | 46 | 11 | reference has findings, candidate has none |
| `9DH3-MEUR-DR4` | `likely_degraded` | 3 | 0 | 19 | 19 | reference has findings, candidate has none |
| `947C-9UJT-RYU` | `likely_degraded` | 1 | 0 | 23 | 8 | reference has findings, candidate has none |
| `9PPJ-YP6U-HV6` | `likely_degraded` | 3 | 0 | 14 | 8 | reference has findings, candidate has none |
| `97AH-VUFP-LJJ` | `equivalent` | 5 | 3 | 24 | 12 | candidate preserved drawing type and findings presence |
| `4CFG-LM4Y-7H4` | `equivalent` | 1 | 1 | 23 | 13 | candidate preserved drawing type and findings presence |
| `66M4-Y69W-VCG` | `likely_degraded` | 2 | 0 | 14 | 10 | reference has findings, candidate has none |
| `6PPA-4DDX-6FR` | `equivalent` | 2 | 1 | 13 | 10 | candidate preserved drawing type and findings presence |
| `6R7N-7LRD-AUR` | `likely_degraded` | 2 | 0 | 27 | 11 | reference has findings, candidate has none |
| `7DJ9-EQQ3-QMK` | `likely_degraded` | 3 | 0 | 38 | 15 | reference has findings, candidate has none |
| `6P9T-Q7GT-N9J` | `equivalent` | 4 | 1 | 32 | 20 | candidate preserved drawing type and findings presence |
