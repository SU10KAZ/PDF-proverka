# Chandra Stage02 Reference Compare

- Model: `qwen/qwen3.6-35b-a3b`
- Image max side: `1024`
- Blocks: `12`
- Avg elapsed: `7.68` sec/block
- Verdict counts: `{'fail_parse': 4, 'likely_degraded': 5, 'equivalent': 3}`

| Block | Verdict | Ref findings | Cand findings | Ref KV | Cand KV | Notes |
|---|---|---:|---:|---:|---:|---|
| `3TFL-TNRF-7G6` | `fail_parse` | 1 | 0 | 55 | 0 | candidate JSON parse failed |
| `66NE-7DYY-GQN` | `fail_parse` | 1 | 0 | 46 | 0 | candidate JSON parse failed |
| `9DH3-MEUR-DR4` | `fail_parse` | 3 | 0 | 19 | 0 | candidate JSON parse failed |
| `947C-9UJT-RYU` | `fail_parse` | 1 | 0 | 23 | 0 | candidate JSON parse failed |
| `9PPJ-YP6U-HV6` | `likely_degraded` | 3 | 0 | 14 | 5 | reference has findings, candidate has none |
| `97AH-VUFP-LJJ` | `likely_degraded` | 5 | 0 | 24 | 5 | reference has findings, candidate has none |
| `4CFG-LM4Y-7H4` | `equivalent` | 1 | 1 | 23 | 10 | candidate preserved drawing type and findings presence |
| `66M4-Y69W-VCG` | `equivalent` | 2 | 1 | 14 | 7 | candidate preserved drawing type and findings presence |
| `6PPA-4DDX-6FR` | `equivalent` | 2 | 2 | 13 | 7 | candidate preserved drawing type and findings presence |
| `6R7N-7LRD-AUR` | `likely_degraded` | 2 | 0 | 27 | 26 | reference has findings, candidate has none |
| `7DJ9-EQQ3-QMK` | `likely_degraded` | 3 | 2 | 38 | 9 | candidate key_values_read much shorter than reference |
| `6P9T-Q7GT-N9J` | `likely_degraded` | 4 | 0 | 32 | 10 | reference has findings, candidate has none |
