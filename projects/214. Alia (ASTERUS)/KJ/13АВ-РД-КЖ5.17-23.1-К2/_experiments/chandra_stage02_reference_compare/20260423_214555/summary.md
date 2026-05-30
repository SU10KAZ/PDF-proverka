# Chandra Stage02 Reference Compare

- Model: `qwen/qwen3.6-35b-a3b`
- Image max side: `1024`
- Blocks: `4`
- Avg elapsed: `10.15` sec/block
- Verdict counts: `{'fail_parse': 1, 'uncertain': 1, 'likely_improved': 1, 'likely_degraded': 1}`

| Block | Verdict | Ref findings | Cand findings | Ref KV | Cand KV | Notes |
|---|---|---:|---:|---:|---:|---|
| `3TFL-TNRF-7G6` | `fail_parse` | 1 | 0 | 55 | 0 | candidate JSON parse failed |
| `66NE-7DYY-GQN` | `uncertain` | 1 | 3 | 46 | 20 | candidate produced more findings than reference; may be improved or inflated |
| `9DH3-MEUR-DR4` | `likely_improved` | 3 | 3 | 19 | 20 | candidate preserved findings presence and captured more key values |
| `947C-9UJT-RYU` | `likely_degraded` | 1 | 0 | 23 | 20 | reference has findings, candidate has none |
