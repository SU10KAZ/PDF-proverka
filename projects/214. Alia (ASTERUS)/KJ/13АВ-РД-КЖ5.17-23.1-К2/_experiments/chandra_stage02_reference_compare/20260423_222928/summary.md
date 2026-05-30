# Chandra Stage02 Reference Compare

- Model: `qwen/qwen3.6-35b-a3b`
- Image max side: `1024`
- Blocks: `3`
- Avg elapsed: `8.64` sec/block
- Verdict counts: `{'fail_parse': 2, 'equivalent': 1}`

| Block | Verdict | Ref findings | Cand findings | Ref KV | Cand KV | Notes |
|---|---|---:|---:|---:|---:|---|
| `3TFL-TNRF-7G6` | `fail_parse` | 1 | 0 | 55 | 0 | candidate JSON parse failed |
| `947C-9UJT-RYU` | `fail_parse` | 1 | 0 | 23 | 0 | candidate JSON parse failed |
| `9PPJ-YP6U-HV6` | `equivalent` | 3 | 2 | 14 | 10 | candidate preserved drawing type and findings presence |
