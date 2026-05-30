# Chandra Stage02 Reference Compare

- Model: `qwen/qwen3.6-35b-a3b`
- Image max side: `1024`
- Blocks: `2`
- Avg elapsed: `11.73` sec/block
- Verdict counts: `{'fail_parse': 1, 'equivalent': 1}`

| Block | Verdict | Ref findings | Cand findings | Ref KV | Cand KV | Notes |
|---|---|---:|---:|---:|---:|---|
| `3TFL-TNRF-7G6` | `fail_parse` | 1 | 0 | 55 | 0 | candidate JSON parse failed |
| `947C-9UJT-RYU` | `equivalent` | 1 | 2 | 23 | 22 | candidate preserved drawing type and findings presence |
