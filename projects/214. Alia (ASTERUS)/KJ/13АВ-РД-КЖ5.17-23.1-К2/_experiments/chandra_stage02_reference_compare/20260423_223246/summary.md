# Chandra Stage02 Reference Compare

- Model: `qwen/qwen3.6-35b-a3b`
- Image max side: `1024`
- Blocks: `1`
- Avg elapsed: `11.25` sec/block
- Verdict counts: `{'fail_parse': 1}`

| Block | Verdict | Ref findings | Cand findings | Ref KV | Cand KV | Notes |
|---|---|---:|---:|---:|---:|---|
| `3TFL-TNRF-7G6` | `fail_parse` | 1 | 0 | 55 | 0 | candidate JSON parse failed |
