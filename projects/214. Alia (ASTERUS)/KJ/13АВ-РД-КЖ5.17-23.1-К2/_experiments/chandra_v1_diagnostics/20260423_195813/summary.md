# Chandra V1 Diagnostics

- Project: `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf`
- API: `/api/v1/chat` with `reasoning=off`
- Context length: `8192`

## Prepared Images

| Name | Block | Size | File KB |
|---|---|---:|---:|
| `light_512` | `A9M4-EJ7R-MKH` | 512x267 | 26.0 |
| `light_1024` | `A9M4-EJ7R-MKH` | 1024x534 | 67.8 |
| `heavy_512` | `3TFL-TNRF-7G6` | 512x487 | 67.0 |

## Results

| Model | Test | Verdict | OK | Elapsed s | Content excerpt |
|---|---|---|---:|---:|---|
| `google/gemma-4-31b` | `load` | `` | True | 9.052 |  |
| `google/gemma-4-31b` | `text` | `pass` | True | 1.107 | OK |
| `google/gemma-4-31b` | `light_512` | `fail` | False | 1.441 | {"message": "'input.0.content' is required, Unrecognized key(s) in object: 'text'", "type": "invalid_request", "code": " |
| `qwen/qwen3.6-35b-a3b` | `load` | `` | True | 9.922 |  |
| `qwen/qwen3.6-35b-a3b` | `text` | `pass` | True | 1.257 | OK |
| `qwen/qwen3.6-35b-a3b` | `light_512` | `fail` | False | 2.216 | {"message": "'input.0.content' is required, Unrecognized key(s) in object: 'text'", "type": "invalid_request", "code": " |
| `qwen/qwen3.6-27b` | `load` | `` | True | 10.241 |  |
| `qwen/qwen3.6-27b` | `text` | `pass` | True | 1.103 | OK |
| `qwen/qwen3.6-27b` | `light_512` | `fail` | False | 1.485 | {"message": "'input.0.content' is required, Unrecognized key(s) in object: 'text'", "type": "invalid_request", "code": " |
