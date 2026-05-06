# Stage 02 — Production Profile

**Дата обновления:** 2026-05-01
**Production architecture:** `findings_only_gemma_pair + openai/gpt-5.4`

## Pipeline

```text
Markdown PDF representation
→ Gemma base OCR enrichment, 100 DPI, fast stable pass
→ optional Gemma high-detail retry, 300 DPI only for safe small/medium text-heavy blocks
→ Stage 01 text analysis
→ Stage 02 findings-only single-block analysis using GPT-5.4
→ merge/review/norms/final report
```

## Production Defaults

| Параметр | Значение |
|----------|----------|
| Stage 02 model | `openai/gpt-5.4` |
| Stage 02 batch mode | `findings_only_gemma_pair` |
| Runtime mode | `single_block` |
| Runtime plan | `_output/block_batches.runtime.json` |
| Gemma base crops | `_output/blocks_gemma_100/`, 100 DPI, `min_long_side=800` |
| Gemma high-detail crops | `_output/blocks_gemma_300/`, 300 DPI, only for selected safe candidates |
| Stage 02 crops | `_output/blocks_stage02_100/`, 100 DPI, `min_long_side=800` |
| Required before Stage 02 | Markdown, valid `gemma_enrichment_summary.json`, `01_text_analysis.json` |

## Validation Rules

- Gemma enrichment is a mandatory stage, not a selectable Stage 02 model.
- Stage 02 reads the best final Gemma enrichment per block:
  `gemma_300_high_detail` if successful, otherwise `gemma_100_base`.
- Missing Gemma enrichment must become
  `coverage_status = "missing_gemma_enrichment"`, never “замечаний нет”.
- Partial Gemma coverage is allowed only when it is explicitly reflected in
  `gemma_enrichment_summary.json`, `02_blocks_analysis.json` and final coverage
  sections.
- Stage 02 image input comes from `_output/blocks_stage02_100/`; it must not
  read, overwrite or validate against generic `_output/blocks/`.

## LM Studio Recommendations

- `context_length: 16000` or the largest stable value available
- `parallel: 4` for the base 100 DPI Gemma pass
- `parallel: 1` preferred for targeted 300 DPI retry, but this is an operator
  recommendation for a separate load profile, not something the runtime backend
  should flip automatically between passes
- if backend cannot switch parallelism per stage, keep 300 DPI safety thresholds strict

Runtime pipeline must not reconfigure LM Studio during stage/job execution:

- no runtime `load` / `reload` / `unload`
- no runtime `context_length` changes
- no runtime `parallel` changes
- no runtime reasoning toggles

Treat operator-side LM Studio configuration as the real control surface.

## Post-Queue Cleanup

After the whole queue finishes, AuditManager may do best-effort cleanup:

- unload only allowlisted Gemma models
- never unload denylist models such as `chandra-ocr-2`
- do not unload after every project; unload only after the full queue is idle
- unload failure is a warning and must not fail the audit

## Coverage Fields

Stage 02 runtime summary and `03_findings.json` coverage sections must include:

- base Gemma coverage
- high-detail candidates
- high-detail successful
- high-detail skipped large
- uncovered blocks
- blocks analyzed only with 100 DPI base
- blocks upgraded to 300 DPI
