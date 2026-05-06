# Gemma OCR Enrichment

**Дата обновления:** 2026-05-01

Gemma enrichment is a mandatory OCR-audit stage between crop/document graph and
Stage 01 text analysis.

```text
Markdown PDF representation
→ Gemma base OCR enrichment, 100 DPI, fast stable pass
→ optional Gemma high-detail retry, 300 DPI only for safe small/medium text-heavy blocks
→ Stage 01 text analysis
→ Stage 02 findings_only_gemma_pair + GPT-5.4 using 100 DPI Stage 02 crops
→ merge/review/norms/final report
```

## Crop Sources Of Truth

- Gemma base pass: `_output/blocks_gemma_100/`
- Gemma high-detail retry: `_output/blocks_gemma_300/`
- Stage 02 image input: `_output/blocks_stage02_100/`

Production must not treat generic `_output/blocks/` as the source of truth for
Gemma or Stage 02.

## Crop Policies

```json
{
  "gemma_base_crop_policy": {
    "profile": "gemma_100_base",
    "dpi": 100,
    "min_long_side": 800,
    "compact": false,
    "skip_small": false
  },
  "gemma_high_detail_crop_policy": {
    "profile": "gemma_300_high_detail",
    "dpi": 300,
    "min_long_side": 800,
    "compact": false,
    "skip_small": false
  },
  "stage02_crop_policy": {
    "profile": "stage02_100",
    "dpi": 100,
    "min_long_side": 800,
    "compact": false
  }
}
```

## High-Detail Retry

Base Gemma runs on all image blocks at 100 DPI. High-detail 300 DPI is used only
for targeted retry candidates:

- base result is empty or too short;
- base result contains low-readability markers such as `unreadable`,
  `нечитаемо`, `неразборчиво`, `blurred`, `cannot read`, `text too small`,
  `partially readable`;
- block metadata suggests a table/spec/schedule/statement or dense OCR-heavy
  small text;
- base returned only a reasoning tail / partial OCR salvage.

Safety limits before sending a 300 DPI crop to Gemma:

- `size_kb_300 <= 300`
- `long_side_300 <= 3500`
- `estimated_image_tokens <= 3500`

If a candidate exceeds safety limits, it is recorded as
`high_detail_status = "skipped_large_block"` and the pipeline falls back to base
100 DPI if that base enrichment exists.

## Reasoning Fallback

Gemma runner reads `content` first. If `content` is empty, it falls back to
`reasoning_content` and extracts the best final chunk from:

- `Final check`
- `final answer`
- `ЧИТАЕМО`
- the last structured bullet/list tail
- otherwise the last ~1000 chars of reasoning

If `finish_reason = length` but the reasoning tail still contains useful OCR, the
result is stored as `partial_ok` instead of a full failure.

Per-block metadata now records:

- `response_source = content | reasoning_content | reasoning_tail`
- `finish_reason = stop | length | error`

## Summary Contract

`_output/gemma_enrichment_summary.json` is the source of truth for skip/resume:

- `schema_version = 2`
- `base_profile`, `base_crop_policy`, `base_blocks_index_hash`
- `high_detail_profile`, `high_detail_crop_policy`, `high_detail_blocks_index_hash`
- `md_hash_before_enrichment`
- `blocks_total`, `base_blocks_ok`
- `high_detail_candidates`, `high_detail_ok`, `high_detail_skipped_large`
- `blocks_failed`, `coverage_ratio`
- `uncovered_block_ids`, `large_block_skipped_ids`
- per-block `base_status`, `high_detail_status`, `final_profile`, `coverage_status`

The Markdown marker is human-readable only. Skip/resume must be based on the
summary hashes and final block decisions.

## Gate / Resume Semantics

Gemma stage is considered ready when:

- base 100 DPI pass has been executed;
- summary metadata and hashes are valid;
- every image block has a final decision:
  `ok`, `partial`, `high_detail_skipped_large_block`, or
  `missing_gemma_enrichment`.

The gate does not require 300 DPI for every block.

## Runtime LM Studio Policy

Production runtime must not change LM Studio model config during a stage or job:

- no automatic `load` / `unload` / `reload` while Gemma base or high-detail is running
- no runtime `context_length` changes
- no runtime `parallel` changes
- no runtime reasoning toggles such as `/no_think`, `reasoning=false`, or
  `reasoning="off"`

Gemma base 100 DPI and high-detail 300 DPI must reuse the same already-loaded
model instance. The only thing that changes between passes is crop profile and
backend request concurrency.

## Recommended LM Studio Settings

Do not try to disable reasoning programmatically; treat operator-side LM Studio
configuration as the real control surface.

Recommended operator settings:

- `context_length: 16000` or the largest stable value available
- `parallel: 4` for the base 100 DPI pass
- `parallel: 1` preferred for targeted 300 DPI retry
- if backend cannot switch parallelism per stage, keep the 300 DPI safety
  thresholds strict

Do not change `chandra-ocr-2` or unrelated loaded models without an explicit
reason.

## Post-Queue Cleanup

After the whole queue becomes idle, AuditManager may run best-effort cleanup:

- wait a grace period before unloading
- re-check that prepare/audit/retry/resume queues are still idle
- unload only Gemma models from the allowlist
- never unload denylist models such as `chandra-ocr-2`
- unload failures must stay warnings only and must not fail the audit

## Schema v1 Migration

Older projects with missing `schema_version` or `schema_version != 2` are no
longer valid for skip/resume. They must rerun `gemma_enrichment`.

Historical `03_findings.json`, reports, and other completed artifacts may stay
on disk, but they are treated as legacy outputs until Gemma schema v2 is rebuilt.
Resume/status detection should expose this as `migration_required` rather than
showing the project as fully compatible with the new production architecture.

After rerun, the project must have:

- `_output/blocks_gemma_100/index.json`
- `_output/gemma_enrichment_summary.json` with `schema_version = 2`
- `_output/blocks_gemma_300/index.json` only if high-detail candidates exist
- `_output/blocks_stage02_100/index.json` before Stage 02 starts
