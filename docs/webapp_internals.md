# Webapp Internals

FastAPI + Vue 3 SPA (без сборки, CDN). Слушает **127.0.0.1:8081**.

## Структура

`main.py` → `routers/` → `services/` → `models/`.

Ключевые сервисы:

- `pipeline_service.py` — orchestration и progress/resume/retry
- `gemma_gate.py` — readiness validation для обязательного Gemma enrichment
- `prepare_service.py` — queue для crop + Gemma enrichment
- `lmstudio_lifecycle_service.py` — post-queue cleanup policy for local Gemma
- `usage_service.py` — usage/cost tracking
- `ws/manager.py` — WebSocket live-лог

## Production Pipeline

```text
Markdown PDF representation
→ Gemma base OCR enrichment, 100 DPI, fast stable pass
→ optional Gemma high-detail retry, 300 DPI only for safe small/medium text-heavy blocks
→ Stage 01 text analysis
→ Stage 02 findings-only single-block analysis using GPT-5.4
→ merge/review/norms/final report
```

Markdown is required. `document_graph.extracted_text` is not a Stage 01 fallback.

## Crop Validation Split

- Gemma base validation uses `_output/blocks_gemma_100/index.json`
- optional high-detail validation uses `_output/blocks_gemma_300/index.json`
- Stage 02 image input uses `_output/blocks_stage02_100/index.json`
- single-block Stage 02 runtime plan uses `_output/block_batches.runtime.json`

The webapp must not treat `_output/blocks/` as the production truth for any of
these stages.

## Gemma Gate Semantics

`gemma_gate.py` treats the stage as ready when:

- Markdown exists
- base crop/source hash is valid
- Markdown hash is valid
- per-block final decisions are present

Ready status may still be `partial` if some blocks are uncovered or if
high-detail was skipped for oversized candidates. Those warnings are surfaced in
Stage 02 summaries and final coverage sections.

## Coverage Propagation

`pipeline_service.py` attaches deterministic coverage metadata from Stage 02 to
`03_findings.json`, including:

- uncovered Gemma blocks
- single-block failures
- Stage 02 crop mismatches
- base 100 DPI only blocks
- blocks upgraded to 300 DPI

This keeps final reports honest even when optional high-detail retry is skipped
or some blocks remain partially covered.

## LM Studio Lifecycle

Runtime policy:

- pipeline does not change LM Studio `context_length`, `parallel`, or reasoning
  config during stage/job execution
- `llm_runner.py` may detect context mismatch, but auto-reload is disabled by
  default and only returns a diagnostic warning/error
- Gemma base 100 DPI and high-detail 300 DPI reuse the same preloaded model
  instance

Cleanup policy:

- after prepare/audit/retry/resume queues are all idle, webapp schedules
  best-effort unload after a grace period
- only allowlisted Gemma models are unloaded
- denylist models, especially `chandra-ocr-2`, are never touched
- unload failures are warnings and must not mutate job/project status
