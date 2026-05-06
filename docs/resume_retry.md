# Resume / Retry Policy

**Дата обновления:** 2026-05-01

## Stage Order

```text
prepare / crop / document graph
→ gemma_enrichment
→ text_analysis
→ block_analysis
→ findings_merge
→ review / norms / post-processing / final report
```

Legacy aliases such as `tile_audit` and `main_audit` may map to new stages, but
they must not bypass mandatory prerequisites.

## Mandatory Gates

- `gemma_enrichment` requires Markdown and `_output/blocks_gemma_100/index.json`.
- `text_analysis` cannot run without Markdown; if Gemma summary is invalid,
  resume should run Gemma first.
- `block_analysis` depends on:
  `gemma_enrichment_summary.json`,
  `_output/01_text_analysis.json`,
  `_output/blocks_stage02_100/index.json`.
- `findings_merge` cannot run without `_output/02_blocks_analysis.json`.

Gemma gate is ready when base 100 DPI pass has valid metadata and every block has
a final decision. The gate does not require 300 DPI for every block.

## Retry Modes

Backend Gemma retry supports three paths:

- force base rerun
- force high-detail rerun
- rerun only failed/missing high-detail candidates

If unresolved base failures remain, retry escalates to a full base rerun.
High-detail-only retry is used when base decisions are already valid and only the
optional 300 DPI refinement needs another pass.

## UI/API

`POST /api/audit/{project_id}/retry/gemma_enrichment` uses the same prerequisite
validation as start-from flows. The UI should surface backend errors instead of
offering invalid skips.

Granular retry can be added later in UI without changing the summary contract,
because the backend already records retry mode and per-block final decisions.

## Legacy Project Migration

Projects with old Gemma summary schema (`schema_version` absent or not equal to
`2`) must rerun Gemma enrichment before resume can continue.

If a project has historical completed OCR artifacts (`01_text_analysis.json`,
`02_blocks_analysis.json`, `03_findings.json` or later outputs) but Gemma gate is
not compatible with schema v2, resume detection must not report plain
`completed`. Instead it returns `migration_required = true` and points to the
next migration stage:

- `prepare` if `_output/blocks_gemma_100/index.json` is missing
- `gemma_enrichment` if base crops exist but summary is missing or schema-mismatched

Expected migration result:

- `gemma_gate` reports `schema_mismatch` instead of silently accepting old data
- `resume-info` reports `migration_required` instead of plain `completed`
- rerun strips/replaces old Gemma markdown sections instead of duplicating them
- new base crops appear in `_output/blocks_gemma_100/`
- new summary appears in `_output/gemma_enrichment_summary.json`
