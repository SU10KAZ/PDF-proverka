# Phase 1 prompts — scaffolding (NOT wired)

Production-ready prompts for the Stage 01 completeness lens. **Not yet
consumed by any pipeline stage.** Copied byte-identically from
`experiments/md_analysis_comparison/production_preparation/prompts/`
at Phase 1 Step 0.3.

Sub-directory isolation: existing loaders that glob
`prompts/pipeline/ru/*_task.md` will not pick these up. The future
completeness-lens runner will reach them via
`backend.app.services.text_analysis.prompt_loader` (see
[../../../../backend/app/services/text_analysis/prompt_loader.py](../../../../backend/app/services/text_analysis/prompt_loader.py)).

## Manifest

| File | Lines | Placeholders | Purpose |
|---|---:|---|---|
| `completeness_lens_production_prompt.md` | 184 | `{DISCIPLINE}`, `{DOCUMENT_TYPE}`, `{CHECKLIST_CONTENT}`, `{MD_CONTENT}` | Standalone Sonnet completeness-lens prompt (Balanced Engineering v3). Run as a parallel LLM leg next to current_method. |
| `stage01_document_type_block.md`         | 136 | `{DOCUMENT_TYPE}` | Reusable document-type routing block, embedded by other Stage 01 prompts. |
| `stage01_few_shot_examples.md`           | 366 | — | Static few-shot examples (no placeholders). |
| `stage01_production_prompt.md`           | 208 | `{PROJECT_ID}`, `{DISCIPLINE}`-family, `{DOCUMENT_TYPE}`, `{MD_FILE_PATH}`, `{OUTPUT_PATH}`, … | v2 Stage 01 prompt with discipline-aware role, checklist injection, and document-type routing. |
| `stage01_severity_calibration.md`        | 213 | — | Severity-mapping reference block (no placeholders). |

## Placeholder convention

Single-brace `{NAME}`, matching the existing pipeline prompts
(`prompts/pipeline/ru/*_task.md`). The Phase 1 loader exposes
`validate_placeholders(text)` to enumerate placeholders present, but does
**not** substitute — substitution stays the runner's job.

## Sources of truth

| Concern | File |
|---|---|
| Prompt text                | this directory (immutable copy of `production_preparation/prompts/`) |
| Discipline checklists      | `backend/app/data/discipline_checklists/` (copied at Step 0.2) |
| Document-type detector     | `backend/app/services/text_analysis/document_type_detector.py` (Step 0.1) |
| Severity/applicability rules | `experiments/md_analysis_comparison/production_preparation/checklists/checklist_rules.md` (design doc, not runtime data) |
| Rollout plan / flags       | `experiments/md_analysis_comparison/production_preparation/rollout/phase1_rollout.md` |

## Editing policy

While Phase 1 is in scaffolding mode (no wiring, all flags OFF), changes
here are no-ops at runtime. Once wired, edits to these files change lens
behaviour at runtime — treat them like the rest of `prompts/pipeline/ru/`.

After editing:

```bash
python -m pytest tests/text_analysis/test_prompt_loader.py -v
```

This re-checks placeholder inventories and minimum sizes.
