# Pipeline Manager Split Plan

**Date:** 2026-05-06

## Goal

Reduce `backend/app/pipeline/manager.py` (7277 lines) by extracting pure/static
helper functions into stage-specific runner modules without changing any business
logic, JSON formats, or pipeline behaviour.

## Functions to Extract

### Target: `backend/app/pipeline/stages/crop_blocks/runner.py`

| Original name in manager.py | New name in runner.py | Risk |
|-----------------------------|----------------------|------|
| `_build_crop_args` (top-level) | `build_crop_args` | Low — pure, only uses standard lib + gemma_enrichment_contract |
| `_existing_crop_matches_policy` (top-level) | `existing_crop_matches_policy` | Low — thin wrapper around `crop_index_matches_policy` |
| `_crop_policy_label` (top-level) | `crop_policy_label` | Low — pure string format |

Dependencies: `backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract`

### Target: `backend/app/pipeline/stages/block_analysis/runner.py`

| Original name in manager.py | New name in runner.py | Risk |
|-----------------------------|----------------------|------|
| `_expand_block_batches_for_local_model` (top-level) | `expand_block_batches_for_single_block_mode` | Low — pure data transform |
| `_build_single_block_runtime_plan` (top-level) | `build_single_block_runtime_plan` | Low — calls expand_* |
| `_write_single_block_runtime_plan` (top-level) | `write_single_block_runtime_plan` | Low — calls build_*, writes JSON |
| `_load_or_create_single_block_runtime_plan` (top-level) | `load_or_create_single_block_runtime_plan` | Low — reads/writes JSON |
| `_runtime_batch_failure_entry` (top-level) | `runtime_batch_failure_entry` | Low — pure dict builder |
| `_write_block_analysis_runtime_summary` (top-level) | `write_block_analysis_runtime_summary` | Low — writes JSON |
| `_attach_stage02_coverage_to_findings` (@staticmethod) | `attach_stage02_coverage_to_findings` | Medium — reads/writes multiple JSON files |
| `_validate_and_repair_json` (@staticmethod) | `validate_and_repair_json` | Medium — pure file JSON repair, also used by findings_merge |

Dependencies: `json`, `pathlib.Path`, `datetime`, `backend.app.services.common.project_service`

### Target: `backend/app/pipeline/stages/findings_merge/runner.py`

| Original name in manager.py | New name in runner.py | Risk |
|-----------------------------|----------------------|------|
| `_backfill_text_evidence_in_findings` (@staticmethod) | `backfill_text_evidence_in_findings` | Medium — reads/writes 03_findings.json |
| `_refresh_finding_quality` (@staticmethod) | `refresh_finding_quality` | Low — delegates to finding_quality service |
| `_merge_similar_findings` (@staticmethod) | `merge_similar_findings` | Medium — reads/writes 03_findings.json |

Dependencies: `backend.app.services.findings`, `backend.app.services.common.project_service`

### Target: `backend/app/pipeline/stages/norms/runner.py`

| Original name in manager.py | New name in runner.py | Risk |
|-----------------------------|----------------------|------|
| `_enrich_norm_quotes_from_checks` (@staticmethod) | `enrich_norm_quotes_from_checks` | Low — try/except import of bare `norms` module |
| `_fix_paragraph_refs` (@staticmethod) | `fix_paragraph_refs` | Low — pure JSON manipulation with regex |
| `_count_manual_check_flags` (@staticmethod) | `count_manual_check_flags` | Low — pure JSON read |

Dependencies: `json`, `re`, `shutil`, `pathlib.Path`, bare `norms` import (try/except)

## Stub-Only Targets (no functions to move yet)

- `backend/app/pipeline/stages/text_analysis/runner.py`
- `backend/app/pipeline/stages/findings_review/runner.py`
- `backend/app/pipeline/stages/optimization/runner.py`
- `backend/app/pipeline/stages/report/runner.py`

## Functions Explicitly Skipped

| Function | Reason |
|----------|--------|
| `_backfill_highlight_regions` | Has bare `from backfill_highlights import` — skip per task spec |
| `_extract_error_detail` | Useful helper, not assigned to specific stage |
| `_project_path` | Uses `resolve_project_dir` + `BASE_DIR`, not purely stage-logic |

## Backwards Compatibility Strategy

- Top-level functions in manager.py: replace body with import alias
- `@staticmethod` methods in `PipelineManager`: replace body with call to imported function
- All original names preserved as aliases/wrappers — zero call-site changes

## Verification

```bash
python -m compileall backend -q
python - <<'PY'
import backend.app.main
import backend.app.pipeline.manager
import backend.app.pipeline.stages.text_analysis.runner
import backend.app.pipeline.stages.findings_merge.runner
import backend.app.pipeline.stages.findings_review.runner
import backend.app.pipeline.stages.optimization.runner
import backend.app.pipeline.stages.block_analysis.runner
import backend.app.pipeline.stages.norms.runner
import backend.app.pipeline.stages.crop_blocks.runner
print("OK")
PY
```
