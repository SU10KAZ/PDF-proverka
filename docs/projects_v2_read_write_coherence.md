# projects_v2 READ ↔ WRITE coherence

Дата: 2026-06-23. Scope: `fix/audit-queue-rate-limit-md-resolution`, без правок deploy `read_canary.py`.

## Контракт deploy read_canary

| read_canary v2 function | v2 source path / structure | response shape notes | write-side status |
|---|---|---|---|
| `v2_projects_list` | `objects/*/disciplines/*/documents/*/document.json`, current version metadata, latest/runs analysis summary | legacy-like `{projects, object_name, storage_backend, canary}` | OK: `StorageWriteFacade._ensure_document_scaffold()` writes visible `document.json`, `versions`, `version_ids`, `current_version.txt`. |
| `v2_project_details` | document scaffold + `01_input`, `03_analysis/latest`, runs fallback | `ProjectStatus` fields + `storage_backend/canary` | OK. |
| `v2_project_versions` | `document.json.versions`, `current_version.txt`, per-version `version.json` | legacy versions summary, physical `v001` denormalized where needed | OK: version create/upload path writes physical `vNNN`. |
| `v2_version_files` | `versions/<vid>/01_input/**` only | `{project_id, version_id, file_count, files, storage_backend, canary}` | Fixed on fix: public listing now follows `01_input` originals; `02_work` remains internal canonical source. |
| `v2_findings` | priority latest/runs: `03a_norms_verified.json`, `03_findings.json`, `03_findings_pre_merge.json` | top-level findings list/count/severity | OK: analysis artifacts write latest + run when `run_id` exists. |
| `v2_finding_by_id` | same findings source as `v2_findings` | finding fields at top level + `storage_backend/canary` | OK. |
| `v2_blocks_analysis` | `03_analysis/latest/02_blocks_analysis.json`, fallback runs; `blocks/index.json` and batches where available | legacy block-analysis summary + canary fields | OK for JSON artifact; blocks image/index bridge fixed below. |
| `v2_blocks` | `03_analysis/latest/blocks/index.json`, fallback newest `runs/*/blocks/index.json` | `{project_id, document_code, version_id, total_blocks, pages, ...}` | Fixed on fix: v2 crop writes read-compatible `blocks/` alias next to `blocks_gemma_100`. |
| `v2_block_image` | same `blocks/` directory; `file` from index or `block_<id>.png` | `FileResponse` | Fixed by the same `blocks/` alias. |
| `v2_block_map` | findings helpers + `02_blocks_analysis`, `blocks/index.json`, document graph, OCR index | `{block_map, block_info, text_evidence, storage_backend, canary}` | OK after blocks alias; document graph remains in latest/runs artifact path. |
| `v2_document_pages` | Markdown text: `02_work/document.md` first, then `01_input/*_document.md` / `*.md` | `{project_id, md_file, total_pages, version_id, pages}` | OK: upload syncs canonical `02_work/document.md`; source resolver keeps fallback to `01_input`. |
| `v2_document_page` | same MD source as pages endpoint | page detail with sheet/block data + canary | OK. |

## fix WRITE map

| artifact / operation | v2 write path | read_canary consumer |
|---|---|---|
| Uploaded original files | `versions/<vid>/01_input/<original_name>` | `v2_version_files`, source fallback, project metadata. |
| Canonical audit source | `versions/<vid>/02_work/document.pdf`, `document.md`, `result.json`, `ocr.html` | prepare/crop/gemma/text/document pages. Public file listing intentionally does not show these. |
| Project metadata | `versions/<vid>/01_input/project_info.json`, `version.json.project_info` | source resolver, status/details, audit start gates. |
| Version/document scaffold | `document.json`, `current_version.txt`, `versions/<vid>/version.json`, standard subdirs | projects list/details/versions. |
| Findings | `03_analysis/latest/03_findings.json` and `03_analysis/runs/<run>/03_findings.json` | `v2_findings`, `v2_finding_by_id`; latest has priority, per-file runs fallback covers partial latest. |
| Blocks analysis | `03_analysis/latest/02_blocks_analysis.json` and runs fallback | `v2_blocks_analysis`, block map. |
| Document graph | `03_analysis/latest/document_graph.json` and runs fallback | `v2_block_map`, document page helpers. |
| Optimization | `03_analysis/latest/optimization.json` and runs fallback | optimization/status/export readers. |
| Crop images/index | producer: `03_analysis/{latest|runs}/blocks_gemma_100`; compatibility alias: `03_analysis/{latest|runs}/blocks` | `v2_blocks`, `v2_block_image`, block map. |
| Clean destructive | backup under `_system/destructive_backups/<id>`, removes `03_analysis`, recreates empty `latest` | read_canary no longer sees findings/blocks after clean; `01_input` remains. |

## Coherence decisions

1. `v2_version_files` is a read-canary-owned public contract: show original `01_input` names. Fix branch aligned `version_service._source_file_records()` and stale tests to this shape. Internal audit source remains `02_work` via `resolve_version_source_files()`.
2. `blocks/` is a deploy read-canary path contract. Fix branch did not change deploy code; instead it now materializes a v2-only `blocks/` alias after Gemma crop success/partial success/recrop and manager direct crop paths. Legacy `_output` is not modified by the helper.
3. latest→runs fallback is coherent for per-file artifacts: tests remove latest `02_blocks_analysis.json` and verify the read-contract helper sees `runs/<run>/02_blocks_analysis.json`.
4. Destructive clean under v2-primary is coherent with read: backup + confirmation are required, `03_analysis` is removed, `latest` is recreated empty, and read-contract helpers return no findings/blocks while `01_input` survives.

## Tests

Primary contract coverage added in `tests/test_v2_read_write_coherence.py`:

- `test_v2_upload_write_matches_read_canary_version_files_contract`
- `test_v2_analysis_write_is_visible_through_read_canary_latest_and_runs`
- `test_v2_crop_blocks_alias_matches_read_canary_blocks_contract`
- `test_v2_clean_removes_what_read_canary_would_show`
- `test_read_canary_v2_shape_contract_is_tracked_for_all_endpoints`

Target run on 2026-06-23: `python3 -m pytest tests/test_v2_primary_version_upload.py tests/test_v2_primary_source_resolution_gaps.py tests/test_v2_read_write_coherence.py tests/test_clean_project_data_v2_primary.py tests/test_projects_v2_only_compat.py -q` → `33 passed`.

## Integration notes

Do not edit deploy `read_canary.py` on fix. During deploy integration, preserve the deployed read-canary functions and cherry-pick/apply only the write-side compatibility changes: `01_input` public listing alignment, v2 `blocks/` alias, and the new contract tests/docs.
