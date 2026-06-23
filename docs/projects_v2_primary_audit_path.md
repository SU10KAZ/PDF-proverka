# projects_v2 primary audit path

Этот документ фиксирует текущий контракт v2-primary перед ручным cutover. Он не
заменяет `docs/projects_v2_write_cutover_playbook.md`: здесь только инженерная
карта чтения источников, записи audit artifacts и проверок.

## Флаги

v2-primary включается только явными runtime-флагами:

- `AUDIT_STORAGE_BACKEND=projects_v2`
- `AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary`
- `AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=true`

При production OFF-наборе (`AUDIT_STORAGE_BACKEND=legacy`,
`AUDIT_PROJECTS_V2_WRITE_MODE=dual_write_shadow` или `legacy`,
`AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=false`) legacy-путь остается рабочим
контрактом. Кодовые v2-ветки должны быть инертны.

Откат флагов после ручного cutover: вернуть read backend в `legacy`, write mode
в безопасный не-primary режим, read default в `false`, затем человек
перезапускает backend в согласованном окне.

## Layout

Legacy версия:

```text
projects/<project>/
  document.pdf
  *_document.md
  *_result.json
  project_info.json
  _output/
```

projects_v2 версия:

```text
projects_v2/objects/<object>/disciplines/<discipline>/documents/<code>/versions/vNNN/
  01_input/
    <original>.pdf
    <original>_document.md
    <original>_result.json
    project_info.json
  02_work/
    document.pdf
    document.md
    result.json
    ocr.html
  03_analysis/
    runs/<job_id>/
    latest/
  04_review/
  05_export/
  version.json
```

## Source Resolution

Canonical helper:

- `backend/app/services/storage/projects_v2_source_resolver.py`
- `resolve_version_source_files(version_dir, document_code, project_info=...)`
- `load_version_project_info(version_dir)`
- `resolve_project_info_path(version_dir)`

Правило: callers передают `version_dir`. Helper layout-aware:

- если это v2 version dir, источники берутся из `02_work` с fallback в
  `01_input`;
- если это legacy dir, сохраняется прежний root-glob/`project_info.json`
  behavior.

Основные v2-primary consumers:

- prepare: `backend/app/pipeline/stages/prepare/process_project.py`,
  `graph_builder.py`, `prepare_service.py`;
- crop/Gemma gates: `crop_blocks/blocks.py`, `gemma_gate.py`,
  `gemma_enrichment_contract.py`, `gemma_enrich.py`;
- manager/path gates: `backend/app/pipeline/manager.py`;
- readers/export/report/findings: соответствующие services/routers use
  layout-aware project info and source paths.

## Version IDs

Physical v2 folders use `v001`, `v002`, ...

Version APIs must accept both logical legacy ids (`v1`) and physical ids
(`v001`) when v2 context is enabled. The normalized v2 context is resolved in
`backend/app/services/common/version_service.py` and points to
`.../versions/vNNN`, not to legacy project root.

Important invariant: under v2-primary, source reads, output writes and
promotion target use the same physical version context.

## Audit Output

For a job, audit stages write to:

```text
versions/vNNN/03_analysis/runs/<job_id>/
```

Selected artifacts are promoted to:

```text
versions/vNNN/03_analysis/latest/
```

The job-scoped output path is carried by:

- `PipelineStageContext.project_dir`
- `PipelineStageContext.output_dir`
- `PipelineStageContext.version_id`
- subprocess env: `AUDIT_VERSION_DIR`, `AUDIT_OUTPUT_DIR`,
  `AUDIT_PROJECT_ID`, `AUDIT_VERSION_ID`

`PipelineStageContext.run_subprocess()` delegates through the manager helper so
prepare/crop/report subprocesses receive those env values. Agent/LLM stages
must receive explicit `output_dir`/`version_dir`/`version_id`; prompts for
findings merge, critic/corrector, norms and optimization must name the absolute
v2 run dir.

## Modes And Entrypoints

Covered active paths:

- `full-audit`, `standard-audit`, `pro-audit`: full pipeline aliases; v2 output
  is run-dir scoped.
- `main-audit`: legacy stub path receives explicit v2 output/version paths.
- `tile-audit`: uses version-aware job paths and scoped subprocess env.
- retry/resume/batch retry/batch resume: preserve `version_id`; without it a
  retry can target latest instead of the requested `vNNN`.

Known non-v2 blocker:

- smart-audit priority-pages branch remains disabled because
  `process_project.py` does not support `--pages/--quality`; this is the same
  legacy behavior and is not a v2 storage regression.

## Upload/Create Version

Under v2-primary, v2-only documents are valid targets for version endpoints.
New physical versions are created as `versions/vNNN` with standard subdirs,
`document.json.versions/version_ids/current_version`, `current_version.txt`,
and `version.json`.

Uploaded source files for v2 versions go to `01_input`; canonical work copies
are synchronized to `02_work/document.pdf`, `document.md`, `result.json`,
`ocr.html`.

## Destructive Operations

`clean_project_data` under v2-primary requires explicit confirmation and writes
backup under `_system/destructive_backups`; restore creates a preimage backup.

`rename_project` remains blocked under v2-primary until a user-visible
confirmation/backup contract is approved.

No destructive action should be run on real data without preflight idle-check,
backup and dry-run/confirmation, as required by `migration.md`.

## Validation Commands

Focused v2/source tests:

```bash
python3 -m pytest tests/test_v2_primary_audit_entrypoints.py \
  tests/test_v2_primary_version_upload.py \
  tests/test_resolve_active_project_dir.py -q
```

Matrix across real ledger documents, writing only to `/tmp` shadows:

```bash
python3 scripts/projects_v2/check_v2_audit_matrix.py \
  --limit 12 \
  --json-out /tmp/v2_audit_matrix_report.json
```

Regression gate against known baseline:

```bash
python3 scripts/ci_regression_gate.py
```

Cutover readiness/data parity remain separate operational checks:

```bash
python3 scripts/projects_v2/check_cutover_readiness.py --per-type 10
python3 scripts/projects_v2/check_ui_contract_parity.py --all
```

## Current Open Human Gates

- Real external Opus findings merge under production environment after cutover
  remains a watched smoke. Codex did not run external LLM.
- Manual integration into deploy must cherry-pick current fix commits onto the
  deploy line; do not merge the whole fix branch blindly. See
  `migration_deploy_fix_conflicts.md`.
- Actual flag flip and backend restart are human-only steps.
