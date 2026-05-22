# Stage 01 telemetry — example fixtures

Frozen example shapes for the on-disk telemetry artefacts that Phase 1
will write. **These are reference fixtures, not runtime files.** Nothing
in the pipeline reads or writes this directory. Tests in
`tests/text_analysis/test_stage01_telemetry_schema.py` validate that each
fixture parses against the pydantic models in
`backend/app/services/text_analysis/stage01_telemetry_schema.py`.

## Files

| File | Shape | Purpose |
|---|---|---|
| `empty_per_project.json` | `Stage01PerProjectTelemetry` | Zero-filled skeleton with all groups present and Phase 1 fields at safe defaults. |
| `filled_per_project.json` | `Stage01PerProjectTelemetry` | A realistic per-project snapshot — Phase 0 dedup numbers populated, completeness lens OFF, document_type detected as `full_rd`. Mirrors current production (Phase 1 not yet wired). |
| `empty_per_day.json`     | `Stage01PerDayTelemetry`    | Zero-filled daily rollup skeleton for a single date. |

## Source of truth

Metric inventory and aggregation rules:
[experiments/md_analysis_comparison/production_preparation/telemetry/metrics_definition.md](../../../../experiments/md_analysis_comparison/production_preparation/telemetry/metrics_definition.md).

Pydantic schema:
[backend/app/services/text_analysis/stage01_telemetry_schema.py](../../services/text_analysis/stage01_telemetry_schema.py).

## When Phase 1 is wired (later sub-tasks)

- `<project>/_output/stage01_meta.json` will use the
  `Stage01PerProjectTelemetry` shape, written by the future telemetry
  emitter inside the Stage 01 runner.
- `backend/app/data/stage01_telemetry.json` will use the
  `Stage01PerDayTelemetry` shape, accreted by the daily rollup job.

Until then, these fixtures are the only artefacts on disk in that shape.
