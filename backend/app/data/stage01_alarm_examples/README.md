# Stage 01 alarm-event examples

Frozen reference shapes for the future
`backend/app/data/stage01_alarm_events.jsonl` journal. **Reference
fixtures, not runtime files.** Nothing in the pipeline reads or writes
this directory. Tests in
`tests/text_analysis/test_stage01_alarms_schema.py` validate that each
fixture parses against the pydantic `AlarmEvent` model in
[`backend/app/services/text_analysis/stage01_alarms_schema.py`](../../services/text_analysis/stage01_alarms_schema.py).

## Files

| File | Alarm | Severity | Auto-mitigated | What it illustrates |
|---|---|---|---|---|
| `empty_event.json`        | AL-03 dedup_error                  | warn | no  | Minimal event with zero observed count — shape reference for the journal. |
| `warn_event.json`         | AL-11 fp_speculative_spike         | warn | no  | A typical rolling-7d threshold breach (E1 +71% vs A0 baseline). |
| `auto_disable_event.json` | AL-06 completeness_lens_failure_spike_high | page | yes | Sonnet outage → `STAGE01_COMPLETENESS_LENS_ENABLED=false` auto-flip. |

## Source of truth

- Alarm table verbatim:
  [experiments/md_analysis_comparison/production_preparation/telemetry/production_alerts.md](../../../../experiments/md_analysis_comparison/production_preparation/telemetry/production_alerts.md)
- Pydantic schema:
  [backend/app/services/text_analysis/stage01_alarms_schema.py](../../services/text_analysis/stage01_alarms_schema.py)
- Metric registry (each alarm's `metric_refs` are validated against it):
  [backend/app/services/text_analysis/stage01_telemetry_schema.py](../../services/text_analysis/stage01_telemetry_schema.py)

## When Phase 1 is wired (later sub-tasks)

The future `stage01_alarms.evaluate_alarms(daily_telemetry)` function
will produce `AlarmEvent` objects and append them as JSON lines to
`backend/app/data/stage01_alarm_events.jsonl`. The shape on disk will
match these fixtures byte-for-byte except for `timestamp` and `observed`
values.

## Notes on auto-mitigation

Six of the 28 alarms can flip a feature flag to false when
`STAGE01_AUTO_DISABLE_ON_ALARM = true`:

- AL-01, AL-02 → `STAGE01_DEDUP_ENABLED`
- AL-06, AL-17, AL-19, AL-26 → `STAGE01_COMPLETENESS_LENS_ENABLED`

The flag itself does **not** yet exist in `config.py` — that env-var will
be introduced in a later wiring sub-task. Until then the registry simply
records intent.
