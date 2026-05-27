# Telemetry Plan — Phase 0 / Phase 1 Stage 01 Upgrade

**Date:** 2026-05-20
**Scope:** Production telemetry for the Stage 01 upgrade described in
[`../README.md`](../README.md). Covers Phase 0 (dedup post-process at merge
tail) and Phase 1 (Sonnet `completeness` lens + discipline checklist +
`document_type` routing).

**Hard constraint:** this plan extends the existing production telemetry
surfaces (`paid_cost_dashboard.py`, `paid_cost.json`, `usage_data.json`,
`paid_api_events.jsonl`). It does **not** introduce a parallel tracking
system.

---

## 1. Why telemetry, what we are afraid of

The research (24 cases, 4 document types) showed Phase 1 helps some doc
types and hurts others on the strict scoring formula:

- `audit_comparison` → A1-v2 +26 strict_score vs A0 (see
  [FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md))
- `specification_only` → A1-v2 21.7 with no A0 baseline; caught 3/3 GT
- `tz_vs_rd` → A1-v2 worse on 1 case (+11 FP)
- `full_rd` → A1-v2 −26.7 strict_score, +114 FP across 11 cases

The 24-case FP audit ([a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md))
showed **0 cases of speculative_noise** across all 24 cases. The extra
findings are mostly `duplicate_of_gt`, `beyond_gt_useful` or
`wrong_severity`. That means production telemetry must distinguish
**review-load** regression from **noise** regression — they are not the
same thing.

Telemetry exists to detect, in this order:

1. **Critical recall regression** — a Phase 1 lens or dedup rule silently
   drops a КРИТИЧЕСКОЕ finding. This is the most expensive failure mode.
2. **FP / review-load regression** — even with 0 speculative noise, if
   findings/project trends above the +20% budget, engineer time blows up.
3. **Cost blow-up** — Phase 1 was budgeted at ≤ +70% LLM cost
   ([phase0_phase1_validation_report.md §5](../../algorithm_research/reports/phase0_phase1_validation_report.md)),
   so we monitor cost-per-project against the same budget in production.
4. **Lens failure rate** — Sonnet completeness lens has a documented fail
   path (graceful fallback to A0 — see `test_fallback_to_a0.py`). We need
   to know how often that path fires.
5. **Document_type detection drift** — Phase 1 is gated by document_type.
   If detection confidence drops below `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN`,
   we fall back to `full_rd` (Phase 1 OFF). A spike in low-confidence
   detections means routing is silently bypassing Phase 1.

## 2. Four measurement surfaces

| Surface | Cadence | Source of truth | Consumer |
|---|---|---|---|
| Per-project (live) | per audit run | `_output/03_findings.json` + `_output/stage01_meta.json` | webapp project page, alerts |
| Per-day rollup | every audit completion | `paid_cost.json` + new `stage01_telemetry.json` daily section | webapp dashboard, daily alert sweep |
| Per-week trend | every audit completion | `stage01_telemetry.json` weekly aggregates | weekly review meeting |
| A/B shadow | per audit run, when shadow ON | parallel A0 + A1-v2 outputs in `03_findings.json` meta block | research-only dashboard |

Per-project is the live signal — used for in-audit display ("FP estimate
14, Phase 1 ON, completeness +6 findings") and for hard alerts.

Per-day is the rollup that the existing `paid_cost_dashboard.py` already
exposes — we extend its envelope with Stage 01-specific fields. We do not
introduce a new dashboard file.

Per-week is the slow-moving trend we use to detect drift (e.g. critical
recall dropping by 30% in a discipline over 7 days).

A/B shadow is described in [`../rollout/ab_testing_strategy.md`](../rollout/ab_testing_strategy.md):
when Phase 1 is in shadow mode, the A0 leg still runs and both outputs are
captured. This costs more but is necessary for the canary phase.

## 3. Where each metric is produced

| Metric | Produced in (file, hypothetical) |
|---|---|
| `findings_count_total`, severity histogram | `backend/app/pipeline/stages/findings_merge/runner.py` after merge |
| `dedup_report.{total_in,same_class_drops,fuzzy_drops,total_out}` | inside dedup module called from findings_merge tail |
| `completeness_lens.{enabled,applied,findings_added,duration_ms,error}` | new `backend/app/pipeline/stages/text_analysis/completeness_runner.py` |
| `document_type.{detected,confidence,fallback_used}` | new `backend/app/services/findings/document_type_detector.py` |
| `lens_failure_rate` | aggregated from `completeness_lens.error` events |
| `wall_clock_per_stage` | existing `pipeline_log.json` + per-stage timing already collected |
| `llm_cost_per_project_by_lens` | extend `paid_cost_events.jsonl` `stage` field with values `text_analysis.current_method` and `text_analysis.completeness` |

Everything routes to one of two existing sinks:

- **paid_cost_events.jsonl** — for cost / per-call events. Already exists,
  written by `paid_api_guard.py` / `paid_cost_tracker`. Phase 1 just sets
  a more specific `stage` value so the existing dashboard's
  `by_stage` aggregation splits the lens cost out automatically.
- **stage01_telemetry.json** (NEW, lives in `backend/app/data/`) — for
  non-cost metrics (findings counts, dedup reports, lens failure rate,
  document_type distribution). Schema mirrors `paid_cost.json`:
  `{daily_breakdown: {date: {...}}, weekly_breakdown: {...}}`.

## 4. Flow into `paid_cost_dashboard.py`

`paid_cost_dashboard.build_paid_cost_daily_dashboard(...)` already aggregates
`by_model`, `by_project`, `by_stage` from `paid_cost.json + paid_cost_events.jsonl`.

The Stage 01 upgrade extends this by:

1. New `stage` values written by Phase 1:
   - `text_analysis.current_method`
   - `text_analysis.completeness`
   - `findings_merge.dedup` (no LLM, will appear with cost 0; useful for counting calls)

2. A sibling dashboard builder
   `backend/app/services/llm/stage01_telemetry_dashboard.py` (NEW) reads
   the new `stage01_telemetry.json` and merges it with the existing
   `paid_cost_daily_dashboard` output under a new `stage01` key:

   ```json
   {
     "days": [...],
     "totals": {...},
     "stage01": {
       "days": [
         {"date": "2026-05-21", "findings_count": 421, "phase1_applied_count": 17, ...}
       ],
       "totals": {...}
     }
   }
   ```

3. The frontend already calls `GET /api/usage/paid-cost/daily` and renders
   per-day tiles. A new `GET /api/stage01/telemetry/daily` returns the
   Stage 01 fields with the same envelope shape; the UI adds 2-3 KPI tiles
   without changing the existing layout.

**We never replace the existing dashboard.** Stage 01 telemetry sits
alongside cost telemetry and reuses the day-by-day shape.

## 5. Sample per-project telemetry payload

Written to `<project>/_output/stage01_meta.json` at end of `findings_merge`.

```json
{
  "schema_version": 1,
  "project_id": "EOM/13АВ-РД-ЭО-К3",
  "version_id": "v3",
  "run_id": "2026-05-21T09:14:00",
  "document_type": {
    "detected": "audit_comparison",
    "confidence": 0.83,
    "detector_version": "1.0",
    "fallback_used": false,
    "below_min_confidence": false
  },
  "current_method": {
    "model": "claude-opus-4-7",
    "duration_ms": 187432,
    "findings_count": 11,
    "speculative_keyword_hits": 0,
    "low_confidence_count": 2
  },
  "completeness_lens": {
    "enabled": true,
    "applied": true,
    "model": "claude-sonnet-4-6",
    "duration_ms": 142198,
    "findings_added": 5,
    "cap": 10,
    "cap_hit": false,
    "error": null,
    "fallback_fired": false
  },
  "dedup": {
    "enabled": true,
    "total_in": 16,
    "same_class_drops": 1,
    "fuzzy_drops": 2,
    "fuzzy_threshold": 0.7,
    "total_out": 13,
    "critical_protected": 4
  },
  "findings_summary": {
    "total": 13,
    "by_severity": {"КРИТ": 4, "ЭКОН": 2, "ЭКСПЛ": 3, "РЕКОМ": 2, "ПРОВЕРИТЬ": 2},
    "by_problem_class": {"NORM_OUTDATED": 3, "VALUE_MISMATCH": 4, ...},
    "fp_estimate_heuristic": 1,
    "low_confidence_count": 2
  },
  "phase_flags": {
    "STAGE01_DEDUP_ENABLED": true,
    "STAGE01_COMPLETENESS_LENS_ENABLED": true,
    "STAGE01_COMPLETENESS_BY_DOC_TYPE": {"audit_comparison": true, "specification_only": true, "tz_vs_rd": false, "full_rd": false},
    "STAGE01_COMPLETENESS_MAX_FINDINGS": 10,
    "STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN": 0.7
  }
}
```

This payload feeds:

- the per-project page (existing project drill-down adds a "Stage 01" tab);
- the daily rollup (`stage01_telemetry.json` writer reads each
  `stage01_meta.json` after the audit completes);
- the A/B shadow comparison (when shadow ON, this payload includes a
  `shadow_a0` block with the same shape but A0-only numbers).

## 6. What telemetry does NOT do

- Telemetry does **not** decide go/no-go for a project — that is the
  engineer's job in the existing UI. Telemetry only flags risk.
- Telemetry does **not** ground-truth-validate findings — there is no GT
  in production. FP rate is an **estimate**. Critical recall is a
  **proxy** (see [`critical_recall_monitoring.md`](critical_recall_monitoring.md)).
- Telemetry does **not** automatically disable Phase 1 except via the
  alarm hook described in [`production_alerts.md`](production_alerts.md).
  Auto-shutoff requires the explicit `STAGE01_AUTO_DISABLE_ON_ALARM` flag.

## 7. References

- [phase0_phase1_validation_report.md](../../algorithm_research/reports/phase0_phase1_validation_report.md) — gating criteria.
- [FINAL_SUMMARY.md](../../algorithm_research/reports/FINAL_SUMMARY.md) — per-doc-type aggregates.
- [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) — speculative_noise = 0 across 24 cases.
- [backend/app/services/llm/paid_cost_dashboard.py](../../../backend/app/services/llm/paid_cost_dashboard.py) — existing dashboard contract.
- [backend/app/services/llm/paid_api_guard.py](../../../backend/app/services/llm/paid_api_guard.py) — kill-switch pattern.
- [backend/app/data/paid_cost.json](../../../backend/app/data/paid_cost.json) — daily_breakdown schema.
