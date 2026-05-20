# Phase 0 Dashboard Plan

**Date:** 2026-05-20
**Scope:** A focused dashboard specification for the Phase 0 dedup feature
only. This is a **subset** of the broader Stage 01 telemetry described in
[`telemetry_plan.md`](telemetry_plan.md) and a subset of the 28-alarm catalog
in [`production_alerts.md`](production_alerts.md).

Phase 0 does not yet require this dashboard to be built — the
`meta.dedup_report` block written to `_output/03_findings.json` is the
canonical source of truth and can be queried ad-hoc until the dashboard
ships. This document is the spec for a future implementation task.

---

## 1. Dashboard purpose

The Phase 0 dashboard answers a single question, in one screen, for the
on-call engineer: **is the dedup post-process behaving as designed today?**
Concretely: are we dropping any КРИТИЧЕСКОЕ findings (we must not be), is the
fail-open path firing (it should not be), are we burning unexpected wall time
on dedup (we should not be), and is the flag actually rolled out where we
expect it to be. Everything else (FP estimate, completeness lens, document
type drift) lives on the broader Stage 01 dashboard described in
[`telemetry_plan.md`](telemetry_plan.md).

## 2. Tiles / widgets

Ten tiles, each with title, source, unit, refresh cadence, green / yellow /
red thresholds and a pointer to the alert (if any).

### 2.1 `findings_before` (24h sum)

- **Title:** Findings before dedup (24h)
- **Source:** sum of `meta.dedup_report.before` across all `03_findings.json`
  written in the last 24h.
- **Unit:** integer (findings).
- **Refresh:** every 15 min.
- **Thresholds:** **info-only**, no alert. This tile gives context for the
  delta with `findings_after`.

### 2.2 `findings_after` (24h sum)

- **Title:** Findings after dedup (24h)
- **Source:** sum of `meta.dedup_report.after`.
- **Unit:** integer.
- **Refresh:** every 15 min.
- **Thresholds:** info-only. Expected `findings_after ≈ findings_before` on
  A0 baseline.

### 2.3 `same_class_drops` (24h sum)

- **Title:** Same-class dedup drops (24h)
- **Source:** sum of `meta.dedup_report.class_dedup.same_class_drops`.
- **Unit:** integer.
- **Refresh:** every 15 min.
- **Thresholds:**
  | Range | Status |
  |---|---|
  | 0–50 | green |
  | 50–200 | yellow (investigate upstream — possibly prompt drift producing duplicates) |
  | > 200 | red (cross-reference with `production_alerts.md`; no dedicated AL yet but file a ticket) |

### 2.4 `fuzzy_drops` (24h sum)

- **Title:** Fuzzy dedup drops (24h)
- **Source:** sum of `meta.dedup_report.fuzzy_dedup.same_class_drops`.
- **Unit:** integer.
- **Refresh:** every 15 min.
- **Thresholds:**
  | Range | Status |
  |---|---|
  | 0–50 | green |
  | 50–200 | yellow (consider raising `STAGE01_DEDUP_FUZZY_THRESHOLD` to 0.75 or 0.8 per [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md) §2) |
  | > 200 | red (likely upstream duplicate flood; file a ticket) |

### 2.5 `critical_collapsed_count` (24h sum) — most important tile

- **Title:** КРИТ-protect counter (24h)
- **Source:** sum of `meta.dedup_report.critical_collapsed_count`.
- **Unit:** integer.
- **Refresh:** every 5 min (high-priority polling).
- **Thresholds:**
  | Range | Status | Alert |
  |---|---|---|
  | 0 | green | — |
  | 1–2 | yellow (investigate but do not page) | — |
  | > 2 | red — **page** | [AL-01](production_alerts.md) |

  This counter is the hard production invariant: the safety contract
  (see [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md) §1) guarantees
  no КРИТИЧЕСКОЕ finding is silently collapsed, and the counter increments
  exactly when the guard fires. In production it must be `0`. Yellow exists
  to flag isolated guard activations that are technically benign (both КРИТ
  preserved) but warrant a look at the underlying upstream data.

### 2.6 `dedup_error_count` (24h sum)

- **Title:** Phase 0 fail-open events (24h)
- **Source:** count of `Phase 0 dedup: ошибка` lines in pipeline logs across
  projects audited in window. Equivalently: count of projects whose
  `meta.dedup_report` is an `{"error": ..., "skipped": true}` shape (see
  fail-open contract in [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md) §4).
- **Unit:** integer.
- **Refresh:** every 15 min.
- **Thresholds:**
  | Range | Status |
  |---|---|
  | 0 | green |
  | 1–4 | yellow — investigate per-project root cause (likely malformed upstream finding) |
  | > 4 | red — escalate to Tier 2 backend lead |

### 2.7 `dedup_duration_ms` (p50 / p95 / max per 24h)

- **Title:** Dedup duration per project (24h)
- **Source:** derived from pipeline timing in `pipeline_log.json` for the
  `apply_phase0_dedup` step.
- **Unit:** milliseconds (three values: p50, p95, max).
- **Refresh:** every 15 min.
- **Thresholds (applied to p95):**
  | Range | Status |
  |---|---|
  | < 100 ms | green |
  | 100–500 ms | yellow |
  | > 500 ms | red — check for unusually large findings lists (> 200, which triggers the known O(N²) limitation in fuzzy_dedup per [`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §10.2) |

### 2.8 `projects_with_dedup` (24h count)

- **Title:** Projects audited under Phase 0 (24h)
- **Source:** count of distinct `03_findings.json` files written in the last
  24h that contain a `meta.dedup_report` block.
- **Unit:** integer.
- **Refresh:** every 15 min.
- **Thresholds:** info-only. Used to track adoption / rollout coverage.

### 2.9 `no_op_rate` (% of projects where `before == after` in 24h)

- **Title:** No-op rate (24h)
- **Source:** `(count of projects where meta.dedup_report.before == meta.dedup_report.after) / projects_with_dedup`.
- **Unit:** percentage.
- **Refresh:** every 15 min.
- **Thresholds:**
  | Range | Status |
  |---|---|
  | > 90% | green (A0 baseline expectation per [`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §10.1) |
  | 70–90% | yellow |
  | < 70% | red — dedup is firing more than expected; investigate Stage 01 prompts for drift |

### 2.10 `flag_state`

- **Title:** `STAGE01_DEDUP_ENABLED` across workers
- **Source:** a heartbeat endpoint per backend worker that returns the value
  of `config.STAGE01_DEDUP_ENABLED` currently loaded.
- **Unit:** boolean per worker, aggregated to "N of M workers ON".
- **Refresh:** every 1 min.
- **Thresholds:** info-only. Used for operator confidence — if the rollout
  is supposed to be at 100% but some workers report `false`, the deployment
  is drifting and the operator should restart the lagging workers.

## 3. Alert thresholds

Phase 0 alerts in the consolidated catalog [`production_alerts.md`](production_alerts.md):

- **AL-01 (`dedup_silent_critical_drop`, page)** — fires when any project's
  `critical_collapsed_count > 0` AND the input had КРИТ findings. Maps to
  tile §2.5 going red. Auto-mitigation is to set `STAGE01_DEDUP_ENABLED=false`.
- **AL-09 (`document_type_low_confidence`)** — listed in the catalog as the
  most relevant non-AL-01 alarm during Phase 0 rollout context, but applies
  to Phase 1 routing and not directly to dedup. Included here only as a
  cross-reference because it is the second-most-watched Phase 0/1 alarm and
  may surface as a yellow blip during the same rollout window.

This dashboard does not introduce new alert IDs. AL-01 is the only one that
pages for Phase 0 specifically.

## 4. Operator actions

| Tile red / yellow | Action |
|---|---|
| §2.5 `critical_collapsed_count` red | Flip `STAGE01_DEDUP_ENABLED=false` via the deployment env immediately (see L1 in [`../rollout/production_enablement_checklist.md`](../rollout/production_enablement_checklist.md) §6). Restart backend workers. File a ticket with the offending `meta.dedup_report` and the input findings list. Do not re-enable until root cause is identified. |
| §2.5 `critical_collapsed_count` yellow | Open the project's `03_findings.json`, confirm both КРИТ findings are present in `findings[]` (the guard preserves both), document the duplicate-class pattern. Decide whether the upstream pattern needs a prompt-side fix. Do not roll back. |
| §2.6 `dedup_error_count` red | Pull stack traces from `pipeline_log.json` on each affected project. Look for malformed `03_findings.json` fields (None severity, non-string `problem_class`). The dedup module is fail-open, so production findings are safe; the priority is to fix the upstream data, not the dedup. See [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md) §3 for the catalog of degradation modes. |
| §2.7 `dedup_duration_ms` red | Identify the offending project (likely > 200 findings). Profile fuzzy_dedup specifically — it is O(N²) and dominates above ~500 findings per the known limitation in [`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §10.2. Possible follow-up: replace fuzzy_dedup with a hashing/min-hash approximation in a separate PR. |
| §2.3 / §2.4 `same_class_drops` / `fuzzy_drops` surge | Spot-check three projects from the surge. The most common cause is upstream prompt drift producing many near-duplicate findings; dedup is doing its job, but the prompt should also be tuned to emit fewer duplicates. Escalate pattern to Tier 3 (research lead). |
| §2.10 `flag_state` mismatch | If rollout is supposed to be at 100% but some workers report `false`, restart the lagging workers — the env var did not propagate. If rollback is supposed to be in effect but some workers report `true`, repeat the env-flip and restart. |

## 5. Data source contract

Every metric on this dashboard reads from a single canonical source: the
`meta.dedup_report` block of `_output/03_findings.json`. The aggregator must
scan all freshly-written `03_findings.json` files per day. Schema reference:

```json
{
  "meta": {
    "dedup_report": {
      "class_dedup": {
        "total_in": <int>, "total_out": <int>, "clusters": <int>,
        "same_class_drops": <int>,
        "same_class_drops_by_key": {"<class_key>": <int>, ...},
        "critical_collapsed_count": <int>,
        "methods_seen": []
      },
      "fuzzy_dedup": {
        "total_in": <int>, "total_out": <int>, "clusters": <int>,
        "same_class_drops": <int>,
        "same_class_drops_by_key": {"<sig>": <int>, ...},
        "critical_collapsed_count": <int>,
        "sim_threshold": <float>,
        "methods_seen": []
      },
      "before": <int>,
      "after": <int>,
      "critical_collapsed_count": <int>,
      "fuzzy_threshold": <float>
    }
  }
}
```

Fail-open shape (when an exception fired in `apply_phase0_dedup`):

```json
{
  "meta": {
    "dedup_report": {"error": "<exception text>", "skipped": true}
  }
}
```

The aggregator should treat the fail-open shape as a single `dedup_error_count`
increment and skip the numeric tiles for that project. It must not raise an
exception of its own when encountering the fail-open shape.

The dedup_duration is **not** present in `meta.dedup_report`; the aggregator
reads it from `pipeline_log.json`'s per-stage timing entries for the
`findings_merge` step (specifically, the inner `apply_phase0_dedup` span if
present; otherwise the delta between the merge step's existing timestamps).

## 6. Implementation note

This dashboard does NOT yet exist as code. The plan above is the spec for a
future task. Phase 0 itself does not require this dashboard to ship — the
`meta.dedup_report` JSON is the source of truth and can be queried ad-hoc
during the rollout using `jq` / `find` (see examples in
[`../rollout/production_enablement_checklist.md`](../rollout/production_enablement_checklist.md) §1, §3).

The implementation pattern should mirror the existing
`backend/app/services/llm/paid_cost_dashboard.py` envelope (per
[`telemetry_plan.md`](telemetry_plan.md) §4):

- a sibling builder, e.g. `backend/app/services/findings/dedup_dashboard.py`,
  that scans `_output/03_findings.json` files and aggregates `meta.dedup_report`
  per day / per discipline;
- a new endpoint `GET /api/findings/dedup/daily` returning the daily envelope;
- a frontend KPI strip added alongside the existing paid-cost daily dashboard,
  not as a separate page (consistent with the "no parallel dashboards" hard
  constraint in [`telemetry_plan.md`](telemetry_plan.md) §0).

Until the dashboard is built, the on-call engineer relies on:

- the per-project `meta.dedup_report` JSON;
- the structured `ctx.log` lines (`Phase 0 dedup: ...`);
- ad-hoc `jq` sweeps across `_output/03_findings.json` files (patterns in
  [`../rollout/production_enablement_checklist.md`](../rollout/production_enablement_checklist.md) §1).
