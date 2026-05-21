# Production Guardrails — Stage 01 Upgrade

**Date:** 2026-05-20
**Scope:** Master catalog of every kill-switch, cap, fallback, and
auto-mitigation hook needed to ship Phase 0 / Phase 1 safely.

---

## 1. Master kill-switch list

All env vars are read at runtime (per audit run), same pattern as
`PAID_API_ENABLED` in
[`paid_api_guard.py`](../../../backend/app/services/llm/paid_api_guard.py)
(see `_paid_api_enabled_runtime()`). Defaults live in
`backend/app/core/config.py` (do NOT edit it here — this package
documents where they'd be wired).

### 1.1 Phase 0 flags

| Env var | Default | Purpose |
|---|---|---|
| `STAGE01_DEDUP_ENABLED` | `false` at first merge, flipped to `true` after Step 4 of [`phase0_rollout.md`](phase0_rollout.md) | Kill-switch for Phase 0 |
| `STAGE01_DEDUP_FUZZY_THRESHOLD` | `0.7` | Cosine-like similarity threshold for fuzzy_dedup |
| `STAGE01_DEDUP_SAMPLE_PCT` | `0` at first merge; `5`, `25`, `100` during rollout | Per-project sampling for canary |

### 1.2 Phase 1 flags

| Env var | Default | Purpose |
|---|---|---|
| `STAGE01_COMPLETENESS_LENS_ENABLED` | `false` | Kill-switch for Phase 1 |
| `STAGE01_COMPLETENESS_SHADOW` | `false` | When `true`, lens runs but output stays in `_output/stage01_shadow.json` |
| `STAGE01_COMPLETENESS_BY_DOC_TYPE` | JSON: `{"audit_comparison": true, "specification_only": true, "tz_vs_rd": false, "full_rd": false}` | Per-doc-type opt-in |
| `STAGE01_COMPLETENESS_DISCIPLINE_ALLOWLIST` | empty (means all) during canary; widens per Step 4 | Per-discipline opt-in |
| `STAGE01_COMPLETENESS_MAX_FINDINGS` | `10` (default); separately `STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD = 6` | Hard cap on lens output |
| `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN` | `0.7` | Below this confidence, fall back to `full_rd` (Phase 1 OFF) |
| `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE` | `true` | When lens errors, return current_method (A0) findings; no audit failure |
| `STAGE01_SHADOW_ON_DISABLED_DOCTYPE` | `false` | When `true`, run shadow lens on doc_types not in the opt-in matrix; gathers data without surfacing |

### 1.3 Telemetry / alarm flags

| Env var | Default | Purpose |
|---|---|---|
| `STAGE01_AUTO_DISABLE_ON_ALARM` | `true` | Auto-disable Phase 1 on AL-17, AL-19, AL-26 |
| `STAGE01_ALARM_FP_E4_PCT_WARN` | `25` | E4 warn threshold (%) |
| `STAGE01_ALARM_FP_E4_PCT_PAGE` | `50` | E4 page threshold (%) |
| `STAGE01_ALARM_E3_REJECTION_PCT_PER_PROJECT_WARN` | `30` | Per-project rejection rate warn (%) |
| `STAGE01_ALARM_COST_PCT_WARN` | `70` | Cost-vs-A0 warn (%) |
| `STAGE01_ALARM_COST_PCT_PAGE` | `100` | Cost-vs-A0 page (%) |
| `STAGE01_TELEMETRY_PATH` | `backend/app/data/stage01_telemetry.json` | Where rollup writes |

## 2. Max-findings cap policy

Two caps stack:

1. **Lens cap.** The Sonnet completeness lens output is post-processed
   to keep only the top N findings (sorted by severity then by
   confidence). N comes from `STAGE01_COMPLETENESS_MAX_FINDINGS` (10 by
   default) or `STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD` (6) when the
   document_type is `full_rd`. Even though full_rd should be Phase 1 OFF
   in default rollout, this cap protects against override or future
   enablement (research +114 FP across 11 full_rd cases —
   [FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md)).
2. **Total Stage 01 cap.** After merge + dedup, the total findings count
   for the project is capped at `STAGE01_MAX_TOTAL_FINDINGS = 30`. If
   the merged set exceeds 30, the lowest-severity, lowest-confidence
   items are trimmed. This is a backstop against any future drift.

If either cap is hit, `meta.caps_hit` records which cap fired.

## 3. Completeness cap by document_type

| document_type | Lens cap | Rationale |
|---|---|---|
| `audit_comparison` | 10 | research target; cross_01 produced 6, headroom |
| `specification_only` | 10 | ar_03 produced ~7-8 findings |
| `tz_vs_rd` | 8 | research showed lens can over-produce (+11 FP); tighter cap |
| `full_rd` | 6 | research +114 FP across 11 cases; the cap is the primary mitigation |

(Caps separately settable per doc_type — `STAGE01_COMPLETENESS_MAX_FINDINGS_<TYPE>`.)

## 4. Fallback to current_method on lens failure

Default behavior: when the Sonnet lens raises any exception (network,
timeout, validation, parse), the completeness leg returns
`{ "findings": [], "error": "..." }`. The merge step proceeds with
current_method findings only. The metric `completeness_lens_fallback_fired`
(C8) records the event. Verified by
[`test_fallback_to_a0.py`](../../algorithm_research/tests/test_fallback_to_a0.py).

To disable fallback (NOT recommended): set
`STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE=false`. The audit will fail-loud
on lens error. This is for debugging only.

## 5. Per-discipline disable switches

Used during Step 4 of `phase1_rollout.md` (per-discipline expansion).

```
STAGE01_COMPLETENESS_DISCIPLINE_ALLOWLIST = "EOM"
```

Means: only run the lens for EOM projects (in addition to the per-doc-type
matrix). Comma-separated, case-insensitive. Empty = all disciplines (the
old behavior).

This lets us roll a discipline back without losing the others.

## 6. Telemetry-driven auto-shutoff

Auto-shutoff hook lives in
`backend/app/services/stage01_alarms.py` (NEW). It runs every 10 minutes
via the existing manager loop (`backend/app/pipeline/manager.py`) and:

1. Reads `stage01_telemetry.json` and computes the current alarm values.
2. If `STAGE01_AUTO_DISABLE_ON_ALARM=true` AND any of AL-17, AL-19,
   AL-26 currently page → flip
   `STAGE01_COMPLETENESS_LENS_ENABLED=false`.
3. Writes an event to `backend/app/data/stage01_alarm_events.jsonl`.
4. Calls the on-call paging hook (existing channel).
5. Posts the sticky dashboard banner.

Phase 0 (`STAGE01_DEDUP_ENABLED`) does NOT participate in auto-shutoff.
Phase 0 is safe by construction; an alarm on it (AL-01, AL-02) IS
auto-mitigated but with a stricter contract:

- AL-01 and AL-02 (dedup_silent_critical_drop, dedup_mass_drop) fire
  ONLY on contract-violation conditions that mean dedup is fundamentally
  broken. In those cases auto-disable IS appropriate. The drill in
  [`rollback_strategy.md §2`](rollback_strategy.md) covers this.

## 7. Routing confidence threshold

`STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN = 0.7` (default).

If the detector reports confidence below this threshold for a project,
the detected type is overridden to `full_rd` (which has Phase 1 OFF in
default rollout). This is the "when in doubt, do the safe thing" rule.

Tuning notes:

- 0.7 chosen because research detector hit > 0.8 confidence on most
  validated cases; 0.7 is conservative.
- Raise to 0.8 if D3 (low_confidence_rate) is low — more conservatism
  with no cost.
- Do NOT lower below 0.6 — the detector becomes unreliable.

## 8. Document_type ambiguity policy

When a project's MD looks like two types (e.g. could be `audit_comparison`
or `tz_vs_rd`), the detector reports the highest-scoring type and a
confidence score. Policy:

- If confidence ≥ `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN`: use the
  detected type.
- If below: use `full_rd` (Phase 1 OFF).
- Engineer may override per-project via `project_info.json`:
  ```json
  {"document_type_override": "audit_comparison"}
  ```
  Override always wins; writes an audit-log entry to
  `backend/app/data/document_type_overrides.jsonl`. See
  [`routing_rules.md`](routing_rules.md).

## 9. Why this stack of guardrails

The research showed Phase 1 is a **conditional improvement** —
`audit_comparison` and `specification_only` get better,
`full_rd` and `tz_vs_rd` get worse on the strict score
([FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md)).
Each guardrail above mitigates one specific risk:

- Kill-switches: nuclear and per-feature rollback.
- Caps: limit blast radius of bad lens output.
- Fallback: lens failure never breaks audits.
- Per-discipline / per-doc-type opt-in: blast-radius limit by category.
- Auto-shutoff: prevents catastrophic regression from running for days.
- Confidence threshold: routes ambiguous cases to the safer default.

No single mechanism is the safety net; the stack is.

## 10. References

- [phase0_phase1_validation_report.md](../../algorithm_research/reports/phase0_phase1_validation_report.md) — gating evidence.
- [FINAL_SUMMARY.md](../../algorithm_research/reports/FINAL_SUMMARY.md) — per-doc-type matrix.
- [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) — FP audit.
- [`phase0_rollout.md`](phase0_rollout.md), [`phase1_rollout.md`](phase1_rollout.md), [`rollback_strategy.md`](rollback_strategy.md).
- [`../telemetry/production_alerts.md`](../telemetry/production_alerts.md) — alarm table.
- [`routing_rules.md`](routing_rules.md) — per-project overrides.
- [`paid_api_guard.py`](../../../backend/app/services/llm/paid_api_guard.py) — runtime-env-read pattern.
