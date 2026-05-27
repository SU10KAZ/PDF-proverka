# Rollback Strategy — Stage 01 Upgrade

**Date:** 2026-05-20
**Scope:** Per phase: what "rollback" means; which env var to flip; what
data must be cleaned up; how long the rollback takes; how to re-roll.

---

## 1. Why rollback is cheap here

Stage 01 outputs are written to `<project>/_output/03_findings.json` with
the existing version_service mechanism (see
[backend/app/api/routers/](../../../backend/app/api/routers/) and the
project-version migration discussed in
[`../README.md`](../README.md)). Each audit run produces a NEW
`version_id` — we never overwrite a prior finding history. That gives us
the no-regret property: roll forward to a new version, never mutate the
old.

We also do not change the `03_findings.json` schema irreversibly. The
new `meta.dedup_report` and `meta.document_type` fields are additive;
old consumers ignore them.

So rollback is:

1. Flip the relevant `STAGE01_*` env var (no redeploy, env is re-read
   per audit run — same pattern as `PAID_API_ENABLED` in
   [paid_api_guard.py](../../../backend/app/services/llm/paid_api_guard.py)).
2. New audit runs ignore the disabled feature.
3. Already-completed audits keep their existing findings; if engineers
   want a re-audit on the old algorithm, run again — produces a new
   version_id.

## 2. Phase 0 rollback

### What "rollback" means

`STAGE01_DEDUP_ENABLED=false`.

- New audits skip the dedup tail — output is byte-identical to pre-Phase
  0 behavior (proven safe on 8 research cases).
- Already-deduplicated `03_findings.json` files keep their dedup'd
  outputs. They still parse — `meta.dedup_report` is a non-breaking
  additive field.

### Cleanup data

None. Phase 0 is purely a transformation step, no side effects to any
file other than `03_findings.json` (which is per-version-id anyway).

### Time to rollback

Seconds (env var change). Re-tests show the byte-equality contract
holds.

### Roll-forward after rollback

Set `STAGE01_DEDUP_ENABLED=true` again. The flag is read per-audit, so
the next run picks it up.

### Drill (chaos exercise)

Twice a quarter:

1. Pick a staging project. Run audit with Phase 0 ON; capture
   `03_findings.json` hash.
2. Flip `STAGE01_DEDUP_ENABLED=false`. Re-run same project (new
   version_id). Capture hash.
3. Diff the two `03_findings.json` files; the difference should be only
   in `meta.dedup_report` and (if dedup found duplicates) some
   `findings[]` entries.
4. Document the drill outcome in `backend/app/data/rollback_drills.jsonl`.

## 3. Phase 1 rollback

Phase 1 has multiple layers; rollback can be staged.

### 3.1 Full rollback (nuclear)

`STAGE01_COMPLETENESS_LENS_ENABLED=false`.

- New audits skip Sonnet completeness lens entirely.
- A0 (current_method) is the only LLM leg.
- Phase 0 dedup keeps running (it's safe on A0).
- `document_type` detector keeps running (it's deterministic and free —
  we keep the telemetry but don't gate behavior on it).

### 3.2 Partial rollback (per doc_type)

`STAGE01_COMPLETENESS_BY_DOC_TYPE = {"audit_comparison": false, ...}`.

- Switch one or more document_types off without losing the lens for
  the others. Used when one document_type starts misbehaving but the
  others are clean.

### 3.3 Per-discipline rollback

`STAGE01_COMPLETENESS_DISCIPLINE_ALLOWLIST = "EOM"` (remove the
misbehaving discipline from the comma-separated list).

### 3.4 Auto-disable rollback

`STAGE01_AUTO_DISABLE_ON_ALARM=true` (the default).

When AL-17, AL-19, or AL-26 fire (engineer-rejection-spike or
FP-composite-spike or cost-blow-up), production automatically flips
`STAGE01_COMPLETENESS_LENS_ENABLED=false`. The on-call engineer must
manually re-enable after investigation.

This is the "auto" rollback — same effect as 3.1 but triggered by
telemetry.

### Cleanup data

None. Phase 1 output is part of the per-version `03_findings.json`
already. We never delete completed findings; we just produce new ones on
re-audit.

### Time to rollback

- Full rollback: env var flip, seconds. Next audit picks it up.
- Auto-disable: alarm fires → env var flip → on-call paged. Total
  ~10-15 min from the trigger condition becoming true.

### Roll-forward after rollback

Investigation must complete first (this is policy, not technical). On
re-enable:

1. Set the env var back.
2. Watch for 24 hours; verify the original alarm condition has not
   reappeared.
3. Document the rollback + roll-forward in
   `backend/app/data/stage01_alarm_events.jsonl`.

### Drill (chaos exercise)

Quarterly:

1. Synthetically inject E4 = 80% (above the page threshold) into the
   telemetry test harness.
2. Verify the auto-disable fires.
3. Verify the dashboard shows the sticky banner.
4. Verify the env var is flipped.
5. Re-enable manually; verify no findings were lost.
6. Document in `rollback_drills.jsonl`.

## 4. Per-flag rollback summary

| Flag | Default | Rollback effect | Drill cadence |
|---|---|---|---|
| `STAGE01_DEDUP_ENABLED` | true after step 4 | Phase 0 OFF | semi-annual |
| `STAGE01_DEDUP_FUZZY_THRESHOLD` | 0.7 | tune up to 0.8 to make Phase 0 stricter | as needed |
| `STAGE01_COMPLETENESS_LENS_ENABLED` | true after step 3 | Phase 1 OFF | quarterly |
| `STAGE01_COMPLETENESS_BY_DOC_TYPE` | per matrix | per-doc-type rollback | as needed |
| `STAGE01_COMPLETENESS_DISCIPLINE_ALLOWLIST` | per allowlist | per-discipline rollback | as needed |
| `STAGE01_COMPLETENESS_MAX_FINDINGS` | 10 (6 for full_rd) | cap finding count without disabling | as needed |
| `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN` | 0.7 | raise to 0.8 to send more to full_rd default | as needed |
| `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE` | true | leave true — failing audits make no findings is worse | n/a |
| `STAGE01_AUTO_DISABLE_ON_ALARM` | true | leave true unless deliberately testing | quarterly drill |

## 5. Roll-forward safety

If Phase 1 produced bad findings on a real project (e.g. the lens
hallucinated something the engineer rejected), do we:

- **Re-run the project?** Yes — same `project_id`, new `version_id`.
  The version_service handles the bookkeeping. Old version is preserved
  but marked as superseded (existing mechanism, used today for
  prepare/retry).
- **Delete the bad findings?** No. They stay in the old version. They
  are auditable history.
- **Tell the engineer?** Yes, via the existing audit-history UI panel.
  Re-audit is engineer-initiated; we don't auto-re-audit.

This contract means rollback is reversible: rolling forward to fixed
Phase 1 doesn't break already-completed projects.

## 6. What we explicitly DON'T do

- We do NOT migrate old `03_findings.json` files to add the new
  `meta.dedup_report` / `meta.document_type` fields. They simply lack
  those fields, which all readers tolerate.
- We do NOT auto-disable Phase 0. Phase 0 is safe by construction; no
  alarm condition justifies an auto-disable on it.
- We do NOT auto-disable on critical-recall regression alarms (AL-21,
  AL-22). Those are warn-only because the right response is human
  investigation, not flag flip.

## 7. References

- [`phase0_rollout.md`](phase0_rollout.md) — Phase 0 deploy steps.
- [`phase1_rollout.md`](phase1_rollout.md) — Phase 1 deploy steps.
- [`../telemetry/production_alerts.md`](../telemetry/production_alerts.md) — alarm → mitigation map.
- [`production_guardrails.md`](production_guardrails.md) — env-var catalog.
- [paid_api_guard.py](../../../backend/app/services/llm/paid_api_guard.py) — runtime-env-read pattern we copy.
