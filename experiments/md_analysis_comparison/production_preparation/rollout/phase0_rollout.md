# Phase 0 Rollout Plan — Dedup Post-process

**Date:** 2026-05-20
**Goal:** Deploy class + fuzzy dedup as a feature-flagged post-process
inside `findings_merge` step. **No LLM call. No prompt change.**

---

## 1. What Phase 0 actually is

Phase 0 is a Python post-process applied AT THE TAIL of the
`findings_merge` step:

1. `class_dedup.collapse_to_canonical` — collapse findings sharing the
   same `(problem_class, affected_system, page_or_sheet)` key to a single
   canonical representative.
2. `fuzzy_dedup` — collapse findings whose `problem` text similarity
   ≥ `STAGE01_DEDUP_FUZZY_THRESHOLD` (default 0.7).
3. КРИТИЧЕСКОЕ findings bypass fuzzy and only collapse on exact same-class
   match (see [`class_dedup.py`](../dedup/class_dedup.py) `_split_critical_protected`).

It does NOT:

- call any LLM;
- change Stage 01 prompts;
- change the merge logic before the tail;
- change schemas (besides adding `meta.dedup_report`).

Research evidence:

- On A0 production outputs across 8 cases, Phase 0 is a no-op
  ([phase0_phase1_validation_report.md §1.3](../../algorithm_research/reports/phase0_phase1_validation_report.md):
  49 → 49 matched_gt, 3 → 3 missed_crit, 73 → 73 FP).
- On legacy multi-source merged outputs, fuzzy_dedup reduced FP by 18%,
  +20 strict_score ([FINAL_SUMMARY.md §3](../../algorithm_research/reports/FINAL_SUMMARY.md)).
- All 5 gating rules PASS ([phase0_phase1_validation_report.md §2](../../algorithm_research/reports/phase0_phase1_validation_report.md)).

Safety contract verified by
[test_phase0_dedup_safety.py](../../algorithm_research/tests/test_phase0_dedup_safety.py)
(8 cases × 3 variants, no silent КРИТ loss).

## 2. Step-by-step rollout

### Step 0 — Pre-merge checklist (Day −2)

Before the PR is merged on main:

- [ ] `class_dedup.py` and `fuzzy_dedup.py` ported from
      [`../dedup/class_dedup.py`](../dedup/class_dedup.py) into
      `backend/app/services/findings/` (or a new
      `backend/app/services/findings/dedup/` package).
- [ ] Env var `STAGE01_DEDUP_ENABLED` registered in
      `backend/app/core/config.py` (default: **false** at first merge).
- [ ] Env var `STAGE01_DEDUP_FUZZY_THRESHOLD` registered (default: 0.7).
- [ ] `findings_merge` runner: tail call `if STAGE01_DEDUP_ENABLED:
      run_dedup(...)`. When false, behaviour is byte-identical to today.
- [ ] Unit tests ported:
      - `test_class_dedup.py` (existing, in research stand).
      - `test_phase0_dedup_safety.py` (existing).
      - New unit test: `test_dedup_flag_off_is_noop` — flag off ⇒
        `len(findings_out) == len(findings_in)`.
- [ ] CI run: full backend test suite must pass with flag OFF.
- [ ] Hand-spot-check: 1 audit run on a real project with flag OFF —
      `03_findings.json` byte-equal vs before merge.

### Step 1 — Code merged, flag OFF in production (Day 0)

Acceptance gate: no telemetry change, no findings change. Production runs
identically.

Telemetry to watch:

- nothing should move; this is a no-op deploy.
- `backend/app/data/stage01_telemetry.json` daily section begins to exist
  but has dedup section absent (because dedup not run).

Rollback trigger: any unexpected change in `03_findings.json` byte hash
for a project re-audited after the merge.

Rollback action: revert the merge.

Duration: 2 working days (Mon-Tue). Monitor for surprises.

### Step 2 — Flag ON for 5% sampled projects (Day +2)

Implementation: `STAGE01_DEDUP_ENABLED` is read at start of each audit
run; if a random sample hits within 5%, dedup runs for that audit.
Sample selection is deterministic per project_id hash so the same project
always gets the same arm during the canary window.

Acceptance gate (per audit):

- B1 ≥ B5 (count never grows).
- B4 ≥ count of KRIT findings in input (KRIT-protect contract holds).
- B7 == 0 (no dedup errors).
- per-project A1 within ±10% of "what it would have been" (assert against
  pre-dedup snapshot also written to meta).

Telemetry to watch:

- B1, B2, B3, B4, B5, B6, B7 over the day.
- A1, A2 distribution overall (should be flat).
- Engineer rejection rate within 7 days (should be flat or DROP).

Rollback trigger:

- B4 == 0 on any project that had KRIT findings → page (AL-01) → revert
  to flag OFF.
- B7 > 0 on any project → warn (AL-03) → investigate; do not roll back if
  cause is data, not dedup.

Rollback action: set `STAGE01_DEDUP_ENABLED=false` via env-var change
(no redeploy needed if config is read at runtime).

Duration: 5 working days. We need enough samples to see the contract
hold across all 7 disciplines.

### Step 3 — Flag ON for 25% sampled projects (Day +7)

Same sampling logic; widen the sample. Same acceptance gates and
rollback triggers.

Telemetry watch additionally:

- B6 (duplicate_rate) trend — expect 0% on A0 outputs because research
  showed it's a no-op. If we see > 5% per day, investigate (could mean
  the merge layer is now letting duplicates through that the previous
  merge logic was implicitly catching).

Duration: 5 working days.

### Step 4 — Flag ON for 100% (Day +12)

Acceptance gate: previous step was clean. Daily telemetry shows dedup
running but mostly a no-op (a few same-class drops per week).

Telemetry watch:

- AL-01, AL-02, AL-03, AL-04 silence for 14 days.

Rollback action stays available: env var flip → 0% in seconds.

Duration: 14 days of soak.

### Step 5 — Phase 0 considered "stable" (Day +26)

After 14 days at 100% with no warn-or-page alarms, Phase 0 is declared
stable. Removed from the rollout watch-list; alarms remain in production
but at standard rotation.

## 3. Estimated timeline

| Phase | Days | Calendar |
|---|---|---|
| Step 0 pre-merge | 2 | Day -2 to 0 |
| Step 1 flag OFF | 2 | Day 0 to 2 |
| Step 2 5% sample | 5 | Day 2 to 7 |
| Step 3 25% sample | 5 | Day 7 to 12 |
| Step 4 100% soak | 14 | Day 12 to 26 |
| **Total** | **28 days** | |

## 4. Acceptance gates per step (consolidated)

| Step | Gate |
|---|---|
| 0 | tests pass; 1 real-project byte-equal smoke test |
| 1 | no findings change; no telemetry deltas |
| 2 | B4 contract holds; B7 == 0; A1 flat ±10% |
| 3 | Step 2 gates hold at wider sample |
| 4 | 14 days of warn-free operation |
| 5 | merged into standard rotation |

## 5. Why this is safe

- Phase 0 cannot make A0 worse (proven on 8 cases, 3 variants —
  [phase0_phase1_validation_report.md §1.3](../../algorithm_research/reports/phase0_phase1_validation_report.md)).
- КРИТ findings are protected by `_split_critical_protected`
  ([class_dedup.py line 209](../dedup/class_dedup.py)).
- Two kill-switches: `STAGE01_DEDUP_ENABLED` and (per-project) sampling.
- Output is fully observable via `meta.dedup_report` written to
  `03_findings.json`.

## 6. References

- [phase0_phase1_validation_report.md](../../algorithm_research/reports/phase0_phase1_validation_report.md) — §1.3, §2.
- [FINAL_SUMMARY.md](../../algorithm_research/reports/FINAL_SUMMARY.md) — §3.
- [`../dedup/class_dedup.py`](../dedup/class_dedup.py).
- [test_phase0_dedup_safety.py](../../algorithm_research/tests/test_phase0_dedup_safety.py).
- [`../telemetry/production_alerts.md`](../telemetry/production_alerts.md) — AL-01 to AL-04.
- [`production_guardrails.md`](production_guardrails.md) — env-var catalog.
