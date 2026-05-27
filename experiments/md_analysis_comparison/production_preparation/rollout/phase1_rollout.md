# Phase 1 Rollout Plan — Sonnet Completeness Lens

**Date:** 2026-05-20
**Goal:** Deploy the Sonnet `completeness` lens + discipline checklist +
`document_type` routing in production, opt-in by document_type, feature-
flag-gated. Phase 1 sits on top of Phase 0 — Phase 0 must be at 100% and
stable before Phase 1 starts step 2.

---

## 1. What Phase 1 actually is

Phase 1 adds three new pieces to Stage 01:

1. **Document_type detection** — a deterministic detector
   ([`../schemas/document_type_design.md`](../schemas/document_type_design.md)
   when written) classifies each project as one of
   `full_rd`, `audit_comparison`, `tz_vs_rd`, `specification_only`.
   Confidence < `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN` → fallback to
   `full_rd` (Phase 1 OFF).
2. **Sonnet completeness lens** — a parallel LLM leg using
   `claude-sonnet-4-6` that produces completeness findings against a
   discipline checklist
   ([`../checklists/`](../checklists/) when written). Output capped at
   `STAGE01_COMPLETENESS_MAX_FINDINGS`. On lens failure, falls back to
   A0 (current_method only) — verified by
   [test_fallback_to_a0.py](../../algorithm_research/tests/test_fallback_to_a0.py).
3. **Per-doc-type opt-in matrix** — `STAGE01_COMPLETENESS_BY_DOC_TYPE`:
   `{ "audit_comparison": true, "specification_only": true,
      "tz_vs_rd": false, "full_rd": false }`.
   `tz_vs_rd` can be flipped to `true` for hand-picked projects via
   project_info.json (see [`routing_rules.md`](routing_rules.md)).

Phase 1 enters the merge step exactly like current_method does and
flows through Phase 0 dedup at the merge tail. Both legs run in
PARALLEL — wall-clock penalty is ~+49% on the longest leg, not 2×
([phase0_phase1_validation_report.md §5](../../algorithm_research/reports/phase0_phase1_validation_report.md)).

Research evidence:

- `audit_comparison` (3 cases): A0 25.1 → A1-v2 51.4 strict_score
  ([FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md)).
- `specification_only` (1 case): A1-v2 22.0, 3/3 GT found, no A0
  baseline.
- `tz_vs_rd` (1 case): A0 80.0 → A1-v2 36.0 — **worse** on 1 case.
- `full_rd` (11 cases): A0 49.8 → A1-v2 23.1 — **worse** (+114 FP).
- Per-case missed-critical rate is **halved** on Phase 1 (13/16 caught
  all KRIT vs A0 5/8).
- 0 speculative noise across all 24 cases
  ([a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md)).

## 2. Step-by-step rollout

### Step 0 — Pre-merge checklist (Day −5)

- [ ] Phase 0 has been at 100% for ≥ 14 days with no warn/page alarms.
- [ ] `document_type_detector.py` ported from research detector
      (rules + heuristic; deterministic), unit-tested.
- [ ] Completeness lens runner
      `backend/app/pipeline/stages/text_analysis/completeness_runner.py`
      (new file) ported from
      [_common.py run_lens](../../algorithm_research/runners/_common.py).
- [ ] Discipline checklists copied to
      `backend/app/data/discipline_checklists/{AR,EOM,KJ,KM,OV,VK,SS,MULTI}.md`.
- [ ] All env vars registered in `backend/app/core/config.py`
      (`STAGE01_COMPLETENESS_LENS_ENABLED=false` by default,
      `STAGE01_COMPLETENESS_BY_DOC_TYPE`,
      `STAGE01_COMPLETENESS_MAX_FINDINGS=10` (6 for full_rd),
      `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN=0.7`,
      `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE=true`).
- [ ] Tests ported:
      - `test_document_type_routing.py`
      - `test_fallback_to_a0.py`
      - `test_completeness_not_applicable.py`
      - `test_a1_v2_schema.py`
      - New: `test_completeness_disabled_is_noop` (flag OFF ⇒ A0-only output).
      - New: `test_completeness_cap_enforced` (lens output never exceeds cap).
- [ ] Telemetry surfaces (metrics C, D, E, F, G in
      [`../telemetry/metrics_definition.md`](../telemetry/metrics_definition.md)) wired into
      `stage01_telemetry.json`.
- [ ] Alarms AL-05 to AL-22 from
      [`../telemetry/production_alerts.md`](../telemetry/production_alerts.md)
      registered.
- [ ] All Phase 0 alarms still firing correctly.

### Step 1 — Merge, flag OFF (Day 0)

Acceptance gate: byte-identical Stage 01 outputs vs Phase-0-only baseline
on 5 spot-checked projects.

Telemetry to watch:

- `stage01_meta.json` exists per project but has
  `completeness_lens.applied = false`.
- `document_type` field populated (detector runs even when lens OFF —
  this is intentional to give us 30 days of detector telemetry before
  Phase 1 turns on).

Rollback trigger: detector raises an exception on > 1% of projects
(should never happen — it's deterministic).

Rollback action: revert.

Duration: 5 working days. We want 30+ projects audited to see detector
behavior live.

### Step 2 — Shadow mode (Day +5)

`STAGE01_COMPLETENESS_LENS_ENABLED=true` AND `STAGE01_COMPLETENESS_SHADOW=true`.

In shadow mode:

- Both legs run.
- Engineer sees A0-only findings in the UI (production behavior).
- Combined A1-v2 findings are written to `_output/stage01_shadow.json`
  (separate file, not surfaced).
- All telemetry (C, D, F, G) is captured on the shadow output.

Acceptance gate (per day):

- `completeness_lens.error_rate` (C7) < 5%.
- `completeness_lens_duration_ms` (C6) p95 < 240s.
- Composite cost (G5) within 70% of A0-only cost on the same projects.
- No KRIT-protect violations (B4).

Manual review checkpoint at Day +12 and Day +19:

- senior engineer picks 10 random projects, compares
  `03_findings.json` (A0) vs `_output/stage01_shadow.json` (A1-v2).
- subjective score: is shadow output equivalent / better / worse on each?

Rollback trigger: cost > 100% over A0; lens error rate > 15%; manual
review concludes A1-v2 worse on > 30% of audited samples.

Rollback action: set `STAGE01_COMPLETENESS_LENS_ENABLED=false`.

Duration: 14 working days.

### Step 3 — Opt-in `audit_comparison` + `specification_only` only (Day +19)

`STAGE01_COMPLETENESS_BY_DOC_TYPE = {"audit_comparison": true,
"specification_only": true, "tz_vs_rd": false, "full_rd": false}`.

Surface A1-v2 findings to engineers only on projects whose detected
`document_type` is `audit_comparison` or `specification_only` AND
confidence ≥ `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN`.

On all other projects, Phase 1 stays in shadow (or is skipped, depending
on cost budget — see `STAGE01_SHADOW_ON_DISABLED_DOCTYPE` flag).

Acceptance gate after 2 weeks at this step:

- engineer rejection rate (E3) on `audit_comparison` ≤ A0 baseline + 25%.
- engineer rejection rate on `specification_only` ≤ A0 baseline + 25%.
- No AL-05 to AL-22 fires.

Manual review checkpoint at Day +26 and Day +33:

- Senior engineer samples 5 `audit_comparison` and 5 `specification_only`
  Phase 1 projects; signs off in writing that the findings are at least
  as good as A0 would have produced.

Rollback trigger: rejection rate breach OR any KRIT-recall regression
(AL-21, AL-22) on these document_types.

Rollback action: per-type flip in `STAGE01_COMPLETENESS_BY_DOC_TYPE` —
e.g. just turn `audit_comparison` off, keep `specification_only` if it's
still clean.

Duration: 14 working days at minimum.

### Step 4 — Discipline-by-discipline expansion (Day +33)

For each of AR, EOM, KJ, KM, OV, VK, SS, individually flip the discipline
into "Phase 1 also on full_rd in this discipline IF document_type is
audit_comparison or specification_only" (which the matrix already
allows). The new switch is per-discipline:

```
STAGE01_COMPLETENESS_DISCIPLINE_ALLOWLIST = "AR,EOM"  # comma-separated
```

Start with the disciplines that produced the cleanest A1-v2 outputs in
research (per FP audit numbers — see
[a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md)):

- **EOM** (eom_01 high duplicate_of_gt, indicating good GT overlap;
  eom_02, eom_03 have moderate FP) — start with EOM after 14-day soak.
- **AR** next (ar_01 14 dup_of_gt, ar_02 9, ar_03 14 — strong overlap).
- KJ, OV, VK, SS, KM rolled in one at a time, 14 days apart, with
  per-discipline acceptance gate.

Per-discipline acceptance:

- 30+ projects audited in that discipline at Phase 1.
- E3 rejection rate ≤ A0 baseline + 25%.
- No A2 / KRIT regression (AL-21).

Duration: ~12 weeks total (7 disciplines × ~14 days each, partial overlap
if no regressions).

### Step 5 — Opt-in `tz_vs_rd` for picked cases (Day +∞ from Step 4 start)

`tz_vs_rd` is the hardest case — only 1 research case, and it was
**worse** (A0 80.0 → A1-v2 36.0). Conservative policy:

- Phase 1 stays off by default for `tz_vs_rd`.
- Engineers can opt-in per-project via project_info.json:
  ```json
  {"phase1_override": {"completeness": true, "reason": "..."}}
  ```
- Each override generates an audit-log entry; weekly review of these
  overrides looks at outcome.

After 20 opted-in projects with E3 ≤ A0 baseline + 25%, we revisit the
default for `tz_vs_rd`.

### Step 6 — Revisit `full_rd` (NOT in this rollout)

Research showed `full_rd` is +114 FP across 11 cases. This rollout does
NOT enable Phase 1 on `full_rd`. Required before this is even considered:

- `STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD` is tuned to 6 (currently
  set; further tuning may be needed).
- `is_beyond_gt_useful` tagging is reliable — measured by metric A5
  rolling 7-day ratio > 15%.
- Another research round (24+ full_rd cases) with the cap-tuned prompt
  confirms strict_score not worse than A0.

This is a separate task. Not in this rollout.

## 3. Approval matrix

| Step | Sign-off required by |
|---|---|
| 0 Pre-merge | tech lead + senior engineer |
| 1 Merge flag OFF | tech lead |
| 2 Shadow mode | tech lead |
| 3 Opt-in `audit_comparison` + `specification_only` | tech lead + senior engineer (review 10 sample projects) |
| 4 Per-discipline expansion | senior engineer per discipline |
| 5 `tz_vs_rd` per-project | engineer doing the audit (project_info.json override) |
| 6 `full_rd` | tech lead + senior engineer + new research round |

## 4. Estimated timeline

| Step | Days | Calendar |
|---|---|---|
| 0 Pre-merge | 5 | Day -5 to 0 |
| 1 Flag OFF, detector live | 5 | Day 0 to 5 |
| 2 Shadow mode | 14 | Day 5 to 19 |
| 3 Opt-in 2 doc_types | 14 | Day 19 to 33 |
| 4 Per-discipline expansion | ~90 | Day 33 to ~123 |
| 5 tz_vs_rd opt-in pool | open-ended | parallel to step 4 |
| 6 full_rd | not planned | requires new research |
| **Phase 1 launch (step 3 complete)** | ~33 working days from start | |
| **Full discipline coverage (step 4 complete)** | ~123 working days from start | |

## 5. Per-step telemetry windows

- **Step 2 (shadow):** daily refresh of metrics C6, C7, C8, G1, G5;
  weekly refresh of A2 distributions on the shadow output.
- **Step 3 (opt-in 2 doc_types):** daily refresh of E3 (engineer
  rejections); weekly refresh of A2, F1, F2.
- **Step 4 (per-discipline):** rolling 14-day metrics A2 and E3 per
  discipline; rolling 14-day F1; AL-21 monitored.

## 6. Why this rollout is safe

- Phase 0 must already be at 100% and stable.
- Phase 1 is opt-in by `document_type`; full_rd (the majority — research
  17/24 cases) stays on A0 by default.
- Fallback to A0 on lens failure is the default, kill-switched by
  `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE`.
- Auto-disable via `STAGE01_AUTO_DISABLE_ON_ALARM` covers the runaway
  scenarios (AL-17, AL-19, AL-26).
- Shadow mode collects 14 days of production data before any engineer-
  facing change.
- Per-discipline expansion is sequential — one discipline at a time so
  blast radius is bounded.

## 7. References

- [phase0_phase1_validation_report.md](../../algorithm_research/reports/phase0_phase1_validation_report.md) — §1.4, §2, §5.
- [FINAL_SUMMARY.md](../../algorithm_research/reports/FINAL_SUMMARY.md) — §4 per-doc-type matrix.
- [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) — per-case FP breakdown.
- [test_fallback_to_a0.py](../../algorithm_research/tests/test_fallback_to_a0.py).
- [`phase0_rollout.md`](phase0_rollout.md) — prerequisite.
- [`production_guardrails.md`](production_guardrails.md) — env-var catalog.
- [`routing_rules.md`](routing_rules.md) — per-project overrides for `tz_vs_rd`.
- [`ab_testing_strategy.md`](ab_testing_strategy.md) — shadow + canary detail.
