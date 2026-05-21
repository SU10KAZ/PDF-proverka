# Review-Load Monitoring

**Date:** 2026-05-20
**Scope:** Monitor engineer review-load to stay within the +20% budget
defined in the Phase 1 gating ([phase0_phase1_validation_report.md §2](../../algorithm_research/reports/phase0_phase1_validation_report.md)).

---

## 1. The constraint

Research showed A1-v2 produces +27% findings (52 → 66) on the 3-case
head-to-head ([FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md)).
Phase 1 gate failed on `human review load ≤ A0 + 20%`. The remediation
plan is per-doc-type opt-in (full_rd OFF, audit_comparison ON) which
mathematically limits the blast radius.

Review-load monitoring confirms in production that the per-doc-type
opt-in actually keeps the budget.

## 2. Three metrics, three views

### View 1 — Findings per project (cheap proxy)

Metric F1 = A1 = `findings_count_total`. Aggregate by week, by document_type,
by discipline.

**Baseline:** computed from 30 days of A0-only outputs. We expect ~9
findings/project for A0 (from the 8-case research; ~52 / 8 = 6.5
findings/project median, but real audits will be longer).

**Alarm:** per-project A1 > 30 → warn ("this audit needs special review
attention"); rolling 7-day mean A1 > A0 baseline + 30% → warn (budget
breach).

The +30% threshold is set higher than the +20% gate because real
production findings include `is_beyond_gt_useful` and useful
`wrong_severity` items that engineers want to see, not just FP. We
deliberately give the production budget headroom over the research gate.

### View 2 — Findings per engineer-week (capacity proxy)

Metric F2 = sum of A1 across all projects an engineer reviewed in a week.
Needs new linkage between project_id and reviewer in the audit-completion
event.

**Baseline:** unknown until we link reviewer_id to projects. Bootstrap by
collecting 8 weeks of A0-only data after the reviewer linkage ships.

**Alarm:** F2 > A0 baseline + 30% → warn.

### View 3 — Review duration per project (observed time, not estimate)

Metric F3 = time engineer spent on the project page in the UI. The
existing `usage_data.json` (see
[backend/app/data/usage_data.json](../../../backend/app/data/usage_data.json))
already tracks session timing per stage; we extend it to capture
"engineer review session" time — the duration the engineer kept the
findings page open between first-load and "mark as complete".

**Baseline:** unknown; needs 30 days of A0 data after the engineer-session
timer ships (small change to the existing usage tracking, no new system).

**Alarm:** rolling p50 of F3 > A0 baseline + 30% → warn.

## 3. Per-discipline split

Some disciplines naturally produce more findings (EOM and AR routinely
have more КРИТ items than KM in research; see
[a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) —
eom_01 produced 25 findings, km_03 produced 12). We split each metric by
discipline so the alarm doesn't false-fire when EOM-heavy weeks naturally
have higher loads.

Disciplines we monitor independently: AR, EOM, KJ, KM, OV, VK, SS (7).

## 4. Combined dashboard

```
┌────────────────────────────────────────────────┐
│ Review-load (week of 2026-05-21)               │
│   Findings/project median: 11 (baseline: 9)    │
│   Findings/eng-week mean:  74 (baseline: 65)   │
│   Review duration p50:    23m (baseline: 22m)  │
│   Phase 1 share: 35% of projects               │
│ By document_type:                              │
│   full_rd:           ~10/p (Phase 1 OFF)       │
│   audit_comparison:  ~14/p (Phase 1 ON)        │
│   specification_only: ~7/p (Phase 1 ON)        │
│   tz_vs_rd:           N/A (case-by-case)       │
└────────────────────────────────────────────────┘
```

## 5. Alarm thresholds

| Metric | Severity | Condition |
|---|---|---|
| F1 per-project | warn | per-project A1 > 30 |
| F1 rolling | warn | rolling 7-day mean > A0 baseline + 30% |
| F2 per-engineer | warn | F2 > A0 baseline + 30% rolling 4-week mean |
| F3 per-project | warn | rolling p50 of F3 > A0 baseline + 30% |

None of these are paged. Review-load is a non-emergency. They
inform tuning of `STAGE01_COMPLETENESS_MAX_FINDINGS` and per-doc-type
opt-in matrix — not auto-mitigation.

## 6. References

- [phase0_phase1_validation_report.md §2](../../algorithm_research/reports/phase0_phase1_validation_report.md) — `human review load ≤ +20%` gate.
- [FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md) — +27% findings count.
- [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) — per-case findings counts vary by discipline.
- [`metrics_definition.md`](metrics_definition.md) §F.
