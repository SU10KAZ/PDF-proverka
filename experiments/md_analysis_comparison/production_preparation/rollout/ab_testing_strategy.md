# A/B Testing Strategy — Phase 1

**Date:** 2026-05-20
**Scope:** How we measure whether Phase 1 (Sonnet completeness lens) is
actually better in production. Three modes: shadow, canary, and
N-project A/B.

---

## 1. The decision we need to make

The research established Phase 1 helps on `audit_comparison` and
`specification_only` and hurts on `full_rd` strict_score
([FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md)).
That research was on a small dataset (24 synthetic-ish cases). The
production A/B answers:

- **For `audit_comparison`** — does the +26 strict_score lift in research
  translate to engineer-accepted findings in production?
- **For `specification_only`** — same question; no A0 baseline in
  research.
- **For `tz_vs_rd`** — does the lens still hurt (research: A0 80.0 →
  A1-v2 36.0 on 1 case)?
- **For `full_rd`** — kept off; no A/B planned (research: −26.7
  strict_score, +114 FP across 11 cases).

## 2. Three modes

### 2.1 Shadow mode

- Both legs run (A0 + completeness lens).
- Engineer sees A0-only output.
- Combined A1-v2 output written to a separate file
  `_output/stage01_shadow.json` per audit.
- Cost = ~150% of A0 (Sonnet leg is parallel but adds cost).

**Purpose:**

- Collect 14 days of production data with no engineer-facing risk.
- Validate that lens error rate, latency, and KRIT-protect contract
  hold in production data.
- Calibrate FP heuristic baselines (E1, E2, E4) against the live mix.

**What we measure:**

- C6, C7, C8 (lens duration, error rate, fallback fires).
- B4 (KRIT-protect) on the shadow output.
- A1, A2 distribution differences between A0 and shadow A1-v2.
- G1, G5 cost.

**Decision gate to exit shadow:**

- 14+ days of telemetry.
- C7 < 5% (lens stable).
- G5 within +70% of A0 cost on the same project_id set.
- No AL-01 / AL-02 / AL-05 / AL-06 / AL-07 alarms.

### 2.2 Canary mode (per-document_type)

- For projects with detected `document_type` in
  `STAGE01_COMPLETENESS_BY_DOC_TYPE` ON:
  - run Phase 1; surface findings to engineer with explicit UI flag
    "Phase 1 — Sonnet completeness lens applied".
- For all other projects: A0 only.

**Purpose:**

- Get engineer-feedback signal (E3 rejection rate) on real Phase 1
  findings.
- Validate the UI experience.
- Limit blast radius to ~13% of production (3/24 research split for
  `audit_comparison` + `specification_only`).

**What we measure:**

- E3 rejection rate per finding-source (engineer rejects more from lens
  than from current_method? same?).
- F1, F2, F3 review-load.
- A5 `is_beyond_gt_useful` tagging rate.
- Manual checkpoint reviews at Day +12 and Day +19 of Step 3.

**Decision gate to exit canary:**

- Engineer rejection rate on each enabled doc_type ≤ A0 + 25%.
- No AL-21, AL-22 fires.
- Manual review sign-off.

### 2.3 A/B (same project, both versions)

When an engineer explicitly requests A/B comparison on a project
(button in the UI: "Run A/B"), the system:

1. Runs A0 audit → produces version_id `vA`.
2. Runs Phase 1 audit → produces version_id `vB`.
3. UI shows both versions side-by-side.
4. Engineer marks each finding as accepted/rejected on each version.
5. Engineer picks a "preferred" version at end.
6. Both selections and the preferred-version pick are written to
   `backend/app/data/ab_test_results.jsonl`.

**Purpose:**

- Direct comparison on the same project.
- Captures engineer judgment (the gold standard we don't have in
  research).

**Why this is opt-in and rare:**

- 2× LLM cost per A/B project.
- ~30-45 min engineer time per A/B vs ~20-25 for a single review.
- Only meaningful when engineer is willing to do the comparison work.

**What we measure:**

- "vB preferred" rate per document_type.
- KRIT finding overlap between vA and vB (proxy for recall difference).
- Engineer effort (F3) on vA vs vB.

### 2.4 Sample size math

Research detected ~18% strict_score difference on 3 cases at extremely
high variance. For production A/B to detect a 10% strict_score-analog
difference at 80% power assuming engineer-pick is the outcome (binomial
distribution, p=0.55 vs p=0.50 null, two-sided):

- ~ 400 paired samples for 10% delta detection.
- ~ 100 paired samples for 20% delta detection.

Realistic A/B volume:

- `audit_comparison`: ~3% of production (3/24 in research).
- `specification_only`: ~12% (3/24).
- `tz_vs_rd`: ~8% (2/24).

For `audit_comparison` (~3%) to gather 100 A/B samples at typical 50
projects/week throughput = 100 / (50 × 0.03) ≈ 67 weeks → **15 months**
to reach the smaller-delta threshold.

**Practical answer:** N-project A/B is a research-grade signal we collect
opportunistically. Canary mode (E3 rejection rate) is the operational
signal. The two complement each other.

## 3. What metrics gate the A/B verdict

Verdict for "promote Phase 1 from canary to default-on for this
document_type":

| Metric | Threshold | Window |
|---|---|---|
| E3 (engineer rejection) | ≤ A0 baseline + 25% | rolling 28-day, ≥ 30 projects |
| A2 KRIT rate | ≥ A0 baseline − 10% (i.e. recall not worse) | rolling 28-day |
| H3 (A/B preference) | > 55% prefer Phase 1 | minimum 10 A/B samples |
| AL-05 to AL-22 alarms | none firing for 28 days | rolling |

When all four hold for a document_type, that type is considered
"validated" and Phase 1 stays on by default for it.

Verdict for "demote Phase 1 from canary back to shadow for this
document_type":

| Metric | Threshold | Action |
|---|---|---|
| E3 spike (AL-15 / AL-16) | > 30% per-project OR rolling > +25% | demote |
| A2 KRIT drop (AL-21 / AL-22) | drops > 30% in this doc_type | demote and investigate |
| H3 preference | < 40% prefer Phase 1 | demote and investigate |

## 4. Storage and analysis

Three new append-only files:

- `backend/app/data/engineer_review_events.jsonl` — per-finding accept/
  reject events (used by E3 metric and canary mode).
- `backend/app/data/ab_test_results.jsonl` — per-A/B-project paired
  outcomes (used by H3 metric).
- `backend/app/data/manual_recall_audits.jsonl` — monthly senior-engineer
  critical-recall spot-check ([`../telemetry/critical_recall_monitoring.md`](../telemetry/critical_recall_monitoring.md) §3).

All three are jsonl, same pattern as the existing
`paid_api_events.jsonl` and `paid_cost_events.jsonl`.

## 5. Why three modes, not one big A/B

- Shadow gives us **system-stability** signal without engineer involvement
  (we need to know it works before we ask anyone to look).
- Canary gives us **operational-quality** signal at scale (E3 across many
  projects, fast feedback).
- A/B gives us **engineer-judgment** signal (slow, expensive, but it's
  the ground-truth-ish anchor that calibrates the heuristic E3 alarms).

Picking only one would either be too slow (A/B-only) or too unreliable
(canary-only without baseline calibration).

## 6. References

- [FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md) — per-doc-type research result.
- [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) — per-case FP audit baseline.
- [`phase1_rollout.md`](phase1_rollout.md) — where shadow / canary fit in the rollout steps.
- [`../telemetry/fp_monitoring.md`](../telemetry/fp_monitoring.md) — E3 details.
- [`../telemetry/critical_recall_monitoring.md`](../telemetry/critical_recall_monitoring.md) — A2 and manual recall check.
