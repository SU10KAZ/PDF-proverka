# False-Positive Monitoring

**Date:** 2026-05-20
**Scope:** How we approximate FP rate in production for the Phase 1
Stage 01 upgrade, given that we have NO ground truth on live projects.

---

## 1. The constraint

Research scored FP against ground truth. Production has none.

From [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md),
across the 24 audited A1-v2 cases:

- `speculative_noise = 0` in **all 24 cases**;
- the +14 FP in the 3-case head-to-head was split:
  - `duplicate_of_gt` ≈ 24% (overlap with GT, missed by substring match);
  - `beyond_gt_useful` ≈ 38% (real engineering value, just not in GT);
  - `wrong_severity` ≈ 38% (real finding, severity could be downgraded).

So in production we cannot expect to detect "true FP" — what we can
detect is **speculative noise**, **low-confidence claims**, and
**post-hoc engineer rejection**. We combine these three signals.

## 2. Three FP signals

### Signal 1 — Speculative keyword (metric E1)

Heuristic: a finding's `problem` text starts with one of:

- `Проверить`
- `Уточнить`
- `Возможно`
- `Вероятно`
- `по-видимому`
- `по всей видимости`

These are typical hedge-words that A1-v2 v2 prompt forbids
([final_prompt_recommendations.md](../../algorithm_research/prompt_optimization/final_prompt_recommendations.md)).
A surge in this count means the model is hedging more, which historically
correlates with bad routing or weak evidence.

**Baseline:** A1-v2 on 24 cases had `speculative_keyword` = 0 in audit.
Real production may have a non-zero baseline because prompt + dedup can
let some hedge findings through. We compute the rolling 28-day count
before any Phase 1 rollout to establish the live baseline.

**Threshold:** rolling 7-day count > A0 baseline + 50% → warn;
+ 100% → page.

**Limitation:** the heuristic is a coarse approximation — `wrong_severity`
findings will look like real findings, so this signal **under-counts** FP.

### Signal 2 — Low-confidence-no-norm (metric E2)

Heuristic: `confidence < 0.5 AND norm field empty (or null)`.

A finding without a norm and with low confidence is, by Stage 01 prompt
contract, supposed to be filtered before output. If it leaks through, the
model is producing weak claims.

**Baseline:** A0 produces these too (rare, ~1-2 per project on the 8-case
A0 baseline — derived from
[final_verdict.md](../../algorithm_research/reports/final_verdict.md)
review). We compute the live baseline in the first 30 days after Phase 0
rollout (Phase 0 doesn't change the count, but it gives us a clean
production-data baseline before Phase 1 ships).

**Threshold:** > 5 per project → warn; rolling 7-day count > 3× baseline → page.

**Limitation:** misses speculative findings that DO cite a norm but
inappropriately.

### Signal 3 — Engineer rejection within 7 days (metric E3)

This is the **only ground-truth-ish** signal available in production.

When the engineer reviews findings in the UI, each finding has an
accept / edit / reject action. We capture the reject event with:

- finding_id
- project_id
- engineer_id (or session_id)
- timestamp
- optional reason_code (`speculative`, `duplicate`, `wrong_severity`, `not_in_scope`, `other`)

This is a NEW capture surface — the existing UI doesn't write engineer
rejections to a journaled file today (only the in-memory state). The
addition is:

- new file `backend/app/data/engineer_review_events.jsonl` (append-only);
- writer: webapp endpoint when engineer marks a finding as rejected;
- reader: `stage01_telemetry_dashboard.py` aggregates the rejection rate.

**Baseline:** unknown. We need to collect 30 days of A0-only data before
the threshold can fire meaningfully. Until then, the alarm is at
`info` severity (logged but not paged).

**Threshold (post-baseline):** per-project > 30% rejected within 7d → warn;
per-week rate > A0 baseline + 25% → warn; + 50% → page.

**Why 7 days:** an engineer typically finishes a project within 1-3 days.
7-day cap catches late edits and gives signal stability.

## 3. Composite FP rate proxy (metric E4)

```
fp_rate_proxy = (E1_count + E2_count) / findings_count_total
```

This is what the dashboard surfaces as a single number. It does NOT
include E3 because E3 is delayed by up to 7 days; instead, E3 is shown
separately as the "engineer-rejected rate" pane.

The two-pane dashboard makes it clear: the **estimate** can move
independently of the **observation**. If they diverge, we know either:

- the heuristic is too loose (estimate high, rejection low), or
- the heuristic is missing real FP (estimate low, rejection high).

## 4. Alarm thresholds (consolidated)

| Signal | Severity | Condition |
|---|---|---|
| E1 spike | warn | rolling 7-day E1 count > A0 baseline + 50% |
| E1 spike | page | rolling 7-day E1 count > A0 baseline + 100% |
| E2 spike | warn | per-project E2 > 5 |
| E2 spike | page | rolling 7-day E2 count > 3× A0 baseline |
| E3 spike | warn | per-project engineer-rejection rate > 30% within 7d |
| E3 spike | warn | rolling 7-day E3 rate > A0 baseline + 25% |
| E3 spike | page | rolling 7-day E3 rate > A0 baseline + 50% |
| E4 spike | warn | rolling 7-day E4 vs trailing 28-day baseline: +25% |
| E4 spike | page | rolling 7-day E4 vs trailing 28-day baseline: +50% (this is the auto-disable trigger — see [production_alerts.md](production_alerts.md)) |

Numbers above are **starting thresholds**, picked from research deltas
(research: +44% FP on 3 cases → set warn at +25%, page at +50%; A1-v2 had
+27% findings, so per-project >30% rejection is a conservative ceiling).
**They will be tuned after the 30-day Phase 0 production baseline lands.**

## 5. Dashboard sketch

Two panes on the Stage 01 telemetry page:

```
┌───────────────────────────────────────────┐
│ FP estimate (heuristic)                   │
│   E1 today: 4   (7-day avg: 3.1)          │
│   E2 today: 2   (7-day avg: 2.7)          │
│   E4 today: 4.1%  (28-day baseline: 3.0%) │
│   [sparkline: E4 over 28 days]            │
└───────────────────────────────────────────┘
┌───────────────────────────────────────────┐
│ Engineer rejection rate (observation)     │
│   today: N/A (within 7-day delay)         │
│   trailing 7-day: 7.4% (baseline: 5.2%)   │
│   [sparkline: rolling 7-day over 60 days] │
└───────────────────────────────────────────┘
```

Top pane updates per-audit; bottom pane is delayed by 7 days for
stability.

## 6. References

- [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) — research baseline (`speculative_noise = 0` in all 24 cases).
- [`metrics_definition.md`](metrics_definition.md) — metric IDs and sources.
- [`production_alerts.md`](production_alerts.md) — alarm wiring.
- [`../rollout/production_guardrails.md`](../rollout/production_guardrails.md) — auto-disable hook (`STAGE01_AUTO_DISABLE_ON_ALARM`).
