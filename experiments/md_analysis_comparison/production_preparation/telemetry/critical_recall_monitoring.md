# Critical-Recall Monitoring

**Date:** 2026-05-20
**Scope:** How we monitor the most expensive failure mode of the Phase 1
upgrade — a КРИТИЧЕСКОЕ finding being silently dropped — when there is no
ground truth in production.

---

## 1. The constraint

Research shows A1-v2 IMPROVES critical recall: missed_critical 2 → 1
across 3 cases ([phase0_phase1_validation_report.md §1.4](../../algorithm_research/reports/phase0_phase1_validation_report.md));
13/16 cases caught ALL critical vs A0 5/8 (per-case missed-critical rate
halved) ([FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md)).

So in expectation Phase 1 helps. The question is whether routing,
fallback, or dedup ever drops a КРИТ in production — and we won't know
unless we monitor for proxies.

We cannot measure recall directly. We monitor three proxies + one manual
periodic check.

## 2. Three proxies

### Proxy 1 — Rolling KRIT-rate per discipline (metric A2 derived)

For each discipline (AR, EOM, KJ, KM, OV, VK, SS), compute the rolling
7-day mean count of КРИТ findings per project.

**Baseline:** computed from 30 days of A0-only outputs before any Phase 1
rollout. This gives one number per discipline.

**Alarm:** rolling 7-day mean drops > 30% relative to baseline for a
discipline → warn.

**Why 30%:** A1-v2 on 3 head-to-head cases shifted critical_recall from
88.9% → 94.7% — Phase 1 should not make this number FALL. A 30% drop is
large enough to rule out single-project noise (one quiet day) and small
enough to catch genuine regression before two weeks of audits drift.

**Limitation:** projects vary in size. A discipline that audited only
3 projects on a quiet day will trip the alarm by random variation. We
require ≥ 8 projects in the rolling window before the alarm is eligible.

### Proxy 2 — Rolling KRIT-rate per document_type

Same idea as proxy 1 but split by `document_type`. This catches
Phase-1-specific regressions:

- if `audit_comparison` KRIT-rate falls, Phase 1 routing is leaking;
- if `full_rd` KRIT-rate falls, current_method (Opus) regressed
  independently of Phase 1 (because full_rd has Phase 1 OFF by default).

**Baseline:** A0 30-day baseline by document_type. `full_rd` will have
the most data (~17 / 24 cases in research → ~70% of production);
`audit_comparison`, `tz_vs_rd`, `specification_only` will be sparser, so
require ≥ 5 projects in the rolling 14-day window before alarm is
eligible for those types.

**Alarm:** rolling 14-day mean drops > 30% relative to baseline for a
doc_type → warn.

### Proxy 3 — Per-project KRIT count vs project size (MD page count)

The MD page count comes from `document_graph.json` (`pages` array length).
We expect roughly linear scaling: more pages → more KRIT findings.

**Alarm:** per-project KRIT count = 0 AND page_count > 30 → info (the
audit completed with no critical findings on a non-trivial project; worth
a manual spot-check, not necessarily a regression).

**Why this is informational, not paged:** there are real projects with
zero KRIT findings. We just want a queue of them to spot-check.

## 3. Manual periodic sample (the ground-truth-ish check)

Once a month:

1. Sample 5 random completed projects from the last 30 days.
2. Pick at least one of each document_type (if available in window).
3. Have a senior engineer review the project's `03_findings.json` and
   produce a quick "would I have caught more KRIT findings if I were
   doing this manually" 1-page note.
4. Compute month-over-month a manual `critical_recall` score
   (manual_KRIT_count / (manual_KRIT_count + production_KRIT_count_in_overlap)).
5. Log to `backend/app/data/manual_recall_audits.jsonl` (new file).

Numbers from this manual check ground the proxy alarms — if proxy 1 says
"KRIT rate steady" and manual check says "we missed 3 critical findings
this month", we know proxies are missing real regression.

**Cost estimate:** 5 projects × ~30 min senior-engineer time = ~2.5 hours
per month. Acceptable.

## 4. Alarm wiring

| Proxy | Severity | Condition |
|---|---|---|
| Proxy 1 | warn | rolling 7-day KRIT-mean drops > 30% in discipline (≥ 8 projects window) |
| Proxy 2 | warn | rolling 14-day KRIT-mean drops > 30% in document_type (≥ 5 projects window) |
| Proxy 3 | info | per-project KRIT == 0 AND page_count > 30 |
| Manual check | manual | quarterly review |

Page (vs warn) thresholds for proxy 1 and 2 are intentionally NOT set:
we don't have enough evidence to know what "definitely a regression" looks
like on these proxies. After 90 days of production data with Phase 0 +
Phase 1 on `audit_comparison` we'll revisit and likely set
`drop > 50% → page`.

## 5. Why this is enough

The research data strongly supports Phase 1 IMPROVING critical recall —
not weakening it. We are monitoring for the **unexpected**, not the
expected. The proxy + manual combination gives:

- **Fast signal** (per-discipline 7-day) for blatant regression.
- **Slower signal** (per-doc-type 14-day) for routing-specific regression.
- **Ground-truth signal** (monthly manual check) for the bias all proxies
  carry.

Auto-disable on this signal **is not used** (unlike FP regression). A
critical recall regression should be looked at by a human, not auto-mitigated
by flipping a flag. The right response is to investigate
(could be Sonnet outage, dedup bug, document_type misrouting) and decide
manually.

## 6. References

- [phase0_phase1_validation_report.md §1.4](../../algorithm_research/reports/phase0_phase1_validation_report.md) — A1-v2 critical_recall 88.9 → 94.7%.
- [FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md) — A1-v2 missed-critical RATE is halved.
- [`metrics_definition.md`](metrics_definition.md) §A — base metrics.
- [`production_alerts.md`](production_alerts.md) — alarm routing.
