# Production Alerts — Stage 01 Upgrade

**Date:** 2026-05-20
**Scope:** Single source of truth for all production alarms tied to the
Phase 0 / Phase 1 Stage 01 upgrade. Each row is a single alarm; severity,
condition, and action are explicit.

---

## 1. Severity legend

| Severity | What it means | Channel |
|---|---|---|
| info | surface on dashboard, no notification | dashboard pane |
| warn | engineering Slack ping, look-but-don't-stop-the-world | Slack #pdf-audit-eng |
| page | on-call notification + optional auto-mitigation | on-call rotation |

## 2. Alarm table

| # | Name | Metric ref | Severity | Condition | Action |
|---|---|---|---|---|---|
| AL-01 | dedup_silent_critical_drop | B4 | page | КРИТ in input AND B4 == 0 | Auto-mitigation: set `STAGE01_DEDUP_ENABLED=false`. Investigate; cannot resume until manually re-enabled. |
| AL-02 | dedup_mass_drop | B5 vs B1 | page | B5 == 0 AND B1 > 0 in any single project | Auto-mitigation: same as AL-01. |
| AL-03 | dedup_error | B7 | warn | > 0 dedup errors in a day | Look at log; dedup must not crash. Likely a malformed finding from upstream; fix the data, not the dedup. |
| AL-04 | dedup_over_collapse | B2 or B3 | warn | B2 or B3 > 30% of B1 in any single project | Investigate — fuzzy threshold may be too low (e.g. 0.7 too aggressive). Consider raising `STAGE01_DEDUP_FUZZY_THRESHOLD` to 0.8. |
| AL-05 | completeness_lens_failure_spike | C7 | warn | > 5% lens error rate rolling 24h | Check Sonnet API status; check `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE` is ON. |
| AL-06 | completeness_lens_failure_spike_high | C7 | page | > 15% lens error rate rolling 24h | Auto-mitigation: set `STAGE01_COMPLETENESS_LENS_ENABLED=false`. Likely a Sonnet outage; all projects continue with A0 only. |
| AL-07 | completeness_cap_breach | C3 | page | C3 > `STAGE01_COMPLETENESS_MAX_FINDINGS` in any project | Investigate: cap enforcement bug in completeness_runner. No auto-mitigation. |
| AL-08 | completeness_silently_skipped | C2 / C1 ratio | warn | per-day (applied / enabled) < 0.9 | Routing or precondition is silently skipping the lens. Investigate. |
| AL-09 | document_type_low_confidence | D3 | warn | rolling 7-day > 0.2 | More than 20% of projects fall back to full_rd; Phase 1 silently turning off. Tune detector or expand `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN`. |
| AL-10 | document_type_distribution_drift | D1 distribution | warn | per-day distribution drift > 20 p.p. vs trailing 30d | Detector may be malfunctioning. Look at examples on the drifting class. |
| AL-11 | fp_speculative_spike | E1 | warn | rolling 7-day E1 > A0 baseline + 50% | Investigate — prompts may have been changed; speculative findings rising. |
| AL-12 | fp_speculative_spike_high | E1 | page | rolling 7-day E1 > A0 baseline + 100% | Investigate; consider disabling completeness lens. |
| AL-13 | fp_lowconf_spike | E2 | warn | per-project E2 > 5 | Check this audit's findings; weak-evidence findings leaking. |
| AL-14 | fp_lowconf_spike_high | E2 | page | rolling 7-day E2 > 3× A0 baseline | Investigate the lens prompt; possible regression. |
| AL-15 | engineer_rejection_per_project | E3 | warn | per-project engineer-rejection > 30% within 7 days | Manual: review what got rejected; pattern in problem_class? |
| AL-16 | engineer_rejection_trend | E3 | warn | rolling 7-day rejection rate > A0 baseline + 25% | Engineers are silently disagreeing more often. Tune prompts. |
| AL-17 | engineer_rejection_trend_high | E3 | page | rolling 7-day rejection rate > A0 baseline + 50% | Auto-mitigation: see AL-20. |
| AL-18 | fp_composite_spike | E4 | warn | rolling 7-day E4 > 28-day baseline + 25% | Composite FP estimate climbing; look at E1/E2 panes. |
| AL-19 | fp_composite_spike_high | E4 | page | rolling 7-day E4 > 28-day baseline + 50% | Auto-mitigation: see AL-20. |
| AL-20 | auto_disable_phase1 | E3 or E4 high | page | AL-17 OR AL-19 fires AND `STAGE01_AUTO_DISABLE_ON_ALARM = true` | Set `STAGE01_COMPLETENESS_LENS_ENABLED=false`. Phase 0 dedup stays ON (safe). Project re-audit not triggered; engineers continue with A0-only findings on new audits. Page on-call. |
| AL-21 | critical_recall_discipline_drop | A2 derived | warn | rolling 7-day KRIT-mean drops > 30% in a discipline (≥ 8 projects window) | Manual: investigate which projects in this discipline lost their KRIT findings. |
| AL-22 | critical_recall_doctype_drop | A2 derived | warn | rolling 14-day KRIT-mean drops > 30% in a document_type (≥ 5 projects window) | Manual: same investigation, sliced by document_type. |
| AL-23 | review_load_per_project | F1 | warn | per-project A1 > 30 | Engineer triage required. No mitigation. |
| AL-24 | review_load_trend | F1 rolling | warn | rolling 7-day mean > A0 baseline + 30% | Tune `STAGE01_COMPLETENESS_MAX_FINDINGS` down. |
| AL-25 | cost_blow_up | G5 | warn | rolling 7-day mean USD/project > A0 baseline + 70% | Exceeded research-stated cost budget. Investigate Sonnet duration. |
| AL-26 | cost_blow_up_high | G5 | page | rolling 7-day mean USD/project > A0 baseline + 100% | Auto-mitigation: same as AL-20 (disable Phase 1 keeps Phase 0). |
| AL-27 | daily_limit_approaching | G5 + paid_api_guard daily limit | warn | `today_spent_usd > 0.8 × PAID_API_DAILY_LIMIT_USD` | Existing kill-switch already enforces hard limit; warn is to give engineering heads-up before hard block. |
| AL-28 | wall_clock_p95_blow_up | G1 | warn | rolling 7-day p95 > A0 baseline + 100% | Investigate Sonnet latency. |

## 3. Auto-mitigation contract

Auto-mitigation is gated by a single global env var:

```
STAGE01_AUTO_DISABLE_ON_ALARM = true | false   (default: true)
```

When true, AL-01, AL-02, AL-06, AL-17, AL-19, AL-26 may automatically
flip `STAGE01_COMPLETENESS_LENS_ENABLED` or `STAGE01_DEDUP_ENABLED` to
false. Auto-mitigation:

- writes a `paid_api_blocked_events.jsonl`-style entry to
  `backend/app/data/stage01_alarm_events.jsonl` (NEW journal file);
- sends a page to on-call;
- writes a sticky banner to the dashboard saying "Phase 1 auto-disabled
  at HH:MM:SS — investigate alarm <ID>".

Re-enable is **manual** — the engineer flips the env var back on after
investigation.

## 4. Why these thresholds

Numbers are chosen as follows:

- **Drop-by-30%** thresholds (critical_recall, dedup) — large enough to
  rule out single-project random noise, small enough to catch genuine
  regression within a week.
- **FP +25 / +50%** thresholds — research +44% on 3 cases; we set warn
  at +25 to catch sooner, page at +50 to limit auto-mitigation false fires.
- **Engineer-rejection thresholds** — set conservatively because the
  baseline isn't known yet (E3 metric is new). Will be retuned after 30
  days of production data.
- **Cost +70 / +100%** — research showed +49% wall-clock and ~+50-70%
  LLM cost ([phase0_phase1_validation_report.md §5](../../algorithm_research/reports/phase0_phase1_validation_report.md)).
  Warn at the research ceiling, page at +100%.

All thresholds are settable env vars (e.g.
`STAGE01_ALARM_FP_E4_PCT_WARN=25`, `STAGE01_ALARM_FP_E4_PCT_PAGE=50`)
so they can be tuned without code change.

## 5. Where alarms live in code

Hypothetical implementation:

- `backend/app/services/stage01_alarms.py` (new) — pure function
  `evaluate_alarms(daily_telemetry: dict) -> list[AlarmEvent]`. No state.
- `backend/app/api/routers/stage01_telemetry.py` (new) — endpoint
  `GET /api/stage01/alarms/recent` for the dashboard.
- background sweep: a new cron-like job in `backend/app/pipeline/manager.py`
  runs `evaluate_alarms` every 10 minutes and writes to
  `stage01_alarm_events.jsonl`.

(Implementation belongs to the integration task, not this prep package.)

## 6. References

- [`metrics_definition.md`](metrics_definition.md) — all metric IDs.
- [`fp_monitoring.md`](fp_monitoring.md) — E1-E4 details.
- [`critical_recall_monitoring.md`](critical_recall_monitoring.md) — A2 proxies.
- [`review_load_monitoring.md`](review_load_monitoring.md) — F1-F3.
- [`../rollout/production_guardrails.md`](../rollout/production_guardrails.md) — full env-var catalog.
