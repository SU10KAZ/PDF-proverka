# Metrics Definition — Stage 01 Upgrade

**Date:** 2026-05-20
**Scope:** Single tabular reference. Each row is one metric. One row, one
source. No metric appears twice.

Aggregation levels:

- **PP** = per-project (one value per audit run, written to
  `<project>/_output/stage01_meta.json`).
- **PD** = per-day rollup (aggregated into `backend/app/data/stage01_telemetry.json`
  on audit completion).
- **PW** = per-week trend (computed by `stage01_telemetry_dashboard.py`
  from PD data).
- **AB** = A/B shadow (only when shadow mode ON; see
  [`../rollout/ab_testing_strategy.md`](../rollout/ab_testing_strategy.md)).

Alert severity legend:

- **info** = surface in dashboard; no notification.
- **warn** = engineering Slack ping.
- **page** = on-call notification + auto-mitigation hook.

References at the bottom.

---

## A. Findings volume and severity

| # | Metric | Definition | Unit | Source (hypothetical path) | Agg | Alert threshold | Why we care |
|---|---|---|---|---|---|---|---|
| A1 | `findings_count_total` | Number of findings after dedup tail | int | `backend/app/pipeline/stages/findings_merge/runner.py` | PP, PD, PW | per-project > 30 → warn; per-week median > A0 baseline +30% → warn | Review-load budget guard (research target: ≤ A0 + 20%, [FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md)) |
| A2 | `findings_count_by_severity` | Count by `severity` (КРИТ / ЭКОН / ЭКСПЛ / РЕКОМ / ПРОВЕРИТЬ_ПО_СМЕЖНЫМ) | dict[str,int] | findings_merge runner | PP, PD, PW | КРИТ < 0.5× rolling 7-day baseline per discipline → warn | Proxy for critical recall (see [`critical_recall_monitoring.md`](critical_recall_monitoring.md)) |
| A3 | `severity_distribution_pct` | A2 normalised to percentages | dict[str,float] | derived from A2 | PD, PW | КРИТ share drops > 30% in a discipline → warn | Distribution shift signals routing/lens regression |
| A4 | `avg_findings_per_project` | Mean A1 across the day / week | float | derived | PD, PW | rolling 7-day mean > A0 baseline +30% → warn | Review-load (see [`review_load_monitoring.md`](review_load_monitoring.md)) |
| A5 | `is_beyond_gt_useful_count` | Findings where the LLM set `is_beyond_gt_useful = true` | int | findings_merge runner reads finding field | PP, PD | rolling 7-day ratio < 5% → info (tagging unreliable) | Phase 1 quality signal — beyond_gt is real engineering value (a1v2_fp_audit: 12 beyond_gt across 3 cases) |

## B. Duplicates and dedup

| # | Metric | Definition | Unit | Source | Agg | Alert threshold | Why we care |
|---|---|---|---|---|---|---|---|
| B1 | `dedup_total_in` | Findings entering dedup tail | int | new `class_dedup.py` writes `dedup_report` into `meta` (see [class_dedup.py](../dedup/class_dedup.py)) | PP, PD | none (informational) | Sizes the dedup workload |
| B2 | `dedup_same_class_drops` | Drops by `collapse_to_canonical` | int | class_dedup | PP, PD | > 30% of B1 → warn (over-collapse risk) | Phase 0 effectiveness signal |
| B3 | `dedup_fuzzy_drops` | Drops by fuzzy similarity ≥ threshold | int | fuzzy_dedup | PP, PD | > 30% of B1 → warn | Phase 0 effectiveness signal |
| B4 | `dedup_critical_protected` | КРИТИЧЕСКОЕ findings that bypassed dedup via critical-protect rule | int | class_dedup `_split_critical_protected` | PP, PD | drops to 0 when project has KRIT findings → page | Sanity check for safety contract ([phase0_phase1_validation_report.md §4 test_phase0_dedup_safety](../../algorithm_research/reports/phase0_phase1_validation_report.md)) |
| B5 | `dedup_total_out` | Findings after dedup | int | dedup report | PP | B5 == 0 while B1 > 0 → page (mass-drop bug) | Safety guard |
| B6 | `duplicate_rate` | `(B2 + B3) / B1` | float [0..1] | derived | PD, PW | rolling 7-day > 0.25 → info (lots of noise to collapse); > 0.4 → warn | Indicates upstream model is producing duplicates |
| B7 | `dedup_error_rate` | Dedup module raised exception (caught and counted) | int | findings_merge runner catches `DedupError` | PP, PD | > 0 in a day → warn | Dedup must never crash the audit |

## C. Completeness lens (Phase 1)

| # | Metric | Definition | Unit | Source | Agg | Alert threshold | Why we care |
|---|---|---|---|---|---|---|---|
| C1 | `completeness_lens_enabled` | Was lens eligible given document_type? | bool | new `completeness_runner.py` | PP, PD | none | Routing health |
| C2 | `completeness_lens_applied` | Did it actually run (i.e. eligible AND env-flag ON AND no precondition fail)? | bool | completeness_runner | PP, PD | per-day `applied / enabled` < 0.9 → warn | Detects silent skips |
| C3 | `completeness_findings_added` | Lens-output findings count BEFORE dedup | int | completeness_runner | PP, PD | rolling 7-day median > cap → page (cap not respected) | Verifies cap enforcement |
| C4 | `completeness_findings_after_dedup` | Lens-output findings that survived dedup | int | findings_merge runner | PP, PD | C4 / C3 < 0.3 → info (lens redundancy) | If dedup eats most of lens output, lens is wasted spend |
| C5 | `completeness_cap_hit` | Lens output == cap | bool | completeness_runner | PP, PD | rolling 7-day rate > 0.3 → info (cap may be too low) | Tuning signal for `STAGE01_COMPLETENESS_MAX_FINDINGS` |
| C6 | `completeness_lens_duration_ms` | Wall-clock for the lens leg | int (ms) | completeness_runner | PP, PD | rolling 7-day p95 > 240_000 → warn (Sonnet latency) | SLA |
| C7 | `completeness_lens_error_rate` | Lens raised an exception | float [0..1] | completeness_runner counts errors / runs | PD, PW | > 5% rolling 24h → warn; > 15% → page | Detects Sonnet outage |
| C8 | `completeness_lens_fallback_fired` | A0 fallback was returned because lens errored | bool | completeness_runner (see `test_fallback_to_a0.py`) | PP, PD | per-day count > 5 → warn | Confirms graceful fallback works |

## D. Document_type routing

| # | Metric | Definition | Unit | Source | Agg | Alert threshold | Why we care |
|---|---|---|---|---|---|---|---|
| D1 | `document_type_detected` | Detected type | enum (`full_rd`, `audit_comparison`, `tz_vs_rd`, `specification_only`) | new `document_type_detector.py` | PP, PD | per-day distribution drift > 20 p.p. vs trailing 30d → warn | Routing stability |
| D2 | `document_type_confidence` | Detector confidence | float [0..1] | document_type_detector | PP, PD | rolling 7-day p50 < 0.7 → warn | Falls below `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN`; routing degrades to full_rd default |
| D3 | `document_type_low_confidence_rate` | `confidence < STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN` | float | derived | PD | rolling 7-day > 0.2 → warn | If 20% of projects fall back to full_rd, Phase 1 silently turns off |
| D4 | `document_type_distribution` | A4 split by D1 | dict | derived | PD, PW | none (informational) | Sizes Phase 1 coverage |
| D5 | `document_type_override_count` | Engineer manually overrode detected type via project_info.json | int | project_info reader | PD | > 5/day → info | Detector accuracy signal |

## E. FP / noise estimates

| # | Metric | Definition | Unit | Source | Agg | Alert threshold | Why we care |
|---|---|---|---|---|---|---|---|
| E1 | `fp_estimate_speculative_keyword` | Findings whose `problem` starts with "Проверить" / "Уточнить" / "Возможно" / "Вероятно" | int | findings_merge runner heuristic | PP, PD | rolling 7-day > A0 baseline +50% → warn; > +100% → page | Speculative noise tracker; baseline from [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md): A1-v2 = 0 in audit |
| E2 | `fp_estimate_low_confidence_no_norm` | `confidence < 0.5 AND norm field empty` | int | findings_merge runner | PP, PD | > 5/project → warn; rolling > 3× A0 baseline → page | Soft-FP estimate ([`fp_monitoring.md`](fp_monitoring.md)) |
| E3 | `engineer_rejection_count_7d` | Findings rejected by engineer within 7 days of audit | int | needs new event hook (engineer marks "reject" in UI) | PP, PD | per-project > 30% rejected → warn; per-week rate > A0 +25% → warn; > +50% → page | Ground-truth-ish FP signal |
| E4 | `fp_rate_proxy` | `(E1 + E2) / A1` | float [0..1] | derived | PD, PW | rolling 7-day rate vs rolling 28-day baseline: +25% → warn; +50% → page | Composite FP estimate |

## F. Review load

| # | Metric | Definition | Unit | Source | Agg | Alert threshold | Why we care |
|---|---|---|---|---|---|---|---|
| F1 | `findings_per_project` | Same as A1, projected as review-load proxy | int | A1 | PP, PD, PW | > 30/project → warn | Engineer time |
| F2 | `findings_per_engineer_week` | Sum of A1 for projects reviewed by engineer in a week | int | needs new linkage to engineer in audit events | PW | > A0 baseline +30% → warn | Capacity planning |
| F3 | `review_duration_per_project_ms` | Time engineer spent in UI on the project page | int (ms) | extend existing `usage_data.json` per-session timing | PP, PD | rolling p50 > A0 +30% → warn | Detects review-load reality, not estimate |

## G. Wall-clock and cost

| # | Metric | Definition | Unit | Source | Agg | Alert threshold | Why we care |
|---|---|---|---|---|---|---|---|
| G1 | `wall_clock_stage01_ms` | Stage 01 total wall-clock | int (ms) | existing `pipeline_log.json` per-stage timing | PP, PD | rolling 7-day p95 > A0 +70% → warn; > +100% → page | Cost budget ([phase0_phase1_validation_report.md §5](../../algorithm_research/reports/phase0_phase1_validation_report.md): research +49%) |
| G2 | `wall_clock_current_method_ms` | Opus leg alone | int (ms) | text_analysis runner | PP, PD | rolling p95 > A0 +20% → warn | Detects Opus regression independent of lens |
| G3 | `wall_clock_completeness_ms` | Sonnet lens leg (already in C6) | int (ms) | completeness_runner | PP, PD | covered by C6 | — |
| G4 | `wall_clock_dedup_ms` | Dedup tail | int (ms) | findings_merge runner | PP, PD | > 5000 → info (dedup should be < 1s normally) | Dedup is pure Python, must be fast |
| G5 | `llm_cost_project_usd` | Sum of cost across both legs for one project | float (USD) | existing `paid_cost_events.jsonl` aggregated by `project_id` | PP, PD | rolling 7-day mean > A0 +70% → warn | Hard budget guard |
| G6 | `llm_cost_by_lens` | G5 split by `stage` (`text_analysis.current_method` vs `text_analysis.completeness`) | dict | extend `paid_cost_events.jsonl` `stage` field; already aggregated by `paid_cost_dashboard.build_paid_cost_daily_dashboard()` `by_stage` | PD, PW | completeness cost > 1.5× current_method cost → warn | Detects lens runaway |

## H. A/B shadow (only when shadow mode ON)

| # | Metric | Definition | Unit | Source | Agg | Alert threshold | Why we care |
|---|---|---|---|---|---|---|---|
| H1 | `shadow_a0_findings_count` | A0 leg's findings count when shadow ON | int | shadow runner output | AB | none (research signal) | A/B baseline |
| H2 | `shadow_a1v2_findings_count` | A1-v2 leg's findings count | int | shadow runner output | AB | none | A/B baseline |
| H3 | `shadow_engineer_chose_a1v2` | Engineer picked A1-v2 version over A0 | bool | new UI event when shadow shown side-by-side | AB | rolling 28-day acceptance < 50% → warn | Empirical win-rate (see [`../rollout/ab_testing_strategy.md`](../rollout/ab_testing_strategy.md)) |

---

## Notes on derivation

- **Severity distribution baseline** is computed automatically by
  `stage01_telemetry_dashboard.py` over a 28-day window of A0-only projects
  before any Phase 1 rollout. After rollout the baseline is frozen until
  the next quarterly review.
- **A0 baselines** for `findings_count_per_project`, cost, wall-clock all
  come from the existing 30-day window in `paid_cost.json daily_breakdown`
  (already populated by the existing pipeline). No new data backfill is
  needed before alarms can fire.
- **Speculative-keyword list** in E1 is intentionally Russian-only because
  Stage 01 prompts produce Russian findings. The list:
  `Проверить`, `Уточнить`, `Возможно`, `Вероятно`, `по-видимому`, `по всей видимости`.
- **Critical-protect contract** (B4): when a finding has `severity = КРИТИЧЕСКОЕ`,
  the dedup module routes it through `_split_critical_protected` (see
  [`class_dedup.py`](../dedup/class_dedup.py) line 209) and only collapses
  exact-same-class clusters. Fuzzy never removes a КРИТ finding without an
  exact same-class duplicate. This is verified by
  [`test_phase0_dedup_safety.py`](../../algorithm_research/tests/test_phase0_dedup_safety.py)
  (8 cases × 3 variants).

## References

- [`telemetry_plan.md`](telemetry_plan.md) — the four surfaces and the
  paid_cost_dashboard integration shape.
- [`fp_monitoring.md`](fp_monitoring.md) — E1, E2, E3, E4 in detail.
- [`critical_recall_monitoring.md`](critical_recall_monitoring.md) — A2
  and how we use it as a recall proxy.
- [`review_load_monitoring.md`](review_load_monitoring.md) — A4, F1, F2, F3.
- [`production_alerts.md`](production_alerts.md) — full alarm table that
  pulls thresholds from this file.
- [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) — the
  audit that puts `speculative_noise = 0` across 24 cases and shapes E1.
