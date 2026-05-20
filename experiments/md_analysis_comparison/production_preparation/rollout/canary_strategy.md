# Canary Strategy — Phase 0 Dedup

**Date:** 2026-05-20
**Scope:** Stage-gated ramp from staging to 100% production for the Phase 0
post-merge dedup feature.

The canary plan complements
[`staging_activation_checklist.md`](staging_activation_checklist.md) (Stage 1
how-to) and
[`production_enablement_checklist.md`](production_enablement_checklist.md)
(Stages 2–4 how-to). The 28-day shape and the no-op safety claim come from
[`phase0_rollout.md`](phase0_rollout.md) and
[`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md).

---

## 1. Stages and gates

| Stage | Scope | Duration | Exit criteria | Rollback trigger |
|---|---|---|---|---|
| 1 | 5 staging projects (manual selection, see §2) | 5 calendar days minimum, longer if cohorts are missing | all 5 audits have `meta.dedup_report.critical_collapsed_count == 0`; no fail-open logs; `before == after` on the A0-baseline cohort; intentional duplicates correctly collapsed on the `VK_with_dupes`-style case | any AL-01; any `Phase 0 dedup: ошибка` line; any project where `before > after` and a manual review finds the dropped finding was not a true duplicate |
| 2 | 5% production (hash-sampled by `project_id`; in the current PR, operationally enabled for a hand-picked 5-project sample via per-project override) | 24 h soak | same metrics green at the wider sample; engineer rejection rate unchanged | same as Stage 1 + engineer rejection rate jumps > 25% vs prior week (cross-reference with AL-15) |
| 3 | 25% production (hash-sampled) | 72 h soak | metric green across all disciplines represented; pipeline timing for `findings_merge` not regressed > 10% | same as Stage 2 + cross-discipline coverage gaps (one discipline shows anomalous `same_class_drops` p99 > 10 per project) |
| 4 | 100% production | 7-day soak before "stable" declaration | no AL-01, no AL-03 in window; no support tickets referencing missing findings | any AL-01; any pattern of AL-03; ≥ 2 fail-open events per 100 projects in 24 h |

Each stage's exit gates are concrete telemetry conditions, not subjective
judgement. The gates are listed again, with stricter thresholds, in §4–5.

## 2. Project selection criteria for Stage 1 (5 staging projects)

Pick 5 staging projects (synthetic smoke sandboxes are acceptable; see memory
note `project_smoke_sandbox.md` and
[`staging_activation_checklist.md`](staging_activation_checklist.md) §8) that
jointly satisfy:

- [ ] At least one project per discipline across the rollout cohort:
      **EOM**, **OV** (ОВиК), **ВК**, **AR**, **MULTI**. One project per
      discipline is fine; pairs are not required for Stage 1.
- [ ] At least one project with **> 20 findings** in its most recent
      `03_findings.json`. This stress-tests the O(N²) fuzzy_dedup loop (see
      [`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §10.2)
      and exercises a realistic same_class_drops distribution.
- [ ] At least one project with **≥ 3 КРИТИЧЕСКОЕ findings sharing a class
      key** (so that the КРИТ-protect guard actually fires, mirroring the
      `KJ_KM_critical_heavy` representative-sample case where
      `critical_collapsed_count=2` and all 3 КРИТ were preserved).
- [ ] At least one project with **intentional or naturally occurring
      duplicates** so the dedup actually collapses something — mirroring
      `VK_with_dupes` (7→5 in the representative-sample run).
- [ ] **Avoid projects in active engineer review.** Phase 0 only changes
      newly-emitted `03_findings.json`, but a project under review while
      Phase 0 runs creates noise in the engineer-rejection-rate signal that
      Stage 2 depends on.
- [ ] Recommend **rerunning the audit** on each selected project to get a
      fresh post-Phase-0 `03_findings.json` rather than relying on stale
      pre-flag artifacts.

Suggested Stage 1 cohort (placeholder names — substitute equivalents that
exist in staging):

1. `projects/EOM/_smoke_phase0_2026_05_20` — discipline EOM, A0 no-op baseline.
2. `projects/OV/_smoke_phase0_2026_05_20` — discipline OV, A0 no-op baseline.
3. `projects/ВК/_smoke_phase0_dupes_2026_05_20` — duplicates present, expects
   `before > after`.
4. `projects/AR/_smoke_phase0_large_2026_05_20` — large finding count
   (> 20), stress-tests duration.
5. `projects/MULTI/_smoke_phase0_critical_heavy_2026_05_20` — multiple КРИТ
   in one cluster, stress-tests КРИТ-protect.

## 3. Manual findings review checklist (for staging Stage 1)

For each of the 5 projects, the engineer must open `_output/03_findings.json`
and confirm:

- [ ] All findings from the **previous (pre-Phase-0) run** are still present
      *or* explicitly accounted for in `class_dedup.same_class_drops` /
      `fuzzy_dedup.same_class_drops`. Use a diff:
      ```bash
      jq -S '.findings[].id' <pre>.json | sort > /tmp/pre.ids
      jq -S '.findings[].id' <post>.json | sort > /tmp/post.ids
      diff /tmp/pre.ids /tmp/post.ids
      ```
- [ ] `meta.dedup_report.critical_collapsed_count == 0`. Hard gate.
- [ ] If `before > after`: open each `same_class_drops_by_key` entry and
      review the collapsed duplicates. Engineer judgement is required: did
      anything important disappear, or were these genuine duplicates that the
      LLM emitted multiple times?
- [ ] `meta.dedup_report` shape matches the expected schema in
      [`staging_activation_checklist.md`](staging_activation_checklist.md) §5.1
      (all keys present, types match, `fuzzy_threshold == 0.7`).
- [ ] No findings carry contradictory `is_canonical` flags (every cluster has
      exactly one canonical).

If any of the five fail, halt Stage 1, file a ticket per
[`production_enablement_checklist.md`](production_enablement_checklist.md) §6
guidance, and do not proceed to Stage 2.

## 4. Telemetry that gates each stage

Each gate is the AND of all listed conditions.

### Stage 1 → 2 gate

- 5 staging projects audited under Phase 0 in the last 5 days.
- All 5 have `critical_collapsed_count == 0`.
- Zero `Phase 0 dedup: ошибка` lines in staging logs.
- Engineer-rejection rate on the 5 projects matches pre-Phase-0 baseline
  ± 10%.

### Stage 2 → 3 gate

- 24 h soak completed at 5% scope.
- All Stage 1 conditions still hold for the broader sample.
- `dedup_duration_ms` p95 < 50 ms across the sample.
- Engineer rejection rate unchanged ± 10% vs prior week (Phase 0 only removes
  duplicates and cannot inflate the rejection rate by design).

### Stage 3 → 4 gate

- 72 h soak completed at 25% scope.
- Cross-discipline coverage: at least one audit per discipline (EOM, OV,
  ВК, AR, MULTI) audited under Phase 0 in the window.
- No AL-01 alarms anywhere in production.
- `findings_merge` stage wall-clock not regressed by > 10% vs the 28-day
  baseline (Phase 0 should add < 50 ms per project; if the stage runs slower,
  something else is off).

## 5. Critical telemetry indicators (must be green at each gate)

| Indicator | Source | Green threshold | Notes |
|---|---|---|---|
| `critical_collapsed_count == 0` across all projects in the window | `_output/03_findings.json` `meta.dedup_report` | every value `0` | Any non-zero = AL-01; halt rollout, page on-call |
| `error in meta.dedup_report` count | grep `Phase 0 dedup: ошибка` in pipeline log | exactly `0` | One isolated event per 100 projects in 24 h is yellow but not blocking; > 1% is red |
| `same_class_drops` distribution | `meta.dedup_report.class_dedup.same_class_drops_by_key` | p99 < 10 per project | Higher p99 means upstream is emitting many duplicates per key — investigate prompt, not dedup |
| `findings_merge` stage timing | `pipeline_log.json` per-stage timings | regression < 10% vs 28-day baseline | Phase 0 should add < 50 ms per project |
| `paid_cost.json` delta attributable to dedup | `paid_cost.json` per-day per-stage breakdown | exactly `0` (Phase 0 has no LLM surface) | Any positive value = bug |

## 6. What to do if a gate fails

The rollout has three responses, depending on which gate failed:

1. **Rollback the current stage immediately.** Use the L1 procedure from
   [`production_enablement_checklist.md`](production_enablement_checklist.md) §6:
   `STAGE01_DEDUP_ENABLED=false` in the deployment env, restart the backend
   workers. Time-to-rollback is under 1 minute and Phase 0 simply stops
   running on the next pipeline invocation.
2. **File a ticket** in the engineering tracker with:
   - `project_id` of the offending audit (or list of audits);
   - the full `meta.dedup_report` JSON from the offending project;
   - a diff between pre-Phase-0 and post-Phase-0 findings (use the `jq` diff
     pattern in §3);
   - log excerpts from `pipeline_log.json` for the `findings_merge` step,
     specifically the `Phase 0 dedup: ...` line and any preceding warnings.
3. **Halt further rollout** until the ticket is resolved. The next attempt
   restarts from the failed stage, not from Stage 1 — the prior stages do not
   need to be re-soaked unless the resolution itself introduced new code.

If the fail is at Stage 1 (staging), the resolution may be as light as
adjusting `STAGE01_DEDUP_FUZZY_THRESHOLD` (e.g. raise to 0.75 or 0.8 — both
strictly safer per [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md) §2)
and re-running Stage 1.

If the fail is at Stage 4 (100%, post-soak), the most likely cause is a data
quality regression upstream (e.g. Stage 01 prompt emitting many duplicates),
not a dedup bug — escalate to Tier 3 per
[`production_enablement_checklist.md`](production_enablement_checklist.md) §8.
