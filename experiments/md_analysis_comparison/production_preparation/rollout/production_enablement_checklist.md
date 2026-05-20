# Production Enablement Checklist — Phase 0 Dedup

**Date:** 2026-05-20
**Audience:** on-call engineer + backend lead executing the production
rollout of Phase 0 dedup after staging soak.
**Pre-requisite:** staging activation green for ≥ 5 days (see
[`staging_activation_checklist.md`](staging_activation_checklist.md)).
**Source of safety guarantees:** [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md).
**Source of rollout shape:** [`phase0_rollout.md`](phase0_rollout.md) and
[`canary_strategy.md`](canary_strategy.md).

Phase 0 is a Python-only post-process in
`backend/app/pipeline/stages/findings_merge/runner.py::apply_phase0_dedup()`.
It is gated by `STAGE01_DEDUP_ENABLED` (default `false`) and is provably a
no-op on the current A0 prompt output (see
[`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §10.1).

---

## 1. Pre-enable checks

- [ ] Staging Phase 0 has been **ON for ≥ 5 calendar days** with no rollback
      events (per [`staging_activation_checklist.md`](staging_activation_checklist.md) §4 and §6).
- [ ] Staging `meta.dedup_report.critical_collapsed_count == 0` across at
      least **20 distinct audits** during the soak window. Pull via:
      ```bash
      find projects -name 03_findings.json -newer /tmp/.phase0_staging_start \
        -exec jq -r '.meta.dedup_report.critical_collapsed_count // "missing"' {} \;
      ```
      Expected: all `0`. Any `missing` means staging audit ran without the
      flag — investigate routing before enabling production.
- [ ] Staging has zero fail-open errors. Grep across staging pipeline logs:
      ```bash
      grep -r "Phase 0 dedup: ошибка" projects/ | wc -l
      ```
      Expected: `0`.
- [ ] PR merged to `main` branch and tagged. Confirm:
      ```bash
      git log --oneline --decorate main..HEAD | head -5
      git tag --contains <merge-commit-sha> | head -5
      ```
- [ ] CI green on `main` after merge:
      ```bash
      python -m pytest tests/findings/dedup/ -v
      ```
      Expected: `49 passed`.
- [ ] Approvers signed off (record names in the rollout ticket):
      - [ ] **Tech Lead** — confirms code + tests acceptable.
      - [ ] **Site Reliability** — confirms rollback procedure rehearsed.
      - [ ] **Research Lead** — confirms staging telemetry shows no surprises.

## 2. Enable steps

Phase 0 must be enabled via the deployment system, not a one-off SSH session,
so the env var survives restarts and is auditable.

- [ ] Update production env config (`/etc/audit-backend/production.env`, the
      Ansible/Terraform deployment manifest, or whichever the team uses) with:
      ```
      STAGE01_DEDUP_ENABLED=true
      STAGE01_DEDUP_FUZZY_THRESHOLD=0.7
      ```
- [ ] Open a change-management ticket referencing the merge SHA and this
      checklist.
- [ ] Schedule a rolling restart during the daily low-traffic window
      (audit queue idle, no batch jobs running).
- [ ] During the rolling restart, tail logs to confirm the new value loads:
      ```bash
      sudo journalctl -u audit-backend -f | grep -E "STAGE01_DEDUP|Phase 0"
      # in a second shell, on the same host:
      python -c "from backend.app.core import config; print(config.STAGE01_DEDUP_ENABLED, config.STAGE01_DEDUP_FUZZY_THRESHOLD)"
      ```
      Expected: `True 0.7`.
- [ ] Trigger one canary audit on a low-risk production project (see
      [`canary_strategy.md`](canary_strategy.md) §2 for selection criteria).
      Confirm the smoke checks in
      [`staging_activation_checklist.md`](staging_activation_checklist.md) §3.

## 3. 5% rollout checks (24-hour observation window)

> Note: per-project hash gating is not yet implemented in the current PR.
> Treat "5% rollout" operationally as **enabling Phase 0 for a hand-picked
> sample of 5 projects** via a temporary per-project env override or a
> feature-flag wrapper added in a follow-up PR. The metric thresholds below
> still apply to that sample.

- [ ] Confirm the 5 sample projects (see
      [`canary_strategy.md`](canary_strategy.md) §2) are queued / completed.
- [ ] 24 hours after activation, sweep `meta.dedup_report.critical_collapsed_count`
      across all completed audits in window:
      ```bash
      find projects -name 03_findings.json -newer /tmp/.phase0_prod_start \
        -exec jq -r '"\(.meta.dedup_report.critical_collapsed_count // "missing") \(input_filename)"' {} \;
      ```
      Expected: all values `0`.
- [ ] Confirm `dedup_duration_ms` p95 < 50 ms across the sample (pipeline
      timing entries; see telemetry plan in
      [`../telemetry/telemetry_plan.md`](../telemetry/telemetry_plan.md)).
- [ ] Grep production logs for any fail-open lines:
      ```bash
      sudo journalctl -u audit-backend --since "24 hours ago" | grep "Phase 0 dedup: ошибка"
      ```
      Expected: empty.
- [ ] Compare findings count delta per discipline vs. the prior-week baseline.
      Phase 0 is expected to be a no-op on A0; any systematic
      `before > after` on a discipline that was previously clean = investigate.

Gate to proceed to 25% rollout: all four checks above green.

## 4. 25% rollout checks (72-hour observation window)

- [ ] Widen the temporary per-project override to a sampled subset
      (~25% of production projects, ideally hash-sampled by `project_id`).
- [ ] Same four metric sweeps as §3, broader sample.
- [ ] Engineer-rejection rate: should be **unchanged** vs the 28-day baseline
      because Phase 0 only removes duplicate findings — it cannot introduce
      new ones (output count ≤ input count, see
      [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md) §1).
      Pull engineer-rejection data from the existing
      `discussions/decisions_log.json` and compare 7-day rolling rate.
- [ ] No Slack pings from `#pdf-audit-eng` about unexpected findings deltas.
- [ ] No support tickets referencing "missed critical" or "missing finding"
      on projects audited during the window.

Gate to proceed to 100% rollout: 72 h elapsed; all checks green.

## 5. 100% rollout checks (7-day observation window)

- [ ] Remove the per-project override and rely solely on
      `STAGE01_DEDUP_ENABLED=true` globally.
- [ ] All disciplines covered (EOM, ОВиК, КР, АР, ВК, СС, БУ — see
      `disciplines/_registry.json`).
- [ ] Repeat the §3 metric sweeps daily for 7 days.
- [ ] No AL-01 alarms (`critical_collapsed_count > 0`) during the entire
      window. AL-01 catalog entry:
      [`../telemetry/production_alerts.md`](../telemetry/production_alerts.md).
- [ ] Phase 0 declared **stable** after 7 days of clean operation; moved from
      the active rollout watch-list to the standard alarm rotation.

## 6. Rollback steps (in order of escalation)

| Level | Action | Command | Expected time | Downstream impact |
|---|---|---|---|---|
| L1 | Env-var flip → next pipeline run skips dedup | edit deployment env, set `STAGE01_DEDUP_ENABLED=false`; restart one worker to verify | < 5 min | next audit on each worker skips `apply_phase0_dedup`; existing `meta.dedup_report` on disk untouched but no new ones emitted |
| L2 | Env-var flip + restart **all** backend workers immediately | edit env + `sudo systemctl restart audit-backend.target` (or the deployment-system equivalent) | < 15 min | all in-flight `findings_merge` jobs on the restarted workers requeue at the resume stage; jobs already past `findings_merge` finish with their existing `meta.dedup_report` |
| L3 | Revert the merge commit and redeploy | `git revert <merge-sha>` → CI → deploy | < 60 min | new `meta.dedup_report` blocks no longer emitted; existing blocks on disk remain valid (purely additive, legacy readers already ignore unknown keys — see [`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §9) |

Rollback decision tree:

- One project shows `critical_collapsed_count > 0` → go straight to **L1**.
- > 1 project, or repeating after L1 → **L2**.
- L2 not effective after one full restart cycle, or the team decides Phase 0
  itself is faulty rather than a per-project bug → **L3**.

## 7. Alert response matrix

| Condition | Alert | Severity | First action | Follow-up |
|---|---|---|---|---|
| `meta.dedup_report.critical_collapsed_count > 0` on any production project | AL-01 | **page** | L1 rollback immediately (`STAGE01_DEDUP_ENABLED=false`) | investigate the project's input findings; КРИТ-protect counter > 0 means two КРИТ findings shared a class-key; file ticket with the offending `meta.dedup_report` and the diff between input/output findings |
| Fail-open log `Phase 0 dedup: ошибка` in any project | AL-03 | warn | within 2 hours: read the exception text in `pipeline_log.json`; verify `03_findings.json` is intact (`jq . projects/<x>/_output/03_findings.json | head`) | likely cause is malformed upstream finding (None field, non-string `problem_class`) — fix upstream input rather than dedup; see [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md) §3 |
| `dedup_duration_ms > 500` on any project | INFO | info | profile next maintenance window; likely a finding list > 200 (fuzzy_dedup is O(N²)) | see [`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §10.2 — known limitation |
| `same_class_drops` skyrockets on a previously-quiet project | INFO | info | spot-check the project's `03_findings.json`; verify dropped duplicates were genuine | likely upstream prompt drift producing many duplicates — escalate to Tier 3 (research lead) if pattern persists |

Full alarm catalog and severity definitions: [`../telemetry/production_alerts.md`](../telemetry/production_alerts.md).

## 8. Escalation matrix

| Tier | Role | Owns | Page criteria |
|---|---|---|---|
| Tier 1 | on-call engineer | env-var flips, L1/L2 rollback, sweeping `meta.dedup_report` across projects | AL-01 (critical_collapsed_count > 0) |
| Tier 2 | backend lead | L3 rollback (`git revert`), deeper investigation of fail-open exceptions, decision on follow-up PR | AL-01 unresolved after Tier 1 L1+L2; AL-03 recurring on ≥ 3 projects in a day |
| Tier 3 | research lead | decision on prompt changes, fuzzy threshold tuning, КРИТ-protect logic changes | any decision involving `STAGE01_DEDUP_FUZZY_THRESHOLD` change; sustained `same_class_drops` surge across multiple projects |

Slack / channel handles (placeholder — fill on first activation):

- Tier 1: `<#oncall-pdf-audit>`
- Tier 2: `<#pdf-audit-eng>`
- Tier 3: `<#pdf-audit-research>`

Page criteria (immediate paging via the on-call rotation):

- **AL-01** — `critical_collapsed_count > 0` on any production project. This
  is the only AL that pages for Phase 0; everything else is `warn` or `info`
  per [`../telemetry/production_alerts.md`](../telemetry/production_alerts.md).
