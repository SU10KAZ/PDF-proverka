# Staging Activation Checklist — Phase 0 Dedup

**Date:** 2026-05-20
**Audience:** on-call / staging operator flipping `STAGE01_DEDUP_ENABLED=true`
on staging for the first time.
**Time-to-run:** ~30 минут включая smoke audit.
**Time-to-rollback:** < 1 минуты (env-flip + restart).

This is the copy-paste-runnable checklist for the **first activation** of
Phase 0 dedup on staging. Phase 0 is gated by env vars and OFF by default in
production code (see
[`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md)).
Safety reasoning: [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md).
Rollout plan: [`phase0_rollout.md`](phase0_rollout.md).

---

## 1. Pre-flight checks (do BEFORE flipping anything)

- [ ] Confirm the staging branch is at the expected SHA and the merge commit
      includes the Phase 0 changes:
      ```bash
      git log --oneline -n 3
      git show --stat HEAD | head -40
      ```
      Expected: a commit titled something like `feat(findings): Phase 0 dedup
      post-merge` touching `backend/app/core/config.py`,
      `backend/app/pipeline/stages/findings_merge/runner.py` and
      `backend/app/services/findings/dedup/`.
- [ ] Confirm both env vars are at default values on the staging host:
      ```bash
      printenv STAGE01_DEDUP_ENABLED   # expected: empty or "false"
      printenv STAGE01_DEDUP_FUZZY_THRESHOLD   # expected: empty or "0.7"
      ```
- [ ] Run the new Phase 0 test suite on staging CI — must be green:
      ```bash
      python -m pytest tests/findings/dedup/ -v
      ```
      Expected: `49 passed`.
- [ ] Import smoke — package loads cleanly with the flag still OFF:
      ```bash
      python -c "from backend.app.services.findings.dedup import collapse_to_canonical, fuzzy_dedup; print('OK')"
      ```
      Expected: `OK`.
- [ ] Confirm no live audit jobs are running (Phase 0 only takes effect on
      the next `findings_merge` invocation, but we want a clean baseline for
      smoke testing):
      ```bash
      curl -s http://localhost:8081/api/audit/queue | jq '.running, .pending'
      ```
      Expected: both `[]` or zero counts.
- [ ] Snapshot the staging `paid_cost.json` for the day — Phase 0 must not
      change cost (no LLM calls):
      ```bash
      cp backend/app/data/paid_cost.json /tmp/paid_cost.before_phase0.json
      ```

## 2. Enable sequence

- [ ] Set the env var in the staging environment file (do NOT export inline if
      the backend is managed by systemd — the variable will not survive a
      restart). Staging convention on this host is systemd:
      ```bash
      sudo systemctl edit audit-backend
      # add: Environment="STAGE01_DEDUP_ENABLED=true"
      ```
      If staging is using pm2 or a bare `uvicorn` invocation, edit the launch
      script or the project `.env` file instead.
- [ ] Restart the backend process:
      ```bash
      sudo systemctl restart audit-backend
      # or, for the dev convention:
      #   uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload
      ```
- [ ] Confirm the new value is loaded inside the running process. There is
      currently no `/api/health/config` route; the most reliable check is to
      import config in a one-shot subprocess that shares the env:
      ```bash
      python -c "from backend.app.core import config; print('STAGE01_DEDUP_ENABLED =', config.STAGE01_DEDUP_ENABLED); print('STAGE01_DEDUP_FUZZY_THRESHOLD =', config.STAGE01_DEDUP_FUZZY_THRESHOLD)"
      ```
      Expected:
      ```
      STAGE01_DEDUP_ENABLED = True
      STAGE01_DEDUP_FUZZY_THRESHOLD = 0.7
      ```

## 3. Smoke tests on a fresh dry-run project

Use a synthetic sandbox project under `projects/<discipline>/_smoke_*/` —
those folders are hidden from `iter_project_dirs` but can be addressed by full
path (see memory note `project_smoke_sandbox.md`).

- [ ] Pick or create a smoke project, e.g.
      `projects/EOM/_smoke_phase0_2026_05_20/`.
- [ ] Trigger the standard pipeline:
      ```bash
      python process_project.py projects/EOM/_smoke_phase0_2026_05_20
      python blocks.py crop projects/EOM/_smoke_phase0_2026_05_20
      # then run the audit through the UI or
      #   POST /api/audit/start  ... (staging convention)
      ```
- [ ] Confirm `findings_merge` log contains a Phase 0 line. Tail the live log
      or `pipeline_log.json`:
      ```bash
      grep -E "Phase 0 dedup" projects/EOM/_smoke_phase0_2026_05_20/_output/pipeline_log.json
      ```
      Expected: one of the four canonical log strings (see §5).
- [ ] Confirm `_output/03_findings.json` has the new `meta.dedup_report`
      block and the КРИТ-protect counter is zero:
      ```bash
      jq '.meta.dedup_report' projects/EOM/_smoke_phase0_2026_05_20/_output/03_findings.json
      jq '.meta.dedup_report.critical_collapsed_count' projects/EOM/_smoke_phase0_2026_05_20/_output/03_findings.json
      ```
      Expected: full dedup_report object; `critical_collapsed_count == 0`.
- [ ] Confirm `before == after` on this A0 baseline run (Phase 0 is provably
      a no-op on the current A0 prompt output — see
      [`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §10.1):
      ```bash
      jq '.meta.dedup_report | {before, after, critical_collapsed_count}' \
         projects/EOM/_smoke_phase0_2026_05_20/_output/03_findings.json
      ```
- [ ] Confirm staging `paid_cost.json` did NOT grow because Phase 0 makes no
      LLM calls:
      ```bash
      diff /tmp/paid_cost.before_phase0.json backend/app/data/paid_cost.json | head -40
      ```
      Expected: no new entries attributable to the smoke audit beyond what the
      pre-existing `findings_merge` would have produced.

## 4. Metrics to watch (first 30 min after enable)

- [ ] `meta.dedup_report.critical_collapsed_count` — MUST be `0` on every
      project. Non-zero on any single project = hard alarm AL-01.
- [ ] `meta.dedup_report.before` / `after` — on A0 they should typically be
      equal; a `before > after` is allowed (real dedup activity) but should
      be inspected to confirm the dropped findings were genuine duplicates.
- [ ] `dedup_duration_ms` (implicit via the pipeline timing entry) — expected
      < 5 ms per project for the typical 5–30 findings cohort, < 50 ms ceiling.
- [ ] Pipeline error logs (`pipeline_log.json`, WS live log, stderr of the
      backend) — there must be no new `ERROR`/`WARNING` lines emitted from
      `findings_merge/runner.py` other than the structured Phase 0 log lines
      listed in §5.
- [ ] `paid_cost.json` daily total — must be unchanged vs the pre-flip
      snapshot for any project audited after the flip (Phase 0 has no LLM
      surface).

## 5. Expected logs / `meta.dedup_report` example

### 5.1 Sample `meta.dedup_report` (no-op case, A0 baseline)

```json
{
  "meta": {
    "dedup_report": {
      "class_dedup": {
        "total_in": 14,
        "total_out": 14,
        "clusters": 14,
        "same_class_drops": 0,
        "same_class_drops_by_key": {},
        "critical_collapsed_count": 0,
        "methods_seen": []
      },
      "fuzzy_dedup": {
        "total_in": 14,
        "total_out": 14,
        "clusters": 14,
        "same_class_drops": 0,
        "same_class_drops_by_key": {},
        "critical_collapsed_count": 0,
        "sim_threshold": 0.7,
        "methods_seen": []
      },
      "before": 14,
      "after": 14,
      "critical_collapsed_count": 0,
      "fuzzy_threshold": 0.7
    }
  }
}
```

### 5.2 Sample log lines (operator-facing, Russian — match production tone)

- Success / dedup actually fired:
  ```
  Phase 0 dedup: 7 → 5 замечаний (class+fuzzy, threshold=0.7)
  ```
- No-op (expected on A0 baseline):
  ```
  Phase 0 dedup: no-op (0 duplicates)
  ```
- ALARM (КРИТ-protect counter > 0 — must not happen in production):
  ```
  Phase 0 dedup: ALARM critical_collapsed_count=2 (must be 0 in production)
  ```
- Error / fail-open (findings are returned untouched on disk):
  ```
  Phase 0 dedup: ошибка (findings оставлены без изменений) — <exception text>
  ```

## 6. Acceptable ranges

| Metric | Green | Yellow (investigate) | Red (alarm) |
|---|---|---|---|
| `before == after` | A0 baseline default | `before > after` with small delta (e.g. 7→5, similar to the VK_with_dupes synthetic) — log only on staging | unexpected mass collapse > 30% of `before` |
| `critical_collapsed_count` | `0` | `1–2` on a single project (investigate but no page on staging) | `> 2`, or any non-zero in production — hard alarm AL-01 |
| `dedup_duration_ms` | < 50 ms per project | 50–500 ms | > 500 ms (likely a > 200-finding project; profile) |
| dedup error rate | 0 errors per 100 projects | 1 error per 100 projects | > 1% — escalate |
| `paid_cost.json` delta attributable to dedup | exactly 0 | n/a | any positive value = bug (Phase 0 has no LLM surface) |

## 7. Rollback command (single-step)

```bash
# 1) Flip the flag back off
sudo systemctl edit audit-backend   # set STAGE01_DEDUP_ENABLED=false
# or remove the line entirely

# 2) Restart
sudo systemctl restart audit-backend
```

- Time-to-rollback: **< 1 минуты**.
- Subsequent `findings_merge` runs will skip `apply_phase0_dedup` entirely —
  the runner short-circuits when the flag is false (see
  [`../reports/phase0_implementation_report.md`](../reports/phase0_implementation_report.md) §9).
- No data migration required. Existing `03_findings.json` files with
  `meta.dedup_report` blocks remain valid (the block is purely additive and
  legacy readers already ignore unknown keys).
- Hard rollback (if env-flip is somehow not enough): `git revert` the merge
  commit and redeploy — see
  [`production_enablement_checklist.md`](production_enablement_checklist.md) §6.

## 8. Sample projects to run smoke on

Pick 3–5 projects covering different disciplines. If these specific codes are
not present in staging, choose any project of the appropriate discipline.

- [ ] `projects/EOM/_smoke_phase0_2026_05_20` — synthetic smoke (EOM discipline).
- [ ] `projects/OV/_smoke_phase0_2026_05_20` — synthetic smoke (ОВиК).
- [ ] `projects/ВК/_smoke_phase0_dupes_2026_05_20` — synthetic, ideally with
      a couple of intentional duplicates so that `before > after` actually
      fires once (stress-test for dedup activity, mirrors the
      `VK_with_dupes` representative-sample case where 7→5).
- [ ] `projects/AR/_smoke_phase0_large_2026_05_20` — synthetic, stress-test
      with > 20 findings (mirrors the `AR_large` synthetic case).
- [ ] `projects/MULTI/_smoke_phase0_critical_heavy_2026_05_20` — synthetic
      with ≥ 3 КРИТ findings sharing a class key, so the КРИТ-protect guard
      gets exercised. Expected: all 3 КРИТ preserved, counter increments
      (mirrors the `KJ_KM_critical_heavy` synthetic case where
      `critical_collapsed_count=2` and all 3 КРИТ remained on disk).

After all 5 projects pass §3 and §4, staging activation is considered green
and the rollout can proceed to the 5% canary stage described in
[`canary_strategy.md`](canary_strategy.md) and
[`production_enablement_checklist.md`](production_enablement_checklist.md).
