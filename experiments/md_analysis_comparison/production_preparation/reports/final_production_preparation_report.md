# Final Production Preparation Report — Stage 01 MD-Analysis Upgrade

**Date:** 2026-05-20
**Package:** [`experiments/md_analysis_comparison/production_preparation/`](../)
**Production status:** **NOTHING CHANGED IN PRODUCTION.** Every file in this
package lives under `production_preparation/`; the production pipeline, prompts,
schemas, and pipeline files are untouched.

> This is the synthesis report for the production-preparation package. For the
> one-page go/no-go decision, jump to
> [`final_verdict.md`](final_verdict.md).

---

## 1. What was prepared

A complete production-preparation package containing prompts, dedup modules,
schemas, checklists, rollout/telemetry/integration plans, tests, migration,
examples and final reports.

**63 files, ~12 800 LOC** (~10 800 markdown + ~2 000 Python):

| Chapter | Files | Purpose |
|---|---|---|
| `prompts/` | 5 | Drop-in Stage 01 prompt, completeness lens prompt, few-shot examples, severity calibration, document_type block |
| `schemas/` | 5 | document_type design + detection rules (runnable Python) + finding schema v2 + meta block |
| `dedup/` | 5 | class_dedup.py + fuzzy_dedup.py (production-ready, pure stdlib) + problem_class vocabulary + thresholds + safety reasoning |
| `checklists/` | 10 | 8 discipline checklists (AR/KJ/KM/EOM/OV/VK/SS/MULTI) + shared rules + applicability matrix |
| `telemetry/` | 6 | Telemetry plan, metrics definition, FP/critical-recall/review-load monitoring, alarms |
| `rollout/` | 6 | Phase 0/1 rollout, rollback, A/B, guardrails, routing rules |
| `integration_plan/` | 6 | Per-file changes, LOC estimates, risk inventory, rollback steps |
| `tests/` | 5 | Test plan, regression / golden / stochasticity / shadow-mode |
| `migration/` | 2 | Schema v1→v2 migration, backfill policy (no backfill) |
| `examples/` | 9 | Concrete worked examples (good / bad / speculative / dup / severity / doc_type) |
| `reports/` | 2 | This report + final verdict |

Top-level [`README.md`](../README.md) is the entry point; it lists everything.

## 2. What is ready to put into production

### Phase 0 — class + fuzzy dedup as post-process

**READY.** Production-ready, validated safe, deployable with feature flag.

Evidence:

- [`../algorithm_research/reports/phase0_phase1_validation_report.md`](../../algorithm_research/reports/phase0_phase1_validation_report.md#13-phase-0-dedup-safety) — Phase 0 on A0 baseline:
  matched_gt 49→49, missed_crit 3→3, FP 73→73 (**no-op, no harm**).
- On legacy multi-source merged outputs: fuzzy_dedup removes 18% FP and adds
  +20 strict_score (validation across 8 cases).
- `production_preparation/dedup/{class_dedup.py, fuzzy_dedup.py}` are
  production-ready: pure stdlib, КРИТИЧЕСКОЕ-protect guard, fail-open posture,
  hard-asserted count invariant. Both modules compile and self-test on Python
  3.11+.

Integration ([`integration_plan/phase0_integration.md`](../integration_plan/phase0_integration.md)):

- 5 new files (~1115 LOC) + 3 modified production files (~53 LOC).
- All gated behind `STAGE01_DEDUP_ENABLED=false` default. Flip-to-true is an
  env-var change.
- Total production touch: `findings_service.py` (+30), `findings_merge/runner.py`
  (+15), `core/config.py` (+8). Net production review surface ~37 LOC of
  effective signal.

### Phase 1 — completeness lens, opt-in by `document_type`

**READY for opt-in deploy on `audit_comparison` and `specification_only`.**
Not ready as a blanket replacement.

Evidence:

- [`../algorithm_research/reports/FINAL_SUMMARY.md`](../../algorithm_research/reports/FINAL_SUMMARY.md) §4 — per-doc-type strict_score:
  - `audit_comparison`: 25.1 → **51.4** (A1-v2 wins, +26).
  - `specification_only`: A1-v2 = 22.0 (no A0 baseline; 3/3 critical caught).
  - `full_rd`: 49.8 → 23.1 (A1-v2 worse, +114 FP from completeness lens; **HOLD**).
  - `tz_vs_rd`: 80.0 → 36.0 on 1 case (HOLD until more data).
- Per-case missed-critical rate **halved** (8 A0 missed_crit cases ÷ 8 → 3 A1-v2 missed_crit cases ÷ 16).
- Speculative-noise rate is **0** across audited cases — extra A1-v2 findings
  are duplicates of GT, wrong-severity, or `beyond_gt_useful`, not random
  hallucinations ([`a1v2_fp_audit.md`](../../algorithm_research/reports/a1v2_fp_audit.md)).
- Sonnet failure → graceful fallback to A0 verified.

Integration ([`phase1_integration.md`](../integration_plan/phase1_integration.md)):

- 13 new files (~2150 LOC: completeness_lens.py + document_type_detector.py +
  new prompt + 8 discipline checklists + test scaffolding) + 6 modified
  production files (~250 LOC).
- All gated behind `STAGE01_COMPLETENESS_LENS_ENABLED=false` default; per-doc-type
  routing controlled by `STAGE01_COMPLETENESS_BY_DOC_TYPE`.
- Recommended initial routing: `audit_comparison` only (1 doc_type); expand
  per-discipline after canary.

## 3. What is **NOT** production-ready

| Item | Status | Required before unlock |
|---|---|---|
| Phase 1 on `full_rd` | **HOLD** | (a) completeness cap tuned from 14 → 6 for full_rd (cap is in the lens prompt — already trimmed in `production_preparation/prompts/completeness_lens_production_prompt.md`); (b) Phase 1 `is_beyond_gt_useful` tagging reliability validated on ≥ 6 full_rd cases; (c) stochasticity (3-run) IQR/median ≤ 0.25. None of these are done yet. |
| Phase 1 on `tz_vs_rd` | **OPT-IN ONLY** | Only 1 `tz_vs_rd` case tested. Each new project must be flagged manually until ≥ 5 cases give stable data. |
| Cross-discipline lens / router | **WAIT** | Not justified by current data; defer to Phase 2. |
| Reviewer agent | **WAIT** | Not needed — A1-v2 reaches recall without it. |
| Full multi-agent (6 lenses) | **REJECTED** | ×5.2 cost, +2 recall, +145 FP. Not coming back. |

## 4. What can be deployed immediately vs opt-in vs blocked

| Deployment lane | Now | Opt-in | Blocked |
|---|---|---|---|
| Phase 0 dedup (`STAGE01_DEDUP_ENABLED=true`) | ✅ | — | — |
| Phase 1 on `audit_comparison` | — | ✅ | — |
| Phase 1 on `specification_only` | — | ✅ | — |
| Phase 1 on `tz_vs_rd` | — | ⚠ per-project | — |
| Phase 1 on `full_rd` | — | — | ❌ |
| Production Stage 01 prompt swap | — | ⚠ requires shadow mode first | — |
| Document_type detection in production | ✅ once Phase 1 lands | — | — |

Note: the new Stage 01 prompt
([`prompts/stage01_production_prompt.md`](../prompts/stage01_production_prompt.md))
is back-compatible with the current `01_text_analysis.json` consumers. New
fields are additive only.

## 5. Open risks (after this package)

| Risk | Severity | Mitigation in package | Residual |
|---|---|---|---|
| Sonnet lens timeout / failure | MEDIUM | Fallback to A0 enforced; alarm AL-13 ([`production_alerts.md`](../telemetry/production_alerts.md)); `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE=true` default | LOW — fall-open posture is built in |
| document_type misdetection drops Phase 1 silently | MEDIUM | Confidence threshold + audit-log trail in [`routing_rules.md`](../rollout/routing_rules.md); fallback to `full_rd` is the conservative path (no lens runs) | LOW |
| FP increase invisible until aggregated weekly | MEDIUM | Rolling 24h auto-shutoff (Phase 1 only) via AL-09/10 | LOW once telemetry deployed |
| Dedup collapsing КРИТИЧЕСКОЕ accidentally | LOW | Mathematical proof in [`dedup_safety.md`](../dedup/dedup_safety.md) + `critical_collapsed_count` counter; hard assert | NEGLIGIBLE |
| Stochasticity unknown for full_rd | MEDIUM | 3-run plan in [`stochasticity_strategy.md`](../tests/stochasticity_strategy.md) — must run BEFORE unblocking full_rd | applies only to future full_rd unlock |
| Prompt regression on disciplines we haven't tested | MEDIUM | Regression suite ([`regression_strategy.md`](../tests/regression_strategy.md)) gates merge; ≥ 3 cases per discipline required | LOW after suite green |
| Checklist-driven phantom findings on audit_comparison | LOW (already hit, already fixed) | `document_type` STRICT BAN block in completeness prompt | NEGLIGIBLE |
| Synthetic ground truth narrower than real audits | MEDIUM | Inter-rater review TBD; not gated yet | UNCHANGED — research backlog item |
| Cost growth | LOW | Research shows +50-70% LLM cost; under the +70% gate; alarm AL-19 caps it | LOW |
| Schema v1 readers breaking on v2 outputs | LOW | Strict additive-only schema bump; v1 readers must tolerate unknown fields (validated by [`migration_plan.md`](../migration/migration_plan.md)) | LOW |

## 6. Mandatory guardrails before Phase 0 ships

From [`production_guardrails.md`](../rollout/production_guardrails.md):

1. `STAGE01_DEDUP_ENABLED` env var with default `false`.
2. `STAGE01_DEDUP_FUZZY_THRESHOLD` env var with default `0.7`.
3. `critical_collapsed_count` always 0 in dedup_report — assert in CI test;
   alarm AL-01 if > 0 in production.
4. Dedup is fail-open — any exception → log + skip dedup, return original
   findings.

## 7. Mandatory guardrails before Phase 1 ships

Stacked on Phase 0:

5. `STAGE01_COMPLETENESS_LENS_ENABLED` env var with default `false`.
6. `STAGE01_COMPLETENESS_BY_DOC_TYPE` map; initial value `"audit_comparison"` only.
7. `STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD=6` / `..._OTHER=10`.
8. `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE=true` (default).
9. `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN=0.7`.
10. Engineer-rejection feedback wired (manual rollout AL-14).
11. Per-discipline kill-switch wired (`STAGE01_DISCIPLINE_DISABLE_LIST`).
12. Rolling-24h FP auto-shutoff alarm (AL-09 → AL-10).

## 8. Mandatory telemetry before Phase 0 / Phase 1 ships

From [`telemetry_plan.md`](../telemetry/telemetry_plan.md):

- Findings count (total + per severity), per-project.
- Severity distribution.
- `dedup_report.same_class_drops` + `same_class_drops_by_key` (Phase 0).
- `critical_collapsed_count` (Phase 0 — must be 0).
- `document_type` distribution + confidence histogram (Phase 1).
- Completeness lens applied/not (Phase 1).
- Lens duration + lens failure rate (Phase 1).
- LLM cost per stage split (current_method vs completeness) — extends
  existing `paid_cost_dashboard.py`.
- FP-estimate (heuristic: speculative-keyword + low-confidence-no-norm +
  engineer-rejection-7d).
- Critical recall proxies (per-discipline KRIT-rate, per-doc-type KRIT-rate).

## 9. Production files potentially modified

From [`files_to_modify.md`](../integration_plan/files_to_modify.md):

**Phase 0 (8 files touched, 1115 LOC new + 53 LOC modified ≈ 1168 LOC):**

- `backend/app/services/findings/dedup/__init__.py` — NEW.
- `backend/app/services/findings/dedup/class_dedup.py` — NEW (~430 LOC).
- `backend/app/services/findings/dedup/fuzzy_dedup.py` — NEW (~280 LOC).
- `backend/app/services/findings/dedup/_normalise.py` — NEW (~50 LOC, optional shared util).
- `backend/app/services/findings/dedup/README.md` — NEW.
- `backend/app/services/findings/findings_service.py` — MODIFY (+30 LOC: call dedup).
- `backend/app/pipeline/stages/findings_merge/runner.py` — MODIFY (+15 LOC).
- `backend/app/core/config.py` — MODIFY (+8 LOC: env vars).
- `tests/findings/dedup/*` — NEW (~270 LOC).

**Phase 1 stacks on top (additional 13 new + 6 modified ≈ 2261 LOC, ~70% review surface):**

- `backend/app/pipeline/stages/text_analysis/completeness_lens.py` — NEW (~250 LOC).
- `backend/app/services/findings/document_type_detector.py` — NEW (~180 LOC).
- `prompts/pipeline/ru/text_analysis_task.md` — REPLACE with `stage01_production_prompt.md` (210 LOC vs 129 LOC).
- `prompts/pipeline/ru/completeness_lens_task.md` — NEW (~130 LOC).
- `backend/app/data/discipline_checklists/{AR,KJ,KM,EOM,OV,VK,SS,MULTI}.md` — NEW (8 files × ~115 LOC = 920 LOC).
- `backend/app/pipeline/stages/text_analysis/runner.py` — MODIFY (+40 LOC: parallel lens dispatch).
- `backend/app/pipeline/stages/prepare/task_builder.py` — MODIFY (+15 LOC: inject DOCUMENT_TYPE).
- `backend/app/services/llm/claude_runner.py` — MODIFY (+25 LOC: `run_completeness_lens`).
- `backend/app/services/findings/findings_service.py` — MODIFY (+50 LOC: lens merger).
- `backend/app/core/config.py` — MODIFY (+15 LOC: more env vars).
- `backend/app/schemas/text_analysis.json` — MODIFY (+25 LOC: new optional fields).
- `tests/*` — NEW.

**Total review surface (effective signal, ~70% of LOC):**

- Phase 0 alone: ~820 LOC.
- Phase 0 + Phase 1: ~2400 LOC.

## 10. Safest rollout

From [`phase0_rollout.md`](../rollout/phase0_rollout.md),
[`phase1_rollout.md`](../rollout/phase1_rollout.md).

### Phase 0 (28-day arc)

1. **Day 0:** Merge code with `STAGE01_DEDUP_ENABLED=false`. CI regression
   suite green on golden 24-case dataset. No behavioural change.
2. **Day 1-3:** Flip on for staging (`STAGE01_DEDUP_ENABLED=true`). Validate
   `critical_collapsed_count = 0` across 5-10 projects.
3. **Day 4-7:** Enable for 5% of production (sample by hash of project_id).
   Watch FP-estimate alarm (AL-09), `same_class_drops` distribution.
4. **Day 8-14:** Expand to 25% of production. Same telemetry.
5. **Day 15-28:** Expand to 100%. Trigger rollback if `critical_collapsed_count
   > 0` or FP-estimate up > 25% rolling 7-day.

### Phase 1 (33-day to first launch; ~123-day to full discipline coverage)

1. **Pre-merge:** Code merged with `STAGE01_COMPLETENESS_LENS_ENABLED=false`.
2. **Day 0-5:** Shadow mode — lens runs but output not surfaced. Compare logs.
3. **Day 6-12:** Opt-in `audit_comparison` for 10 projects, surfaced in UI
   with experimental flag. Engineer feedback loop active.
4. **Day 13-30:** Opt-in `specification_only` for 10 projects.
5. **Day 31-60:** Per-discipline expansion within opt-in doc_types (one
   discipline at a time, observe 7-day stability per discipline).
6. **Day 61-90:** Opt-in `tz_vs_rd` for hand-picked cases.
7. **Day 91-123:** Stable. `full_rd` REMAINS DISABLED pending separate
   research round.

Approval matrix per step is in [`phase1_rollout.md`](../rollout/phase1_rollout.md).

## 11. Rollback

### Phase 0
- `STAGE01_DEDUP_ENABLED=false` (env-var change) → next run is plain A0.
- No data corruption possible — dedup is post-process only.
- Time to rollback: < 1 minute (env-var flip).

### Phase 1
- `STAGE01_COMPLETENESS_LENS_ENABLED=false` → all projects use A0 only.
- Or `STAGE01_COMPLETENESS_BY_DOC_TYPE=""` → no doc_type opts in.
- Per-discipline rollback: `STAGE01_DISCIPLINE_DISABLE_LIST="EOM,OV"` etc.
- Per-project rollback: re-audit via existing `version_service` (production
  pattern).
- Time to rollback: < 1 minute (env-var flip); < 1 hour to fully drain
  in-flight requests.

Both phases have **no schema-breaking migrations**. v1 readers must tolerate
unknown v2 fields — validated as a precondition in
[`migration_plan.md`](../migration/migration_plan.md).

## 12. How to measure success

| Phase | Metric | Target | Window |
|---|---|---|---|
| Phase 0 | `critical_collapsed_count` | = 0 (always) | continuous |
| Phase 0 | `dedup_report.same_class_drops` | > 0 on multi-source merges (proves it's doing something) | rolling 7d |
| Phase 0 | FP-estimate rolling delta | ≤ 0 (Phase 0 should reduce or be no-op) | rolling 14d |
| Phase 1 (audit_comparison) | matched_gt | ≥ A0 baseline | per-project |
| Phase 1 (audit_comparison) | missed_critical | ≤ A0 baseline | rolling 30d |
| Phase 1 (audit_comparison) | engineer rejection rate | ≤ 25% of new findings | rolling 30d |
| Phase 1 (audit_comparison) | `is_beyond_gt_useful` tagged rate | ≥ 10% of completeness findings | rolling 30d |
| Phase 1 (any) | lens failure rate | ≤ 5% | rolling 7d |
| Phase 1 (any) | LLM cost delta | ≤ +70% per project | rolling 30d |

## 13. After Phase 0

If Phase 0 stable for 28 days with `critical_collapsed_count = 0` and FP-rate
not worse:

- Open the gate for Phase 1 pre-merge.
- Run [`stochasticity_strategy.md`](../tests/stochasticity_strategy.md) 3-run
  experiment on 6 informative cases. Required before Phase 1 leaves shadow
  mode.
- Update `paid_cost_dashboard.py` to split cost by lens.

## 14. After Phase 1 (audit_comparison + specification_only)

Once Phase 1 stable for the two opt-in doc_types over 30 days:

- Schedule a separate research round to address the `full_rd` block:
  - Run A1-v2 on 6+ `full_rd` cases with the trimmed cap=6 completeness prompt
    (already in this package).
  - Verify FP regression < +15% vs A0 on `full_rd`.
  - Verify `is_beyond_gt_useful` tagging reliability (rate of tag presence on
    real beyond-GT findings ≥ 70%).
  - Verify stochasticity IQR/median ≤ 0.25 on 3-run experiment.
- If those pass, separate implementation task can unlock `full_rd` opt-in.
- If they fail, consider conditional cross_discipline lens (Phase 2 territory).

## 15. Can we move to the implementation task?

**Phase 0 — YES.** This package contains everything needed for a separate
implementation task to:

- Add `backend/app/services/findings/dedup/` package.
- Wire `STAGE01_DEDUP_ENABLED` into `findings_service.py` and `findings_merge`.
- Add unit + regression tests.
- Roll out per [`phase0_rollout.md`](../rollout/phase0_rollout.md).

**Phase 1 — YES, opt-in path.** This package contains everything for a
separate implementation task to:

- Add `completeness_lens.py`, `document_type_detector.py`.
- Drop the new prompts and checklists into production paths.
- Wire env vars and routing.
- Run shadow mode → canary → opt-in (audit_comparison only initially).
- Per [`phase1_rollout.md`](../rollout/phase1_rollout.md).

**Phase 1 on `full_rd` — NO.** Requires a separate research round first (see
§14).

## 16. Files of record

| File | What it is |
|---|---|
| [`../README.md`](../README.md) | Package entry point |
| [`final_verdict.md`](final_verdict.md) | One-page decision matrix |
| [`../prompts/`](../prompts/) | Drop-in prompts (5 files) |
| [`../schemas/`](../schemas/) | document_type + finding schema v2 + detector (5 files) |
| [`../checklists/`](../checklists/) | 8 disciplines + rules + applicability (10 files) |
| [`../dedup/`](../dedup/) | Production-ready dedup modules + safety (5 files) |
| [`../telemetry/`](../telemetry/) | 6 telemetry docs |
| [`../rollout/`](../rollout/) | 6 rollout / guardrails / routing docs |
| [`../integration_plan/`](../integration_plan/) | 6 per-file change plans |
| [`../tests/`](../tests/) | 5 test strategy docs |
| [`../migration/`](../migration/) | 2 migration docs |
| [`../examples/`](../examples/) | 9 worked examples |

## 17. What was explicitly **NOT** done

- No production code modified.
- No production prompts modified.
- No production schemas modified.
- No `manager.py` modified.
- No deploy, no production commit.
- No LLM calls (this whole package is design + drop-in artifacts).
- No multi-agent architecture redesign (rejected by research).
- No reviewer / cross_discipline lens added (deferred to Phase 2).
