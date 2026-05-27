# Final Verdict — Stage 01 MD-Analysis Production Preparation

**Date:** 2026-05-20
**Package:** [`production_preparation/`](../)
**Production status:** **NOTHING CHANGED.** Design + drop-in artifacts only.

> One-pager decision matrix. Full reasoning in
> [`final_production_preparation_report.md`](final_production_preparation_report.md).

---

## Go / No-go matrix

| Item | Verdict | Why |
|---|---|---|
| **Phase 0 — dedup post-process** | ✅ **GO** | Validated no-op on A0 baseline (8 cases); +20 strict_score on legacy merged outputs. Pure Python, no LLM, feature-flag gate-able. Time-to-rollback < 1 min via env var. |
| **Phase 1 — completeness lens on `audit_comparison`** | ✅ **GO (opt-in)** | strict_score 25.1 → 51.4 (+26). document_type routing eliminates phantom-RD findings. |
| **Phase 1 — completeness lens on `specification_only`** | ✅ **GO (opt-in)** | 3/3 critical caught on tested case; routing keeps lens in scope. |
| **Phase 1 — completeness lens on `tz_vs_rd`** | ⚠ **OPT-IN per project** | Only 1 case tested; need ≥ 5 more before default opt-in. |
| **Phase 1 — completeness lens on `full_rd`** | ❌ **HOLD** | +114 FP across full_rd cases (cap=14 too aggressive). Requires cap=6 + `is_beyond_gt_useful` reliability + stochasticity 3-run. Separate research round needed. |
| **New Stage 01 prompt** (replaces `prompts/pipeline/ru/text_analysis_task.md`) | ✅ **GO (with shadow mode first)** | Adds `problem_class`, `document_type`, no-speculation rule, severity calibration. Back-compatible additive schema. |
| **Document_type detection in production** | ✅ **GO** (lands with Phase 1) | Pure Python, default-to-`full_rd` fallback is conservative (no Phase 1 fires). |
| **Reviewer agent** | ⏸ **DEFER** | Not needed — A1-v2 reaches recall without it. |
| **Cross_discipline lens / router** | ⏸ **DEFER to Phase 2** | Not justified by current data. |
| **Full 6-lens multi-agent** | ❌ **REJECTED** | ×5.2 cost, +2 recall, +145 FP. Confirmed not coming back. |

---

## Production-ready prompts

| Prompt | Status | Where it lives in this package |
|---|---|---|
| Stage 01 main prompt (drop-in for `text_analysis_task.md`) | ✅ Production-ready | [`../prompts/stage01_production_prompt.md`](../prompts/stage01_production_prompt.md) |
| Completeness Sonnet lens prompt (NEW production file) | ✅ Production-ready | [`../prompts/completeness_lens_production_prompt.md`](../prompts/completeness_lens_production_prompt.md) |
| Document_type block (reusable hint) | ✅ Production-ready | [`../prompts/stage01_document_type_block.md`](../prompts/stage01_document_type_block.md) |
| Few-shot examples (calibration corpus) | ✅ Reference | [`../prompts/stage01_few_shot_examples.md`](../prompts/stage01_few_shot_examples.md) |
| Severity calibration corpus | ✅ Reference | [`../prompts/stage01_severity_calibration.md`](../prompts/stage01_severity_calibration.md) |

---

## Production-ready checklists

| Checklist | Status | File |
|---|---|---|
| AR (Архитектурные решения) | ✅ Ready | [`../checklists/AR.md`](../checklists/AR.md) |
| KJ (Конструкции железобетонные) | ✅ Ready | [`../checklists/KJ.md`](../checklists/KJ.md) |
| KM (Конструкции металлические) | ✅ Ready (NEW) | [`../checklists/KM.md`](../checklists/KM.md) |
| EOM (Электроснабжение) | ✅ Ready | [`../checklists/EOM.md`](../checklists/EOM.md) |
| OV (Отопление, вентиляция) | ✅ Ready | [`../checklists/OV.md`](../checklists/OV.md) |
| VK (Водоснабжение, канализация) | ✅ Ready | [`../checklists/VK.md`](../checklists/VK.md) |
| SS (Слаботочные системы) | ✅ Ready | [`../checklists/SS.md`](../checklists/SS.md) |
| MULTI (cross-discipline / cross-section) | ✅ Ready | [`../checklists/MULTI.md`](../checklists/MULTI.md) |
| Shared rules | ✅ Ready | [`../checklists/checklist_rules.md`](../checklists/checklist_rules.md) |
| Applicability matrix | ✅ Ready | [`../checklists/checklist_applicability_matrix.md`](../checklists/checklist_applicability_matrix.md) |

---

## Mandatory guardrails (must be in place)

From [`../rollout/production_guardrails.md`](../rollout/production_guardrails.md):

**Phase 0 guardrails (4):**
1. `STAGE01_DEDUP_ENABLED` env var (default `false`).
2. `STAGE01_DEDUP_FUZZY_THRESHOLD` env var (default `0.7`).
3. `critical_collapsed_count` must be 0 in all dedup_report objects (asserted + alarm AL-01).
4. Dedup is fail-open — exception → log + skip dedup, return original findings.

**Phase 1 guardrails (8 on top of Phase 0):**
5. `STAGE01_COMPLETENESS_LENS_ENABLED` env var (default `false`).
6. `STAGE01_COMPLETENESS_BY_DOC_TYPE` map (initial: `"audit_comparison"` only).
7. `STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD=6` / `..._OTHER=10`.
8. `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE=true` (default).
9. `STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN=0.7`.
10. `STAGE01_DISCIPLINE_DISABLE_LIST` per-discipline kill-switch.
11. Engineer-rejection feedback loop wired (AL-14).
12. Auto-shutoff on rolling-24h FP regression > 50% (AL-09 → AL-10).

---

## Mandatory telemetry (must be in place)

From [`../telemetry/telemetry_plan.md`](../telemetry/telemetry_plan.md):

- Findings count + severity distribution (per project, per day).
- `dedup_report.same_class_drops` + `critical_collapsed_count` (Phase 0).
- `document_type` distribution + confidence histogram (Phase 1).
- Completeness lens applied/not, lens duration, lens failure rate (Phase 1).
- LLM cost split by lens (extends `paid_cost_dashboard.py`).
- FP-estimate (3-signal composite: speculative-keyword + low-confidence-no-norm + engineer-rejection-7d).
- Critical recall proxies (per-discipline KRIT-rate, per-doc-type KRIT-rate).

Alarms catalog: 28 entries (AL-01 to AL-28) in
[`../telemetry/production_alerts.md`](../telemetry/production_alerts.md).

---

## Most dangerous risks

| Risk | Severity | Mitigation |
|---|---|---|
| Phase 1 accidentally enabled for `full_rd` → +114 FP | HIGH (if mis-routed) | Routing is doc_type-keyed and confidence-gated; default fallback is `full_rd` which disables Phase 1; auto-shutoff if FP-estimate spikes |
| Sonnet timeout/failure silently degrades audit | MEDIUM | `STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE=true` enforced; alarm AL-13 |
| Dedup drops КРИТИЧЕСКОЕ silently | LOW (proven impossible) | Severity-first canonical_score + hard-asserted `critical_collapsed_count = 0`; mathematical proof in [`../dedup/dedup_safety.md`](../dedup/dedup_safety.md) |
| Stochasticity unknown (no 3-run yet) | MEDIUM | 3-run gate in [`../tests/stochasticity_strategy.md`](../tests/stochasticity_strategy.md) MUST run before Phase 1 leaves shadow |
| Schema v1 readers break on v2 | LOW | Additive-only fields; v1-tolerance is a documented precondition |

---

## Safest rollout

| Phase | Days | Stages |
|---|---|---|
| Phase 0 | 28 | merge flag-off → staging → 5% prod → 25% prod → 100% prod |
| Phase 1 | 33 to first launch, ~123 to full | merge flag-off → shadow → `audit_comparison` 10 projects → `specification_only` → per-discipline expansion → `tz_vs_rd` opt-in → (`full_rd` stays off) |

Both phases use env-var flags; rollback is < 1 minute. No schema-breaking
migration. See [`../rollout/phase0_rollout.md`](../rollout/phase0_rollout.md)
and [`../rollout/phase1_rollout.md`](../rollout/phase1_rollout.md).

---

## LOC delta in production

From [`../integration_plan/estimated_loc_changes.md`](../integration_plan/estimated_loc_changes.md):

- **Phase 0 alone:** +1168 LOC (mostly the new `dedup/` subpackage and tests).
  Modified production files: +53 LOC across 3 files (`findings_service.py`,
  `findings_merge/runner.py`, `core/config.py`). Effective review surface ~820 LOC.
- **Phase 0 + Phase 1:** +3429 LOC (Phase 1 adds 13 new files including 8 discipline checklists and the lens module, plus ~250 LOC across 6 modified production files). Effective review surface ~2400 LOC.

Highest-risk modifications:

- `prompts/pipeline/ru/text_analysis_task.md` — FULL REPLACEMENT (129 → 210 lines). Mitigation: shadow mode for first 5 days; `STAGE01_USE_LEGACY_PROMPT` env hatch as additional rollback path.
- `backend/app/pipeline/stages/text_analysis/runner.py` — +40 LOC for parallel lens dispatch. Mitigation: lens call is gated on env var; fall-open default.
- `backend/app/services/findings/findings_service.py` — +80 LOC across both phases. Mitigation: all new code paths gated on env vars.

---

## Final answer

> **Phase 0 is ready to ship. Phase 1 is ready to ship as opt-in for
> `audit_comparison` and `specification_only`. `full_rd` is NOT ready and
> requires a separate research round to unblock. Everything needed to do
> the implementation is in this package; no more design work is required to
> open the implementation task.**

✅ Phase 0 → production: GO.
✅ Phase 1 (audit_comparison + specification_only) → production: GO (opt-in).
⚠ Phase 1 (tz_vs_rd) → production: per-project opt-in.
❌ Phase 1 (full_rd) → HOLD.
✅ Implementation task can be opened now for Phase 0 + Phase 1 (opt-in).
