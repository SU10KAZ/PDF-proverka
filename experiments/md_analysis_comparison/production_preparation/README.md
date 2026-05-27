# Production Preparation — Stage 01 MD-Analysis Upgrade

**Date:** 2026-05-20
**Owner:** `experiments/md_analysis_comparison/production_preparation/`
**Production status:** **NOTHING CHANGED IN PRODUCTION.** This is a design / prep package.

---

## Why this package exists

The research stand (`experiments/md_analysis_comparison/`) and its sub-stand
(`algorithm_research/`) finished with a clear verdict (see
[algorithm_research/reports/FINAL_SUMMARY.md](../algorithm_research/reports/FINAL_SUMMARY.md)):

- **Full multi-agent architecture is NOT needed.**
- The main quality lift comes from prompts, not orchestration:
  - improved Stage 01 prompt (problem_class, evidence rules, no-speculation),
  - `document_type` awareness,
  - discipline awareness via checklists,
  - completeness logic (Sonnet lens on top of current Stage 01),
  - class + fuzzy dedup as a safety-net post-process.
- **Phase 0** (class_dedup + fuzzy_dedup as feature-flagged post-process) is
  production-ready: proven no-op on current Stage 01 outputs, +20 strict_score
  on legacy multi-source merged outputs.
- **Phase 1** (Sonnet `completeness` lens + checklists + `document_type`
  routing) is opt-in production-ready for `audit_comparison` and
  `specification_only` documents. **NOT ready as a blanket replacement for
  full_rd** — per-doc-type gating required.

This folder packages everything needed to *implement* that integration as a
separate task, without doing the implementation here.

## Hard constraints (do not violate)

- Production pipeline files (`backend/app/pipeline/manager.py`,
  `backend/app/pipeline/stages/text_analysis/runner.py`,
  `backend/app/pipeline/stages/findings_merge/runner.py`,
  `backend/app/services/findings/findings_service.py`,
  `prompts/pipeline/ru/text_analysis_task.md`, etc.) are **NOT** modified
  in this package. Everything that looks like a production drop-in lives
  under this folder and is labelled `*_production_ready.md` / `*.py.proposed`.
- All LLM calls in any test runner under this folder go through the
  Claude Code subscription via `claude -p`. Models allowed:
  `claude-opus-4-7`, `claude-sonnet-4-6`. No OpenAI / Gemini / Ollama / VLLM.
- No deploy. No production commit. No integration. Only artefacts.

## Folder map

```
production_preparation/
├── README.md                                ← this file (the entry point)
├── prompts/
│   ├── stage01_production_prompt.md         ← drop-in replacement for prompts/pipeline/ru/text_analysis_task.md
│   ├── stage01_few_shot_examples.md         ← good / bad / speculative / dup examples
│   ├── stage01_severity_calibration.md      ← severity-rubric examples + counter-examples
│   ├── stage01_document_type_block.md       ← reusable doc_type hint block
│   └── completeness_lens_production_prompt.md ← Sonnet completeness lens (Phase 1)
├── checklists/                              ← discipline checklists production version
│   ├── AR.md / KJ.md / KM.md / EOM.md / OV.md / VK.md / SS.md / MULTI.md
│   ├── checklist_rules.md                   ← shared rules: mandatory vs recommended, severity mapping
│   └── checklist_applicability_matrix.md    ← discipline × document_type × applicability
├── schemas/
│   ├── document_type_design.md              ← the 4 document types + routing semantics
│   ├── document_type_examples.md            ← example MDs labelled with expected type
│   ├── document_type_detection_rules.py     ← Python detection module (drop-in)
│   ├── finding_schema_v2.md                 ← added fields: problem_class, affected_system, ...
│   └── meta_schema.md                       ← meta.dedup_report, meta.document_type, meta.completeness_applied
├── dedup/
│   ├── class_dedup.py                       ← production-ready class_dedup (drop-in)
│   ├── fuzzy_dedup.py                       ← production-ready fuzzy_dedup
│   ├── problem_class_rules.md               ← problem_class vocabulary + canonicalisation
│   ├── dedup_thresholds.md                  ← thresholds & their justifications
│   └── dedup_safety.md                      ← why dedup cannot drop КРИТИЧЕСКОЕ silently
├── telemetry/
│   ├── telemetry_plan.md
│   ├── metrics_definition.md
│   ├── fp_monitoring.md
│   ├── critical_recall_monitoring.md
│   ├── review_load_monitoring.md
│   └── production_alerts.md
├── rollout/
│   ├── phase0_rollout.md                    ← dedup only, no LLM, feature-flag gate
│   ├── phase1_rollout.md                    ← completeness lens, opt-in by document_type
│   ├── rollback_strategy.md                 ← unwind path per phase
│   ├── ab_testing_strategy.md               ← shadow + canary + N-project A/B
│   ├── production_guardrails.md             ← caps, switches, fallbacks
│   └── routing_rules.md                     ← which doc_type opts in to Phase 1
├── integration_plan/
│   ├── phase0_integration.md                ← exactly which files / methods / call-sites
│   ├── phase1_integration.md                ← exactly which files / methods / call-sites
│   ├── files_to_modify.md                   ← consolidated list
│   ├── estimated_loc_changes.md
│   ├── estimated_risk.md
│   └── rollback_steps.md
├── tests/
│   ├── test_plan.md
│   ├── regression_strategy.md
│   ├── golden_dataset_strategy.md
│   ├── stochasticity_strategy.md
│   └── production_validation_strategy.md
├── migration/
│   ├── migration_plan.md                    ← schema-v1 → schema-v2 (findings)
│   └── data_backfill.md                     ← what about old projects' 03_findings.json
├── examples/
│   ├── before_after_findings.md
│   ├── dedup_examples.md
│   ├── severity_calibration_examples.md
│   ├── document_type_examples.md
│   ├── checklist_examples.md
│   ├── good_findings.md
│   ├── bad_findings.md
│   ├── speculative_findings.md
│   └── duplicate_collapse_examples.md
└── reports/
    ├── final_production_preparation_report.md
    └── final_verdict.md
```

## Reading order (5 minutes)

1. [reports/final_production_preparation_report.md](reports/final_production_preparation_report.md) — overall story.
2. [reports/final_verdict.md](reports/final_verdict.md) — one-pager.
3. [rollout/phase0_rollout.md](rollout/phase0_rollout.md) — Phase 0 plan (deploy-now-able).
4. [rollout/phase1_rollout.md](rollout/phase1_rollout.md) — Phase 1 plan (opt-in).
5. [integration_plan/files_to_modify.md](integration_plan/files_to_modify.md) — what touches production.

## What is "production-ready" here

- **Phase 0 (class_dedup + fuzzy_dedup post-process):** ✅ Production-ready.
  Safe-by-construction: validated on 8 cases (no-op on current Stage 01;
  removes 18% FP on legacy merged outputs). Feature-flag gate-able.
- **Phase 1 — opt-in for `audit_comparison` and `specification_only`:**
  ✅ Production-ready as opt-in. Validated qualitatively (cross_01, ar_03).
  HALVES per-case missed-critical rate on tested cases.
- **Phase 1 — `tz_vs_rd`:** ⚠ Opt-in only for hand-picked cases.
  Limited data (1 case so far).
- **Phase 1 — `full_rd` (blanket):** ❌ NOT production-ready. Completeness
  lens is too aggressive (+114 FP across full_rd cases vs A0). Hold until
  completeness cap is tuned (current 14 → ~6 for full_rd) AND
  `is_beyond_gt_useful` tagging is reliable. **Requires more research.**

See [reports/final_verdict.md](reports/final_verdict.md) for the one-page
go/no-go matrix.

## Empirical anchors (data we relied on)

| Source | What it gives us |
|---|---|
| [../reports/final_comparison_report.md](../reports/final_comparison_report.md) | Original architecture-vs-architecture comparison (single-pass vs 6-agent) |
| [../algorithm_research/reports/FINAL_SUMMARY.md](../algorithm_research/reports/FINAL_SUMMARY.md) | 24-case Phase 0 / Phase 1 validation |
| [../algorithm_research/reports/final_verdict.md](../algorithm_research/reports/final_verdict.md) | One-page algorithm verdict |
| [../algorithm_research/reports/phase0_phase1_validation_report.md](../algorithm_research/reports/phase0_phase1_validation_report.md) | Gating evaluation |
| [../algorithm_research/reports/a1v2_fp_audit.md](../algorithm_research/reports/a1v2_fp_audit.md) | FP categorisation: speculative_noise=0 |
| [../algorithm_research/prompt_optimization/final_prompt_recommendations.md](../algorithm_research/prompt_optimization/final_prompt_recommendations.md) | v1 vs v2 prompt verdict |
| [../algorithm_research/prompt_optimization/prompt_diagnostics.md](../algorithm_research/prompt_optimization/prompt_diagnostics.md) | What was wrong with baseline |
| [../algorithm_research/runners/class_dedup.py](../algorithm_research/runners/class_dedup.py) | Reference dedup implementation |

## Out of scope for this package

- Conditional cross_discipline lens (router-gated). Deferred to Phase 2;
  not justified by current data.
- Reviewer agent. Not needed — A1-v2 reaches recall without it.
- Multi-agent safety / normative / calculations lenses. Redundant with
  current pipeline; rejected.
- 6-lens architecture. Rejected (×5.2 cost, +2 recall, +145 FP).
