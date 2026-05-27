# Routing Rules — document_type → Phase 1

**Date:** 2026-05-20
**Scope:** Concrete map from detected `document_type` to Phase 1 behavior,
caps, and applicable checklists. Source of truth for the
`STAGE01_COMPLETENESS_BY_DOC_TYPE` matrix.

---

## 1. Master routing map

| document_type | Phase 1 enabled? (default) | Completeness cap | Checklists applied | Notes |
|---|---|---|---|---|
| `full_rd` | **NO** | 6 (if force-enabled) | discipline checklist + MULTI checklist (mandatory items only) | research: A0 49.8 → A1-v2 23.1 strict_score across 11 cases, +114 FP. Phase 1 stays OFF unless future research clears it. |
| `audit_comparison` | **YES** | 10 | discipline checklist (with audit_comparison routing rules from research [final_prompt_recommendations.md](../../algorithm_research/prompt_optimization/final_prompt_recommendations.md)) | research: A0 25.1 → A1-v2 51.4 (+26 strict_score) across 3 cases. Best Phase 1 case. |
| `specification_only` | **YES** | 10 | discipline checklist (filtered to parameter-level items) | research: A1-v2 22.0 strict_score (no A0 baseline). 3/3 GT caught on ar_03. |
| `tz_vs_rd` | **NO (opt-in only)** | 8 | discipline checklist + MULTI checklist (TZ-vs-RD diff items only) | research: A0 80.0 → A1-v2 36.0 on 1 case (multi_01) — **worse**. Opt-in per project required. |

Checklist sources:
[`../checklists/`](../checklists/) (production version, when written;
derived from [`algorithm_research/prompt_optimization/checklists/`](../../algorithm_research/prompt_optimization/checklists/)).
Checklist-to-discipline-to-doc_type mapping is documented in
[`../checklists/checklist_applicability_matrix.md`](../checklists/checklist_applicability_matrix.md)
(when written by the parallel checklists chapter).

## 2. Opt-in `tz_vs_rd` per project

Engineers can flip Phase 1 ON for a specific `tz_vs_rd` project by
adding to that project's `project_info.json`:

```json
{
  "phase1_override": {
    "completeness": true,
    "reason": "Engineer review: TZ has explicit deliverable checklist; lens helps verify."
  }
}
```

Effect:

- `STAGE01_COMPLETENESS_BY_DOC_TYPE` matrix is overridden for THIS
  project only.
- Override is logged to
  `backend/app/data/phase1_overrides.jsonl` (NEW) with timestamp,
  engineer (if available), reason.
- Subsequent re-audit on the same project_id picks up the override
  automatically until removed.

After 20 opted-in `tz_vs_rd` projects have been completed with E3
rejection rate ≤ A0 baseline + 25%, the team revisits the default in
[`phase1_rollout.md`](phase1_rollout.md) Step 5.

## 3. Manual document_type override

If the detector misclassifies a project (e.g. flags `full_rd` when it's
really `audit_comparison`), the engineer can override:

```json
{
  "document_type_override": "audit_comparison"
}
```

Effect:

- The detected value is ignored; the override is used.
- `meta.document_type.detected` still records the detector's output;
  `meta.document_type.applied` records the override.
- Override is logged to
  `backend/app/data/document_type_overrides.jsonl` with timestamp,
  detector confidence at time of override, reason.
- Routing then proceeds per the routing map (section 1) using the
  overridden type.

This is intentionally per-project, not global — global override would
require an env var change, which is heavier than this lightweight per-
project annotation.

## 4. Override priority

When both project-level overrides exist, the priority is:

1. `phase1_override.completeness` (highest) — controls the lens directly.
2. `document_type_override` — changes routing, which then drives the
   lens via section 1.
3. Detector + matrix (default) — when no override.

If `phase1_override.completeness = false` AND `document_type_override =
"audit_comparison"`, the LENS IS OFF for this project (override wins).
The matrix says "audit_comparison: YES", but the explicit
`phase1_override` outranks it.

## 5. Audit log contract

Every override produces a jsonl entry:

```json
{
  "ts": "2026-05-21T14:32:11",
  "project_id": "EOM/13АВ-РД-ЭО-К3",
  "override_type": "phase1_override",
  "old_value": "default (off)",
  "new_value": "on",
  "reason": "Engineer review: ...",
  "engineer_id": "user42",
  "session_id": "..."
}
```

Files:

- `backend/app/data/phase1_overrides.jsonl` for `phase1_override`.
- `backend/app/data/document_type_overrides.jsonl` for
  `document_type_override`.

Both files are append-only, read by the weekly review and by
`stage01_telemetry_dashboard.py` for the "overrides this week" panel.

## 6. Why two override mechanisms

Two mechanisms because they answer two different questions:

- `document_type_override` says **"the detector got the type wrong"** —
  routing should change, and so should the prompt, the checklist, and
  the cap.
- `phase1_override.completeness` says **"the type is right, but I
  specifically want / don't want the lens"** — leaves routing alone, just
  toggles the lens.

One mechanism would conflate the two, making the override log
unreadable.

## 7. References

- [FINAL_SUMMARY.md §4](../../algorithm_research/reports/FINAL_SUMMARY.md) — per-doc-type research result.
- [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) — per-case FP audit anchors the cap choices.
- [`production_guardrails.md`](production_guardrails.md) §1.2, §3 — env-var matrix and caps.
- [`phase1_rollout.md`](phase1_rollout.md) — Steps 3 and 5 reference this routing map.
- [`../checklists/checklist_applicability_matrix.md`](../checklists/checklist_applicability_matrix.md) — checklist × discipline × doc_type cross-link (written by parallel chapter).
