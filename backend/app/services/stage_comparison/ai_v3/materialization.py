"""Project verified backend candidates through the existing deterministic path."""
from __future__ import annotations

import copy
from collections import Counter
from typing import Any, Mapping

from backend.app.pipeline.stages.block_grounding.electrical_table_diff import (
    compare_match,
)

from ..ai import identity as table_identity
from ..ai_v2 import schemas as v2_schemas
from ..ai_v2.materialization import materialize_verified_resolutions
from ..production_artifacts import content_signature, stable_id
from . import schemas

SOURCE = "AI_ANALYST_V3"


def pending_manual_audit(run: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": "stage_comparison_ai_v3_manual_audit",
        "schema_version": "stage-comparison-ai-v3-manual-audit.v1",
        "status": "PENDING_MANUAL_AUDIT",
        "items": [
            {
                "task_id": value.get("task_id"),
                "selected_candidate_id": value.get("selected_candidate_id"),
                "manual_verdict": None,
                "note": "",
            }
            for value in run.get("stable_selections") or ()
            if isinstance(value, Mapping)
            and value.get("status") == schemas.VERIFIED_SELECTION
        ],
    }


def _audit_index(audit: Mapping[str, Any] | None) -> dict[str, str]:
    if not isinstance(audit, Mapping) or audit.get("status") != "COMPLETE":
        return {}
    return {
        str(value.get("task_id") or ""): str(value.get("manual_verdict") or "")
        for value in audit.get("items") or ()
        if isinstance(value, Mapping) and value.get("task_id")
    }


def _candidate_index(factory: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(candidate.get("candidate_id") or ""): candidate
        for task in factory.get("tasks") or () if isinstance(task, Mapping)
        for candidate in task.get("candidates") or () if isinstance(candidate, Mapping)
    }


def _task_index(factory: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(task.get("task_id") or ""): task
        for task in factory.get("tasks") or () if isinstance(task, Mapping)
    }


def _selected_supported(
    run: Mapping[str, Any], audit: Mapping[str, Any] | None,
) -> list[Mapping[str, Any]]:
    audited = _audit_index(audit)
    return [
        value for value in run.get("stable_selections") or ()
        if isinstance(value, Mapping)
        and value.get("status") == schemas.VERIFIED_SELECTION
        and audited.get(str(value.get("task_id") or "")) == "SUPPORTED"
    ]


def _adapter_record(
    selection: Mapping[str, Any],
    task: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any] | None:
    materialization = candidate.get("materialization") or {}
    kind = str(materialization.get("kind") or "")
    evidence_refs = sorted({
        str(ref)
        for key in (
            "left_refs", "right_refs", "entity_refs", "graph_refs",
            "table_refs", "text_refs",
        )
        for ref in candidate.get(key) or () if str(ref)
    })
    binding_refs = sorted({
        str(ref)
        for key in ("left_refs", "right_refs")
        for ref in candidate.get(key) or () if str(ref)
    })
    if kind in {"TABLE_ROW_IDENTITY", "ENTITY_IDENTITY"}:
        task_type = (
            v2_schemas.TABLE_ROW_IDENTITY
            if kind == "TABLE_ROW_IDENTITY" else v2_schemas.FUNCTIONAL_IDENTITY
        )
        verdict = "SAME_ENTITY"
    elif kind == "CHANGE_INTERPRETATION":
        task_type = v2_schemas.CHANGE_INTERPRETATION
        verdict = str(materialization.get("verdict") or "")
    elif kind == "LABEL_CONFLICT":
        task_type = v2_schemas.LABEL_CONFLICT
        verdict = "DOCUMENT_ERROR"
    else:
        return None
    task_id = str(selection.get("task_id") or "")
    return {
        "task_id": task_id,
        "task_type": task_type,
        "source_kind": materialization.get("source_kind") or task.get("source_kind"),
        "status": "AI_RESOLVED_VERIFIED",
        "reason_code": None,
        "reason_detail": "",
        "published": True,
        "saves_human_decision": False,
        "resolution": {
            "task_id": task_id,
            "task_type": task_type,
            "status": "RESOLVED",
            "verdict": verdict,
            "selected_candidate_refs": binding_refs,
            "evidence_refs": evidence_refs,
            "claims": [],
            "confidence": selection.get("confidence_bucket") or "LOW",
            "requested_evidence": [],
            "engineering_summary": candidate.get("summary") or "",
            "human_question": None,
        },
        "verifier": {
            "ok": True,
            "errors": [],
            "version": schemas.VERIFIER_VERSION,
            "candidate_id": candidate.get("candidate_id"),
            "candidate_signature": candidate.get("candidate_signature"),
        },
    }


def _derived_table(
    records: list[Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    direct = artifacts.get("direct_page_mode2") or {}
    tables = ((direct.get("diagnostics") or {}).get("electrical_load_tables") or {})
    rows = {
        str(row.get("row_id") or ""): row
        for side in ("LEFT", "RIGHT")
        for row in (tables.get(side) or {}).get("rows") or ()
        if isinstance(row, Mapping)
    }
    matches = []
    for record in records:
        if record.get("task_type") != v2_schemas.TABLE_ROW_IDENTITY:
            continue
        candidate_id = str((record.get("verifier") or {}).get("candidate_id") or "")
        candidate = candidates.get(candidate_id) or {}
        materialization = candidate.get("materialization") or {}
        left_id = str(materialization.get("left_row_id") or "")
        right_id = str(materialization.get("right_row_id") or "")
        if left_id not in rows or right_id not in rows:
            continue
        matches.append({
            "match_id": stable_id("etm", left_id, right_id),
            "method": table_identity.METHOD_AI_IDENTITY,
            "designation": (
                rows[right_id].get("consumer_designation")
                or rows[left_id].get("consumer_designation")
                or rows[right_id].get("consumer_label")
                or rows[left_id].get("consumer_label")
            ),
            "left": dict(rows[left_id]),
            "right": dict(rows[right_id]),
            "question_id": str(record.get("task_id") or ""),
            "source_item_id": str(record.get("task_id") or ""),
        })
    return table_identity.deterministic_changes(matches, compare_match)


def _rewrite_v3(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _rewrite_v3(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_rewrite_v3(item) for item in value]
    if value == "AI_ANALYST_V2":
        return SOURCE
    return value


def _overlay_hro(
    base: Mapping[str, Any],
    *,
    supported: list[Mapping[str, Any]],
    tasks: Mapping[str, Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    result = copy.deepcopy(dict(base))
    removable: set[str] = set()
    graph_children: dict[str, list[bool]] = {}
    for selection in supported:
        task_id = str(selection.get("task_id") or "")
        task = tasks.get(task_id) or {}
        candidate = candidates.get(str(selection.get("selected_candidate_id") or "")) or {}
        question_id = str(task.get("human_question_id") or "")
        if not question_id:
            continue
        effect = str(candidate.get("resolution_effect") or "")
        if task.get("task_type") == schemas.ENTITY_IDENTITY:
            graph_children.setdefault(question_id, []).append(effect == "VERIFIED_RELATION")
        elif effect in {
            "RESOLVE_HUMAN_QUESTION", "VERIFIED_RELATION", "MATERIALIZED_CHANGE",
            "NO_CHANGE",
        }:
            removable.add(question_id)
    for question_id, children in graph_children.items():
        # Aggregate graph question disappears only if every bounded child was
        # verified. A subset must not masquerade as complete correspondence.
        all_tasks = [
            task for task in tasks.values()
            if task.get("human_question_id") == question_id
            and task.get("task_type") == schemas.ENTITY_IDENTITY
        ]
        if children and len(children) == len(all_tasks) and all(children):
            removable.add(question_id)

    original = list(result.get("standalone_questions") or ())
    kept, removed = [], []
    for question in original:
        if isinstance(question, Mapping) and question.get("question_id") in removable:
            removed.append(copy.deepcopy(dict(question)))
        else:
            kept.append(copy.deepcopy(question))
    result["standalone_questions"] = kept
    result["v3_resolved_questions"] = removed
    summary = dict(result.get("summary") or {})
    summary["standalone_human_questions"] = len(kept)
    summary["mandatory_human_interactions"] = int(summary.get("review_groups") or 0) + len(kept)
    summary["v3_stable_questions_removed"] = len(removed)
    result["summary"] = summary
    result["kind"] = "stage_comparison_human_review_plan_ai_v3_experimental"
    result["constraints"] = {
        **dict(result.get("constraints") or {}),
        "production_hro_unchanged": True,
        "mode_group_ai_resolvable": False,
        "manual_supported_only": True,
    }
    return result, sorted(removable)


def materialize_stable_selections(
    *,
    artifacts: Mapping[str, Mapping[str, Any]],
    factory: Mapping[str, Any],
    run: Mapping[str, Any],
    pair_id: str,
    manual_audit: Mapping[str, Any] | None,
) -> dict[str, Any]:
    candidates = _candidate_index(factory)
    tasks = _task_index(factory)
    supported = _selected_supported(run, manual_audit)
    records = []
    for selection in supported:
        task = tasks.get(str(selection.get("task_id") or "")) or {}
        candidate = candidates.get(str(selection.get("selected_candidate_id") or "")) or {}
        record = _adapter_record(selection, task, candidate)
        if record is not None:
            records.append(record)
    adapter_run = {
        "model": run.get("model"),
        "reasoning_effort": run.get("reasoning_effort"),
        "context_signature": run.get("candidate_set_signature"),
        "input_signature": run.get("input_signature"),
        "resolutions": records,
        "derived_table": _derived_table(records, candidates, artifacts),
    }
    delegate_audit = {
        "status": "COMPLETE",
        "items": [
            {"task_id": record["task_id"], "manual_verdict": "SUPPORTED"}
            for record in records
        ],
    }
    delegated = materialize_verified_resolutions(
        artifacts=artifacts,
        run=adapter_run,
        pair_id=pair_id,
        manual_audit=delegate_audit,
        human_entity_relations=artifacts.get("entity_relations"),
        generated_at="FROZEN",
    )
    delegated = _rewrite_v3(delegated)
    hro, removed_question_ids = _overlay_hro(
        delegated.get("human_review_plan") or artifacts.get("human_review_plan") or {},
        supported=supported,
        tasks=tasks,
        candidates=candidates,
    )
    delegated_outcomes = {
        str(value.get("task_id") or ""): value
        for value in delegated.get("outcomes") or () if isinstance(value, Mapping)
    }
    audited = _audit_index(manual_audit)
    outcomes = []
    product_fingerprints = []
    for selection in run.get("stable_selections") or ():
        if not isinstance(selection, Mapping):
            continue
        task_id = str(selection.get("task_id") or "")
        candidate_id = str(selection.get("selected_candidate_id") or "")
        candidate = candidates.get(candidate_id) or {}
        task = tasks.get(task_id) or {}
        verdict = audited.get(task_id)
        delegate = delegated_outcomes.get(task_id)
        if selection.get("status") != schemas.VERIFIED_SELECTION:
            outcome, reason = "HUMAN_REQUIRED", str(selection.get("reason_code") or "NOT_VERIFIED")
        elif verdict != "SUPPORTED":
            outcome, reason = "HUMAN_REQUIRED", (
                "MANUAL_AUDIT_PARTIAL" if verdict == "PARTIALLY_SUPPORTED"
                else "MANUAL_AUDIT_UNSUPPORTED" if verdict == "UNSUPPORTED"
                else "MANUAL_AUDIT_REQUIRED"
            )
        elif delegate is not None:
            outcome, reason = str(delegate.get("outcome") or "HUMAN_REQUIRED"), str(delegate.get("reason_code") or "")
        elif task.get("human_question_id") in removed_question_ids:
            outcome, reason = "RESOLVED_HUMAN_QUESTION", "STABLE_BOUNDED_SELECTION"
        else:
            outcome, reason = "VERIFIED_NO_PRODUCT_EFFECT", "NO_DETERMINISTIC_PRODUCT_EFFECT"
        materialized_ids = list((delegate or {}).get("materialized_finding_ids") or ())
        removed_targets = list((delegate or {}).get("removed_review_target_ids") or ())
        removed_questions = (
            [task.get("human_question_id")]
            if task.get("human_question_id") in removed_question_ids else []
        )
        row = {
            "task_id": task_id,
            "selected_candidate_id": candidate_id or None,
            "candidate_type": candidate.get("candidate_type"),
            "manual_verdict": verdict,
            "outcome": outcome,
            "reason_code": reason,
            "materialized_finding_ids": materialized_ids,
            "removed_review_target_ids": removed_targets,
            "removed_human_question_ids": removed_questions,
        }
        if outcome not in {"HUMAN_REQUIRED", "VERIFIED_NO_PRODUCT_EFFECT"}:
            fingerprint = content_signature({
                "task_id": task_id,
                "selected_candidate_id": candidate_id,
                "outcome": outcome,
                "findings": materialized_ids,
                "removed_targets": removed_targets,
                "removed_questions": removed_questions,
            })
            row["product_fingerprint"] = fingerprint
            product_fingerprints.append(fingerprint)
        outcomes.append(row)
    outcomes.sort(key=lambda value: value["task_id"])
    counts = Counter(value["outcome"] for value in outcomes)
    unsupported = sum(
        value["manual_verdict"] == "UNSUPPORTED"
        and value["outcome"] not in {"HUMAN_REQUIRED", "VERIFIED_NO_PRODUCT_EFFECT"}
        for value in outcomes
    )
    baseline_hro = artifacts.get("human_review_plan") or {}
    before = int((baseline_hro.get("summary") or {}).get("mandatory_human_interactions") or 0)
    after = int((hro.get("summary") or {}).get("mandatory_human_interactions") or before)
    artifact = {
        **delegated,
        "kind": "stage_comparison_ai_v3_materialization",
        "schema_version": "stage-comparison-ai-v3-materialization.v1",
        "materializer_version": "ai-v3-bounded-existing-pipeline-adapter.v1",
        "source": SOURCE,
        "outcomes": outcomes,
        "human_review_plan": hro,
        "product_fingerprints": sorted(product_fingerprints),
        "diagnostics": {
            **dict(delegated.get("diagnostics") or {}),
            "outcome_counts": dict(sorted(counts.items())),
            "manual_audit_status": (manual_audit or {}).get("status"),
            "manual_supported": sum(value == "SUPPORTED" for value in audited.values()),
            "manual_partially_supported": sum(value == "PARTIALLY_SUPPORTED" for value in audited.values()),
            "manual_unsupported": sum(value == "UNSUPPORTED" for value in audited.values()),
            "unsupported_materialized": unsupported,
            "human_interactions_before": before,
            "human_interactions_after": after,
            "human_interactions_saved": max(0, before - after),
            "model_calls": int((run.get("diagnostics") or {}).get("model_calls") or 0),
        },
        "constraints": {
            **dict(delegated.get("constraints") or {}),
            "model_generated_facts": False,
            "candidate_only_materialization": True,
            "manual_partials_materialized": False,
            "production_hro_unchanged": True,
            "fast_unchanged": True,
        },
    }
    artifact["input_signature"] = content_signature({
        "schema": artifact["schema_version"],
        "run": run.get("input_signature"),
        "candidate_set": factory.get("candidate_set_signature"),
        "manual_audit": audited,
        "products": sorted(product_fingerprints),
    })
    return artifact


__all__ = ["materialize_stable_selections", "pending_manual_audit"]
