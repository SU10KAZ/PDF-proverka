"""Deterministic verification of a candidate selection, never model prose."""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from backend.app.services.common.electrical_values import parse_cable

from ..production_artifacts import content_signature
from . import schemas


def _signature(candidate: Mapping[str, Any]) -> str:
    core = {key: value for key, value in candidate.items() if key != "candidate_signature"}
    return content_signature(core)


def _all_refs(candidate: Mapping[str, Any]) -> list[str]:
    return sorted({
        str(ref)
        for key in (
            "left_refs", "right_refs", "entity_refs", "graph_refs",
            "table_refs", "text_refs",
        )
        for ref in candidate.get(key) or ()
        if str(ref)
    })


def _grounded_cable_values(
    candidate: Mapping[str, Any], catalog: Mapping[str, Mapping[str, Any]]
) -> bool:
    values = candidate.get("values") or {}
    if "before" not in values or "after" not in values:
        return True
    candidate_type = str(candidate.get("candidate_type") or "")
    if not (
        candidate_type.startswith("REAL_CHANGE_")
        or candidate_type.startswith("FORMATTING_ONLY_")
    ):
        return True

    def counts(refs: Sequence[Any]) -> set[int]:
        output: set[int] = set()
        for ref in refs:
            item = catalog.get(str(ref)) or {}
            raw = list(item.get("cables") or ())
            attrs = item.get("attrs") or {}
            if isinstance(attrs, Mapping):
                raw.extend(attrs.get("cables") or ())
            for text in raw:
                parsed = parse_cable(text)
                if parsed and parsed.get("parallel_count_proven") is True:
                    output.add(int(parsed["parallel_count"]))
        return output

    return (
        int(values["before"]) in counts(candidate.get("left_refs") or ())
        and int(values["after"]) in counts(candidate.get("right_refs") or ())
    )


def _units_grounded(
    candidate: Mapping[str, Any], catalog: Mapping[str, Mapping[str, Any]]
) -> bool:
    declared = {
        str(value).casefold()
        for raw in (candidate.get("units") or {}).values()
        for value in (raw if isinstance(raw, list) else [raw])
        if value not in (None, "")
    }
    if not declared:
        return True
    observed: set[str] = set()
    for ref in _all_refs(candidate):
        for item in (catalog.get(ref) or {}).get("values") or ():
            if isinstance(item, Mapping) and item.get("unit"):
                observed.add(str(item["unit"]).casefold())
    return declared <= observed


def _human_protected_ids(artifacts: Mapping[str, Mapping[str, Any]]) -> set[str]:
    output: set[str] = set()
    for decision in (artifacts.get("engineer_decisions") or {}).get("decisions") or ():
        if not isinstance(decision, Mapping):
            continue
        human = decision.get("human_decision")
        state = str(decision.get("decision") or "")
        if not isinstance(human, Mapping) and state in {"", "PENDING_REVIEW"}:
            continue
        for key in ("target_id", "change_id", "finding_id", "decision_id"):
            if decision.get(key):
                output.add(str(decision[key]))
    return output


def verify_selection(
    *,
    task: Mapping[str, Any],
    selection: Mapping[str, Any],
    catalog: Mapping[str, Mapping[str, Any]],
    artifacts: Mapping[str, Mapping[str, Any]],
    frozen_fast_signature: str,
    current_fast_signature: str,
) -> dict[str, Any]:
    errors: list[str] = []
    task_id = str(task.get("task_id") or "")
    if str(selection.get("task_id") or "") != task_id:
        errors.append("TASK_BINDING_MISMATCH")
    candidate_id = str(selection.get("selected_candidate_id") or "")
    by_id = {
        str(value.get("candidate_id") or ""): value
        for value in task.get("candidates") or () if isinstance(value, Mapping)
    }
    candidate = by_id.get(candidate_id)
    if candidate is None:
        errors.append("CANDIDATE_NOT_IN_TASK")
    elif candidate_id not in set(task.get("selectable_candidate_ids") or ()):
        errors.append("CANDIDATE_PREFILTERED")
    if frozen_fast_signature != current_fast_signature:
        errors.append("FROZEN_INPUT_CHANGED")
    if candidate is not None:
        if candidate.get("candidate_signature") != _signature(candidate):
            errors.append("CANDIDATE_SIGNATURE_MISMATCH")
        missing = [ref for ref in _all_refs(candidate) if ref not in catalog]
        if missing:
            errors.append("EVIDENCE_REF_MISSING:" + ",".join(missing))
        if not _grounded_cable_values(candidate, catalog):
            errors.append("PREBOUND_VALUE_NOT_GROUNDED")
        if not _units_grounded(candidate, catalog):
            errors.append("UNIT_NOT_GROUNDED")
        failed = [
            str(value.get("code") or "")
            for value in candidate.get("proof_requirements") or ()
            if isinstance(value, Mapping) and value.get("status") == "FAILED"
        ]
        if failed:
            errors.append("PROOF_REQUIREMENT_FAILED:" + ",".join(failed))
        protected = _human_protected_ids(artifacts)
        targets = set(task.get("affected_target_ids") or ())
        targets.update((candidate.get("materialization") or {}).get("affected_target_ids") or ())
        if protected & targets:
            errors.append("HUMAN_DECISION_HAS_PRIORITY")

    if errors:
        status = schemas.INVALID_RESPONSE if (
            "CANDIDATE_NOT_IN_TASK" in errors or "TASK_BINDING_MISMATCH" in errors
        ) else schemas.REJECTED_SELECTION
    elif candidate is None:
        status = schemas.INVALID_RESPONSE
    elif (
        candidate.get("eligibility") != schemas.AUTO
        or candidate.get("resolution_effect") == "HUMAN_REQUIRED"
    ):
        status = schemas.HUMAN_REQUIRED
    else:
        status = schemas.VERIFIED_SELECTION
    return {
        "task_id": task_id,
        "selected_candidate_id": candidate_id or None,
        "candidate_signature": (
            candidate.get("candidate_signature") if candidate is not None else None
        ),
        "status": status,
        "ok": status in {schemas.VERIFIED_SELECTION, schemas.HUMAN_REQUIRED},
        "errors": errors,
        "checks": {
            "candidate_exists": candidate is not None,
            "candidate_belongs_to_task": candidate_id in by_id,
            "candidate_signature_matches": bool(candidate) and not any(
                error == "CANDIDATE_SIGNATURE_MISMATCH" for error in errors
            ),
            "all_refs_exist": not any(error.startswith("EVIDENCE_REF_MISSING") for error in errors),
            "values_grounded": "PREBOUND_VALUE_NOT_GROUNDED" not in errors,
            "units_grounded": "UNIT_NOT_GROUNDED" not in errors,
            "proof_obligations_pass": not any(error.startswith("PROOF_REQUIREMENT_FAILED") for error in errors),
            "frozen_input_valid": "FROZEN_INPUT_CHANGED" not in errors,
            "human_priority_preserved": "HUMAN_DECISION_HAS_PRIORITY" not in errors,
        },
        "verifier_version": schemas.VERIFIER_VERSION,
    }


def verify_batch_response(
    tasks: Sequence[Mapping[str, Any]], payload: Mapping[str, Any]
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Validate the tiny response before per-candidate verification."""
    errors: list[str] = []
    rows = payload.get("selections")
    if not isinstance(rows, list):
        return {}, ["SCHEMA_SELECTIONS_NOT_ARRAY"]
    expected = [str(task.get("task_id") or "") for task in tasks]
    output: dict[str, dict[str, Any]] = {}
    allowed_fields = {
        "task_id", "selected_candidate_id", "confidence_bucket",
        "optional_short_reason",
    }
    for row in rows:
        if not isinstance(row, Mapping):
            errors.append("SCHEMA_SELECTION_NOT_OBJECT")
            continue
        if set(row) != allowed_fields:
            errors.append("SCHEMA_FIELDS_INVALID")
            continue
        task_id = str(row.get("task_id") or "")
        if task_id in output:
            errors.append(f"DUPLICATE_TASK:{task_id}")
            continue
        if row.get("confidence_bucket") not in schemas.CONFIDENCE_BUCKETS:
            errors.append(f"CONFIDENCE_INVALID:{task_id}")
        output[task_id] = dict(row)
    if set(output) != set(expected) or len(output) != len(expected):
        errors.append("TASK_ACCOUNTING_MISMATCH")
    return output, errors


__all__ = ["verify_batch_response", "verify_selection"]
