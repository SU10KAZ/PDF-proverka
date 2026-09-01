"""Materialize verified AI v2 resolutions through the deterministic pipeline.

The model is allowed to resolve identity.  It is not allowed to mint a
parameter, facet, direction or approval.  This module therefore projects only
verified identity into frozen graph/table/text evidence, invokes the existing
fact/atom/entity/G2.4.6 producers, and records an exact outcome for every
unresolved task.
"""
from __future__ import annotations

import copy
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (
    compare_system_graphs,
)
from backend.app.services.common.electrical_values import parse_cable

from .. import entity_matcher
from ..engineer_review import build_engineer_decisions
from ..graphic_comparison.contract import validate_ledger
from ..graphic_comparison.graphic_change_ledger_adapter import (
    adapt_system_graph_comparison_to_ledger,
)
from ..preliminary_report import build_preliminary_report
from ..production_artifacts import content_signature, stable_id, utc_now
from ..unified_change_synthesizer import (
    ledger_to_graphic_atoms,
    synthesize_unified_changes,
    validate_synthesis,
)
from ..unified_change_synthesizer.normalization import (
    load_table_diff_to_graphic_atoms,
)
from . import schemas

SOURCE = "AI_ANALYST_V2"
RELATION_KIND = "stage_comparison_verified_entity_relations"
RELATION_SCHEMA_VERSION = "verified-entity-relations.v1"
MATERIALIZATION_KIND = "stage_comparison_ai_v2_materialization"
MATERIALIZATION_SCHEMA_VERSION = "stage-comparison-ai-v2-materialization.v1"
MATERIALIZER_VERSION = "ai-v2-deterministic-materializer-v1"

MATERIALIZED_FINDING = "MATERIALIZED_FINDING"
NO_CHANGE = "NO_CHANGE"
HUMAN_REQUIRED = "HUMAN_REQUIRED"
REJECTED_VERIFIER = "REJECTED_VERIFIER"
OUTCOMES = (
    MATERIALIZED_FINDING,
    NO_CHANGE,
    HUMAN_REQUIRED,
    REJECTED_VERIFIER,
)

_IDENTITY_TYPES = frozenset({
    schemas.ENTITY_IDENTITY,
    schemas.TABLE_ROW_IDENTITY,
    schemas.FUNCTIONAL_IDENTITY,
})


def _verifier_passed(value: Mapping[str, Any]) -> bool:
    check = value.get("verifier")
    return bool(
        isinstance(check, Mapping)
        and check.get("ok") is True
        and not list(check.get("errors") or ())
    )


def _audit_index(audit: Mapping[str, Any] | None) -> dict[str, str]:
    if not isinstance(audit, Mapping) or audit.get("status") != "COMPLETE":
        return {}
    return {
        str(item.get("task_id") or ""): str(item.get("manual_verdict") or "")
        for item in audit.get("items") or ()
        if isinstance(item, Mapping) and item.get("task_id")
    }


def _manual_supported(
    task_id: str, audit: Mapping[str, Any] | None,
) -> bool:
    if audit is not None and audit.get("status") != "COMPLETE":
        return False
    verdicts = _audit_index(audit)
    return not verdicts or verdicts.get(task_id) == "SUPPORTED"


def _candidate_pair(resolution: Mapping[str, Any]) -> tuple[str, str] | None:
    refs = [
        str(value) for value in resolution.get("selected_candidate_refs") or ()
        if isinstance(value, str) and value
    ]
    left = next((value for value in refs if value.startswith("LEFT:")), None)
    right = next((value for value in refs if value.startswith("RIGHT:")), None)
    if left and right:
        return left, right
    left = str(resolution.get("left_row_ref") or "")
    right = str(resolution.get("right_row_ref") or "")
    return (left, right) if left and right else None


def _human_relation_rows(value: Any) -> list[Mapping[str, Any]]:
    raw = value.get("relations") or [] if isinstance(value, Mapping) else value
    return [item for item in (raw or ()) if isinstance(item, Mapping)]


def _endpoint(value: Any, side: str) -> str:
    text = str(value or "")
    prefix = side + ":NODE:"
    return text[len(prefix):] if text.startswith(prefix) else text


def _relation_value(value: Mapping[str, Any]) -> str:
    return str(
        value.get(
            "effective_relation",
            value.get("relation_type", value.get("relation", value.get("status"))),
        )
        or ""
    )


def build_verified_entity_relations(
    run: Mapping[str, Any],
    *,
    manual_audit: Mapping[str, Any] | None = None,
    human_relations: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build the typed, independently replayable identity artifact."""
    human = _human_relation_rows(human_relations)
    human_left = {
        _endpoint(item.get("left_entity_ref"), "LEFT")
        for item in human
        if isinstance(item.get("human_decision"), Mapping)
        and _relation_value(item) == "SAME_ENTITY"
    }
    human_right = {
        _endpoint(item.get("right_entity_ref"), "RIGHT")
        for item in human
        if isinstance(item.get("human_decision"), Mapping)
        and _relation_value(item) == "SAME_ENTITY"
    }
    relations: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for record in run.get("resolutions") or ():
        if not isinstance(record, Mapping):
            continue
        task_id = str(record.get("task_id") or "")
        resolution = record.get("resolution")
        resolution = resolution if isinstance(resolution, Mapping) else {}
        if (
            record.get("status") != "AI_RESOLVED_VERIFIED"
            or record.get("task_type") not in _IDENTITY_TYPES
            or resolution.get("verdict") != "SAME_ENTITY"
        ):
            continue
        reason = None
        if not _verifier_passed(record):
            reason = "VERIFIER_NOT_PASS"
        elif not _manual_supported(task_id, manual_audit):
            reason = "MANUAL_AUDIT_NOT_SUPPORTED"
        pair = _candidate_pair(resolution)
        if pair is None:
            reason = reason or "ENTITY_REFS_MISSING"
        if reason:
            excluded.append({"task_id": task_id, "reason": reason})
            continue
        assert pair is not None
        left_ref, right_ref = pair
        if (
            _endpoint(left_ref, "LEFT") in human_left
            or _endpoint(right_ref, "RIGHT") in human_right
        ):
            excluded.append({
                "task_id": task_id,
                "reason": "HUMAN_CONFIRMED_RELATION_HAS_PRIORITY",
            })
            continue
        model_metadata = {
            "model": run.get("model"),
            "reasoning_effort": run.get("reasoning_effort"),
            "prompt_version": schemas.PROMPT_VERSION,
            "response_schema_version": schemas.SCHEMA_VERSION,
        }
        relation = {
            "resolution_id": stable_id(
                "aiv2_relation_", task_id, left_ref, right_ref, length=28
            ),
            "task_id": task_id,
            "left_entity_ref": left_ref,
            "right_entity_ref": right_ref,
            "relation_type": "SAME_ENTITY",
            "evidence_refs": sorted({
                str(value) for value in resolution.get("evidence_refs") or ()
                if isinstance(value, str) and value
            }),
            "verifier_result": copy.deepcopy(dict(record.get("verifier") or {})),
            "confidence": str(resolution.get("confidence") or "UNKNOWN"),
            "source": SOURCE,
            "model_metadata": model_metadata,
            "input_context_signature": run.get("context_signature"),
            "priority": "AI_VERIFIED",
        }
        relations.append(relation)
    relations.sort(key=lambda item: item["resolution_id"])
    excluded.sort(key=lambda item: (item["task_id"], item["reason"]))
    core = {
        "kind": RELATION_KIND,
        "schema_version": RELATION_SCHEMA_VERSION,
        "version": 1,
        "source": SOURCE,
        "generated_at": generated_at or utc_now(),
        "context_signature": run.get("context_signature"),
        "relations": relations,
        "excluded": excluded,
        "constraints": {
            "human_priority": True,
            "ai_parameters_allowed": False,
            "verifier_pass_required": True,
            "manual_partials_materialized": False,
        },
    }
    core["input_signature"] = content_signature({
        "schema": RELATION_SCHEMA_VERSION,
        "source_run": run.get("input_signature"),
        "context": run.get("context_signature"),
        "relations": relations,
        "excluded": excluded,
    })
    return core


def _source_atoms(
    synthesis: Mapping[str, Any], source: str,
) -> list[dict[str, Any]]:
    values: dict[str, dict[str, Any]] = {}
    for bucket in ("changes", "review_items"):
        for finding in synthesis.get(bucket) or ():
            if not isinstance(finding, Mapping):
                continue
            for atom in (finding.get("provenance") or {}).get("source_atoms") or ():
                if (
                    isinstance(atom, Mapping)
                    and atom.get("source") == source
                    and atom.get("atom_id")
                ):
                    restored = copy.deepcopy(dict(atom))
                    # The synthesis keeps only a compact source-atom witness.
                    # If a direct atom artifact is unavailable, the enclosing
                    # finding restores ordinary synthesis keys losslessly.
                    for key in (
                        "scope_ref", "subject_ref", "project_entity_ref",
                        "facet_ref", "dimension", "direction", "outcome",
                        "before_value", "after_value", "review_status",
                    ):
                        if key not in restored and key in finding:
                            restored[key] = copy.deepcopy(finding[key])
                    confidence = finding.get("confidence")
                    if "confidence" not in restored:
                        restored["confidence"] = (
                            confidence.get("level")
                            if isinstance(confidence, Mapping)
                            else confidence
                        ) or "UNKNOWN"
                    evidence_refs = finding.get("evidence_refs") or ()
                    witness = next((
                        value for value in evidence_refs
                        if isinstance(value, Mapping)
                        and value.get("atom_id") == atom.get("atom_id")
                    ), {})
                    restored.setdefault("evidence_ref", witness.get("evidence_ref"))
                    restored.setdefault("source_artifact", witness.get("source_artifact"))
                    values[str(atom["atom_id"])] = restored
    return sorted(values.values(), key=lambda item: str(item.get("atom_id") or ""))


def _baseline_atoms(
    artifacts: Mapping[str, Mapping[str, Any]], source: str,
) -> list[dict[str, Any]]:
    direct_name = "text_atoms" if source == "TEXT" else "graphic_atoms"
    direct = artifacts.get(direct_name) or {}
    if source == "GRAPHIC" and not direct:
        direct = artifacts.get("bound_atoms") or {}
    values = direct.get("atoms") if source == "TEXT" else direct.get("graphic_atoms")
    if isinstance(values, list):
        return [copy.deepcopy(dict(item)) for item in values if isinstance(item, Mapping)]
    return _source_atoms(artifacts.get("unified_synthesis") or {}, source)


def _target_index(synthesis: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    output = {
        str(item.get("change_id") or ""): item
        for item in synthesis.get("changes") or ()
        if isinstance(item, Mapping) and item.get("change_id")
    }
    output.update({
        str(item.get("review_evidence_id") or ""): item
        for item in synthesis.get("review_items") or ()
        if isinstance(item, Mapping) and item.get("review_evidence_id")
    })
    return output


def _target_atom_ids(target: Mapping[str, Any] | None) -> set[str]:
    return {
        str(value.get("atom_id") or "")
        for value in (target or {}).get("evidence_refs") or ()
        if isinstance(value, Mapping) and value.get("atom_id")
    }


def _protected_targets(
    decisions: Mapping[str, Any] | None,
) -> set[str]:
    return {
        str(item.get("target_id") or "")
        for item in (decisions or {}).get("decisions") or ()
        if isinstance(item, Mapping)
        and item.get("decision") not in (None, "PENDING_REVIEW")
        and item.get("target_id")
    }


def _copy_direct_scope(
    ledger: dict[str, Any], baseline: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if isinstance(baseline, Mapping) and isinstance(
        baseline.get("comparison_scope"), Mapping
    ):
        ledger["comparison_scope"] = copy.deepcopy(baseline["comparison_scope"])
    return validate_ledger(ledger)


def _relation_node_ids(relation: Mapping[str, Any]) -> tuple[str, str]:
    left = str(relation.get("left_entity_ref") or "")
    right = str(relation.get("right_entity_ref") or "")
    return (
        left.split("LEFT:NODE:", 1)[-1],
        right.split("RIGHT:NODE:", 1)[-1],
    )


def _relation_changes(
    comparison: Mapping[str, Any], relation: Mapping[str, Any],
) -> list[str]:
    left_id, right_id = _relation_node_ids(relation)
    return sorted({
        str(change.get("change_id") or "")
        for change in comparison.get("changes") or ()
        if isinstance(change, Mapping)
        and left_id in set(change.get("left_nodes") or ())
        and right_id in set(change.get("right_nodes") or ())
        and change.get("change_id")
    })


def _annotate_relation_atoms(
    atoms: Sequence[Mapping[str, Any]],
    relations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for value in atoms:
        atom = copy.deepcopy(dict(value))
        provenance = dict(atom.get("provenance") or {})
        structured = provenance.get("structured")
        structured = structured if isinstance(structured, Mapping) else {}
        left_nodes = set(structured.get("left_nodes") or ())
        right_nodes = set(structured.get("right_nodes") or ())
        matched = []
        for relation in relations:
            left_id, right_id = _relation_node_ids(relation)
            if left_id in left_nodes and right_id in right_nodes:
                matched.append({
                    "resolution_id": relation.get("resolution_id"),
                    "task_id": relation.get("task_id"),
                    "relation_type": "SAME_ENTITY",
                    "source": SOURCE,
                    "evidence_refs": list(relation.get("evidence_refs") or ()),
                })
        if matched:
            provenance["ai_verified_relation"] = sorted(
                matched, key=lambda item: str(item.get("resolution_id") or "")
            )
            atom["provenance"] = provenance
        output.append(atom)
    return output


def _evidence_record(
    artifacts: Mapping[str, Mapping[str, Any]], ref: str,
) -> Mapping[str, Any] | None:
    direct = artifacts.get("direct_page_mode2") or {}
    side = "LEFT" if ref.startswith("LEFT:") else "RIGHT" if ref.startswith("RIGHT:") else ""
    if not side:
        return None
    if ":NODE:" in ref:
        node_id = ref.split(":NODE:", 1)[1]
        graph = direct.get(f"{side.lower()}_graph") or {}
        return next((
            value for value in graph.get("nodes") or ()
            if isinstance(value, Mapping) and str(value.get("id") or "") == node_id
        ), None)
    if ":ROW:" in ref:
        row_id = ref.split(":ROW:", 1)[1]
        tables = ((direct.get("diagnostics") or {}).get("electrical_load_tables") or {})
        return next((
            value for value in (tables.get(side) or {}).get("rows") or ()
            if isinstance(value, Mapping) and str(value.get("row_id") or "") == row_id
        ), None)
    return None


def _explicit_cable_count(
    artifacts: Mapping[str, Mapping[str, Any]],
    refs: Iterable[Any],
    side: str,
) -> tuple[int, str, str] | None:
    candidates: list[tuple[int, str, str]] = []
    for raw_ref in refs:
        ref = str(raw_ref or "")
        if not ref.startswith(side + ":"):
            continue
        record = _evidence_record(artifacts, ref)
        if not isinstance(record, Mapping):
            continue
        values = list(record.get("cables") or ())
        attrs = record.get("attrs")
        if isinstance(attrs, Mapping):
            values.extend(attrs.get("cables") or ())
        for raw in values:
            parsed = parse_cable(raw)
            if parsed and parsed.get("parallel_count_proven") is True:
                candidates.append((int(parsed["parallel_count"]), str(raw), ref))
    counts = {value[0] for value in candidates}
    if len(counts) != 1:
        return None
    # Prefer the shortest exact cable token over a row label containing it.
    return min(candidates, key=lambda item: (len(item[1]), item[1], item[2]))


def _cable_resolution(
    artifacts: Mapping[str, Mapping[str, Any]],
    record: Mapping[str, Any],
    target: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    resolution = record.get("resolution") or {}
    refs = [
        *list(resolution.get("evidence_refs") or ()),
        *list(resolution.get("selected_candidate_refs") or ()),
    ]
    left = _explicit_cable_count(artifacts, refs, "LEFT")
    right = _explicit_cable_count(artifacts, refs, "RIGHT")
    facet = str((target or {}).get("facet_ref") or "")
    if left is None or right is None or facet != "cable_parallel_count":
        return None
    task_id = str(record.get("task_id") or "")
    subject = str((target or {}).get("subject_ref") or task_id)
    relation = {
        "task_id": task_id,
        "facet_ref": facet,
        "before_value": left[0],
        "after_value": right[0],
        "left_raw": left[1],
        "right_raw": right[1],
        "left_ref": left[2],
        "right_ref": right[2],
    }
    if left[0] == right[0]:
        return {**relation, "status": NO_CHANGE}
    return {
        **relation,
        "status": MATERIALIZED_FINDING,
        "change": {
            "change_id": stable_id("aiv2_etchg_", task_id, facet, length=28),
            "match_id": stable_id("aiv2_etm_", task_id, length=24),
            "subject": subject,
            "section_ref": None,
            "input_number": None,
            "row_kind": "FEEDER",
            "mode_label": None,
            "mode_key": None,
            "facet_ref": facet,
            "base_facet_ref": facet,
            "facet_title": "Число параллельных кабелей",
            "unit": None,
            "before_value": left[0],
            "after_value": right[0],
            "direction": "INCREASED" if right[0] > left[0] else "DECREASED",
            "match_method": "AI_VERIFIED_IDENTITY",
            "confidence": "HIGH",
            "notes": [
                "Тождество уточнено ИИ и проверено правилами; значения "
                "детерминированно прочитаны из исходных кабельных записей."
            ],
            "evidence": {
                "LEFT": {"raw": left[1], "evidence_ref": left[2]},
                "RIGHT": {"raw": right[1], "evidence_ref": right[2]},
            },
            "source_task_id": task_id,
        },
    }


def _annotate_ai_change_atom(
    atom: Mapping[str, Any], record: Mapping[str, Any],
) -> dict[str, Any]:
    result = copy.deepcopy(dict(atom))
    provenance = dict(result.get("provenance") or {})
    provenance["ai_change_resolution"] = {
        "task_id": record.get("task_id"),
        "source": SOURCE,
        "verdict": (record.get("resolution") or {}).get("verdict"),
        "verifier": copy.deepcopy(dict(record.get("verifier") or {})),
        "evidence_refs": list(
            (record.get("resolution") or {}).get("evidence_refs") or ()
        ),
    }
    result["provenance"] = provenance
    return result


def _materialize_inconsistencies(
    source: Mapping[str, Any] | None,
    records: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], set[str]]:
    result = copy.deepcopy(dict(source or {}))
    materialized: set[str] = set()
    by_id = {
        str(record.get("task_id") or ""): record for record in records
        if (record.get("resolution") or {}).get("verdict") in {
            "DOCUMENT_ERROR", "CONFIRMED_CONTRADICTION"
        }
    }
    items = []
    for value in result.get("items") or ():
        item = copy.deepcopy(dict(value))
        item_id = str(item.get("inconsistency_id") or item.get("row_id") or "")
        record = by_id.get(item_id)
        if record is not None:
            item["verdict"] = "CONFIRMED"
            item["ai_verified_resolution"] = {
                "task_id": item_id,
                "source": SOURCE,
                "verifier": copy.deepcopy(dict(record.get("verifier") or {})),
                "evidence_refs": list(
                    (record.get("resolution") or {}).get("evidence_refs") or ()
                ),
            }
            materialized.add(item_id)
        items.append(item)
    result["items"] = items
    return result, materialized


def _outcome(
    task_id: str,
    outcome: str,
    *,
    reason: str,
    finding_ids: Iterable[str] = (),
    removed_targets: Iterable[str] = (),
) -> dict[str, Any]:
    if outcome not in OUTCOMES:
        raise ValueError(f"unknown materialization outcome: {outcome}")
    return {
        "task_id": task_id,
        "outcome": outcome,
        "reason_code": reason,
        "materialized_finding_ids": sorted({str(value) for value in finding_ids if value}),
        "removed_review_target_ids": sorted({str(value) for value in removed_targets if value}),
    }


def materialize_verified_resolutions(
    *,
    artifacts: Mapping[str, Mapping[str, Any]],
    run: Mapping[str, Any],
    pair_id: str,
    manual_audit: Mapping[str, Any] | None = None,
    human_entity_relations: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Return a complete bounded replay; source FAST artifacts stay immutable."""
    baseline_synthesis = validate_synthesis(dict(artifacts.get("unified_synthesis") or {}))
    baseline_decisions = artifacts.get("engineer_decisions") or {}
    protected = _protected_targets(baseline_decisions)
    targets = _target_index(baseline_synthesis)
    relation_artifact = build_verified_entity_relations(
        run,
        manual_audit=manual_audit,
        human_relations=human_entity_relations,
        generated_at=generated_at,
    )
    relation_by_task = {
        str(value.get("task_id") or ""): value
        for value in relation_artifact["relations"]
    }

    direct = artifacts.get("direct_page_mode2") or {}
    graph_comparison: dict[str, Any] | None = None
    graph_error = ""
    base_ledger = direct.get("graphic_change_ledger")
    if not isinstance(base_ledger, Mapping):
        base_ledger = artifacts.get("graphic_change_ledger")
    graphic_atoms: list[dict[str, Any]]
    graph_relations = [
        value for value in relation_artifact["relations"]
        if str(value.get("left_entity_ref") or "").startswith("LEFT:NODE:")
        and str(value.get("right_entity_ref") or "").startswith("RIGHT:NODE:")
    ]
    try:
        if graph_relations:
            graph_comparison = compare_system_graphs(
                dict(direct.get("left_graph") or {}),
                dict(direct.get("right_graph") or {}),
                verified_entity_relations={"relations": graph_relations},
                human_entity_relations=human_entity_relations,
            )
            materialized_ledger = adapt_system_graph_comparison_to_ledger(
                graph_comparison,
                dict(direct.get("left_graph") or {}),
                dict(direct.get("right_graph") or {}),
            )
            materialized_ledger = _copy_direct_scope(
                materialized_ledger,
                base_ledger if isinstance(base_ledger, Mapping) else None,
            )
        elif isinstance(base_ledger, Mapping):
            materialized_ledger = validate_ledger(copy.deepcopy(dict(base_ledger)))
        else:
            materialized_ledger = None
        graphic_atoms = (
            list(ledger_to_graphic_atoms(materialized_ledger).get("atoms") or ())
            if isinstance(materialized_ledger, Mapping)
            else _baseline_atoms(artifacts, "GRAPHIC")
        )
    except (TypeError, ValueError) as error:
        graph_error = str(error)
        graph_comparison = None
        materialized_ledger = (
            validate_ledger(copy.deepcopy(dict(base_ledger)))
            if isinstance(base_ledger, Mapping)
            else None
        )
        graphic_atoms = (
            list(ledger_to_graphic_atoms(materialized_ledger).get("atoms") or ())
            if isinstance(materialized_ledger, Mapping)
            else _baseline_atoms(artifacts, "GRAPHIC")
        )

    graphic_atoms = _annotate_relation_atoms(graphic_atoms, graph_relations)
    text_atoms = _baseline_atoms(artifacts, "TEXT")
    scope_ref = next(
        (
            str(atom.get("scope_ref"))
            for atom in [*graphic_atoms, *text_atoms]
            if atom.get("scope_ref")
        ),
        "",
    )
    table_changes = artifacts.get("electrical_table_changes") or {}
    if scope_ref and table_changes:
        graphic_atoms.extend(
            load_table_diff_to_graphic_atoms(
                table_changes, scope_ref=scope_ref
            ).get("atoms") or ()
        )
    eligible_records = [
        value for value in run.get("resolutions") or ()
        if isinstance(value, Mapping)
        and value.get("status") == "AI_RESOLVED_VERIFIED"
        and _verifier_passed(value)
        and _manual_supported(str(value.get("task_id") or ""), manual_audit)
    ]
    inconsistency_artifact, materialized_inconsistencies = _materialize_inconsistencies(
        artifacts.get("document_inconsistencies"), eligible_records
    )

    eligible_by_task = {
        str(value.get("task_id") or ""): value for value in eligible_records
    }
    derived_table = run.get("derived_table") or {}
    table_changes_by_task: dict[str, list[dict[str, Any]]] = {}
    table_unchanged_by_task: dict[str, list[dict[str, Any]]] = {}
    for bucket, destination in (
        ("changes", table_changes_by_task),
        ("unchanged", table_unchanged_by_task),
    ):
        for value in derived_table.get(bucket) or ():
            if not isinstance(value, Mapping):
                continue
            task_id = str(value.get("source_item_id") or "")
            if task_id in eligible_by_task:
                destination.setdefault(task_id, []).append(copy.deepcopy(dict(value)))

    suppressed_atoms: set[str] = set()
    pending_outcomes: dict[str, dict[str, Any]] = {}
    derived_cable_changes: list[dict[str, Any]] = []
    cable_record_by_change: dict[str, Mapping[str, Any]] = {}
    derived_table_records: list[dict[str, Any]] = []
    table_record_by_change: dict[str, Mapping[str, Any]] = {}
    for record in eligible_records:
        task_id = str(record.get("task_id") or "")
        resolution = record.get("resolution") or {}
        verdict = str(resolution.get("verdict") or "")
        target = targets.get(task_id)
        target_atoms = _target_atom_ids(target)
        if task_id in protected:
            pending_outcomes[task_id] = _outcome(
                task_id, HUMAN_REQUIRED,
                reason="HUMAN_DECISION_HAS_PRIORITY",
            )
            continue
        if task_id in table_changes_by_task:
            values = table_changes_by_task[task_id]
            suppressed_atoms.update(target_atoms)
            derived_table_records.extend(values)
            for value in values:
                table_record_by_change[str(value.get("change_id") or "")] = record
            pending_outcomes[task_id] = _outcome(
                task_id,
                MATERIALIZED_FINDING,
                reason="DETERMINISTIC_TABLE_DIFF_AFTER_IDENTITY",
                finding_ids=[value.get("change_id") for value in values],
                removed_targets=[task_id],
            )
            continue
        if task_id in table_unchanged_by_task:
            suppressed_atoms.update(target_atoms)
            pending_outcomes[task_id] = _outcome(
                task_id,
                NO_CHANGE,
                reason="DETERMINISTIC_TABLE_NO_CHANGE_AFTER_IDENTITY",
                removed_targets=[task_id],
            )
            continue
        if task_id in relation_by_task:
            continue
        if task_id in materialized_inconsistencies:
            pending_outcomes[task_id] = _outcome(
                task_id,
                MATERIALIZED_FINDING,
                reason="DOCUMENT_INCONSISTENCY_CONFIRMED",
                finding_ids=[task_id],
            )
            continue
        if verdict in {"FORMATTING_ONLY", "SUPPORTED_CHANGE"}:
            cable = _cable_resolution(artifacts, record, target)
            if cable is not None and (
                (verdict == "FORMATTING_ONLY" and cable["status"] == NO_CHANGE)
                or (
                    verdict == "SUPPORTED_CHANGE"
                    and cable["status"] == MATERIALIZED_FINDING
                )
            ):
                suppressed_atoms.update(target_atoms)
                if cable["status"] == NO_CHANGE:
                    pending_outcomes[task_id] = _outcome(
                        task_id,
                        NO_CHANGE,
                        reason="DETERMINISTIC_CABLE_VALUES_EQUAL",
                        removed_targets=[task_id],
                    )
                else:
                    change = dict(cable["change"])
                    derived_cable_changes.append(change)
                    cable_record_by_change[change["change_id"]] = record
                    pending_outcomes[task_id] = _outcome(
                        task_id,
                        MATERIALIZED_FINDING,
                        reason="DETERMINISTIC_CABLE_DIFF",
                        finding_ids=[change["change_id"]],
                        removed_targets=[task_id],
                    )
                continue
            if (
                verdict == "FORMATTING_ONLY"
                and cable is None
                and record.get("source_kind") == "TEXT_REVIEW"
                and target_atoms
            ):
                # A formatting verdict may remove its exact review evidence;
                # no replacement atom is created and the original immutable
                # source remains reachable through this audit artifact.
                suppressed_atoms.update(target_atoms)
                pending_outcomes[task_id] = _outcome(
                    task_id,
                    NO_CHANGE,
                    reason="VERIFIED_FORMATTING_ONLY",
                    removed_targets=[task_id],
                )
                continue
            pending_outcomes[task_id] = _outcome(
                task_id,
                HUMAN_REQUIRED,
                reason=(
                    "DETERMINISTIC_EVIDENCE_CONTRADICTS_VERDICT"
                    if cable is not None else "NO_DETERMINISTIC_TYPED_CHANGE"
                ),
            )

    if derived_cable_changes and scope_ref:
        derived_payload = {
            "contract_version": "electrical-table-diff.v1",
            "changes": derived_cable_changes,
            "blocked": [],
            "unproven": [],
        }
        derived_atoms = load_table_diff_to_graphic_atoms(
            derived_payload,
            scope_ref=scope_ref,
            artifact_ref="sha256:" + content_signature(derived_payload),
        ).get("atoms") or ()
        for atom in derived_atoms:
            source_id = str((atom.get("provenance") or {}).get("source_change_id") or "")
            graphic_atoms.append(
                _annotate_ai_change_atom(atom, cable_record_by_change[source_id])
            )

    if derived_table_records and scope_ref:
        derived_payload = {
            "contract_version": str(
                derived_table.get("contract_version")
                or "electrical-table-diff.v1"
            ),
            "changes": derived_table_records,
            "blocked": [],
            "unproven": [],
        }
        derived_atoms = load_table_diff_to_graphic_atoms(
            derived_payload,
            scope_ref=scope_ref,
            artifact_ref="sha256:" + content_signature(derived_payload),
        ).get("atoms") or ()
        for atom in derived_atoms:
            source_id = str((atom.get("provenance") or {}).get("source_change_id") or "")
            graphic_atoms.append(
                _annotate_ai_change_atom(atom, table_record_by_change[source_id])
            )

    text_atoms = [
        atom for atom in text_atoms
        if str(atom.get("atom_id") or "") not in suppressed_atoms
    ]
    graphic_atoms = [
        atom for atom in graphic_atoms
        if str(atom.get("atom_id") or "") not in suppressed_atoms
    ]
    text_atoms.sort(key=lambda item: str(item.get("atom_id") or ""))
    graphic_atoms.sort(key=lambda item: str(item.get("atom_id") or ""))

    left_entities, right_entities = entity_matcher.entity_records_from_atoms(
        text_atoms, graphic_atoms
    )
    deterministic_relations = entity_matcher.match_entities(
        left_entities, right_entities, generated_at=generated_at
    )
    bound = entity_matcher.bind_atoms_to_entity_relations(
        text_atoms,
        graphic_atoms,
        deterministic_relations,
        generated_at=generated_at,
    )
    candidates = entity_matcher.build_text_graphic_synthesis_candidates(
        bound["text_atoms"],
        bound["graphic_atoms"],
        deterministic_relations,
        source_valid=True,
        coverage_by_side={"LEFT": "CHECKED", "RIGHT": "CHECKED"},
        document_binding_state="DOCUMENT_BINDING_PROVEN",
        generated_at=generated_at,
    )
    synthesis = validate_synthesis(synthesize_unified_changes(
        text_atoms=bound["text_atoms"],
        graphic_atoms=bound["graphic_atoms"],
        candidates=candidates.get("candidates") or (),
        source_states={
            "TEXT": "VALID" if text_atoms else "ABSENT",
            "GRAPHIC": "VALID" if graphic_atoms else "ABSENT",
        },
    ))
    decisions = build_engineer_decisions(
        synthesis,
        existing=baseline_decisions,
        generated_at=generated_at,
    )
    resolved_table_rows = sorted({
        str(value.get(key) or "")
        for bucket in (table_changes_by_task, table_unchanged_by_task)
        for values in bucket.values()
        for value in values
        for key in ("left_row_id", "right_row_id")
        if value.get(key)
    })
    report = build_preliminary_report(
        pair_id=pair_id,
        synthesis=synthesis,
        document_inconsistencies=inconsistency_artifact,
        electrical_table_changes=table_changes,
        # Materialized table changes already live in the ordinary synthesis.
        # Only residual hints remain in the report's table side channel.
        ai_table_identity={
            "derived_changes": [],
            "derived_blocked": list(derived_table.get("blocked") or ()),
            "resolved_row_ids": resolved_table_rows,
        },
        generated_at=generated_at,
    )

    after_targets = set(_target_index(synthesis))
    before_targets = set(targets)
    removed_targets = before_targets - after_targets
    added_targets = after_targets - before_targets
    finding_ids_by_atom = {
        str(ref.get("atom_id") or ""): str(finding.get("change_id") or "")
        for finding in synthesis.get("changes") or ()
        if isinstance(finding, Mapping)
        for ref in finding.get("evidence_refs") or ()
        if isinstance(ref, Mapping) and ref.get("atom_id")
    }

    outcomes: list[dict[str, Any]] = []
    for record in run.get("resolutions") or ():
        if not isinstance(record, Mapping):
            continue
        task_id = str(record.get("task_id") or "")
        if task_id in pending_outcomes:
            value = dict(pending_outcomes[task_id])
            value["removed_review_target_ids"] = sorted(
                set(value["removed_review_target_ids"]) & removed_targets
            )
            outcomes.append(value)
            continue
        relation = relation_by_task.get(task_id)
        if relation is not None:
            if graph_comparison is None:
                outcomes.append(_outcome(
                    task_id,
                    HUMAN_REQUIRED,
                    reason="GRAPH_RECOMPUTE_FAILED",
                ))
                continue
            source_changes = _relation_changes(graph_comparison, relation)
            atom_ids = {f"graphic:{value}" for value in source_changes}
            finding_ids = sorted({
                finding_ids_by_atom[value]
                for value in atom_ids if value in finding_ids_by_atom
            })
            outcomes.append(_outcome(
                task_id,
                MATERIALIZED_FINDING if finding_ids else NO_CHANGE,
                reason=(
                    "DETERMINISTIC_DIFF_AFTER_IDENTITY"
                    if finding_ids else "DETERMINISTIC_NO_CHANGE_AFTER_IDENTITY"
                ),
                finding_ids=finding_ids,
            ))
            continue
        if record.get("reason_code") == "VERIFIER_REJECTED":
            outcomes.append(_outcome(
                task_id,
                REJECTED_VERIFIER,
                reason="VERIFIER_REJECTED",
            ))
            continue
        if (
            record.get("status") == "AI_RESOLVED_VERIFIED"
            and not _manual_supported(task_id, manual_audit)
        ):
            outcomes.append(_outcome(
                task_id,
                HUMAN_REQUIRED,
                reason="MANUAL_AUDIT_NOT_SUPPORTED",
            ))
            continue
        outcomes.append(_outcome(
            task_id,
            HUMAN_REQUIRED,
            reason=str(record.get("reason_code") or "NOT_MATERIALIZED"),
        ))
    outcomes.sort(key=lambda item: item["task_id"])
    expected_ids = {
        str(value.get("task_id") or "")
        for value in run.get("resolutions") or () if isinstance(value, Mapping)
    }
    if len(outcomes) != len(expected_ids) or {
        value["task_id"] for value in outcomes
    } != expected_ids:
        raise AssertionError("materialization accounting is not exactly one-to-one")

    before_rows = len((baseline_decisions or {}).get("decisions") or ())
    after_rows = len(decisions.get("decisions") or ())
    outcome_counts = Counter(value["outcome"] for value in outcomes)
    materialized_finding_ids = sorted({
        finding_id
        for value in outcomes
        if value["outcome"] == MATERIALIZED_FINDING
        for finding_id in value["materialized_finding_ids"]
    })
    unsupported_materialized = sum(
        _audit_index(manual_audit).get(value["task_id"]) == "UNSUPPORTED"
        and value["outcome"] in {MATERIALIZED_FINDING, NO_CHANGE}
        for value in outcomes
    )
    artifact = {
        "kind": MATERIALIZATION_KIND,
        "schema_version": MATERIALIZATION_SCHEMA_VERSION,
        "materializer_version": MATERIALIZER_VERSION,
        "version": 1,
        "generated_at": generated_at or utc_now(),
        "pair_id": pair_id,
        "source": SOURCE,
        "verified_entity_relations": relation_artifact,
        "outcomes": outcomes,
        "materialized_graph_comparison": graph_comparison,
        "materialized_graphic_ledger": materialized_ledger,
        "effective_entity_relations": deterministic_relations,
        "bound_atoms": bound,
        "synthesis_candidates": candidates,
        "unified_synthesis": synthesis,
        "engineer_decisions": decisions,
        "document_inconsistencies": inconsistency_artifact,
        "preliminary_report": report,
        "diagnostics": {
            "outcome_counts": {
                outcome: outcome_counts.get(outcome, 0) for outcome in OUTCOMES
            },
            "verified_relations": len(relation_artifact["relations"]),
            "materialized_tasks": outcome_counts[MATERIALIZED_FINDING],
            "materialized_findings": len(materialized_finding_ids),
            "materialized_finding_ids": materialized_finding_ids,
            "no_change_after_identity": sum(
                value["outcome"] == NO_CHANGE
                and value["reason_code"] == "DETERMINISTIC_NO_CHANGE_AFTER_IDENTITY"
                for value in outcomes
            ),
            "removed_review_targets": len(removed_targets),
            "removed_review_target_ids": sorted(removed_targets),
            "added_review_targets": len(added_targets),
            "added_review_target_ids": sorted(added_targets),
            "stage7_before": before_rows,
            "stage7_after": after_rows,
            "human_decisions_saved": max(0, before_rows - after_rows),
            "preliminary_review_before": int(
                (((artifacts.get("preliminary_report") or {}).get("summary") or {}).get("counts") or {}).get("review")
                or 0
            ),
            "preliminary_review_after": int(
                ((report.get("summary") or {}).get("counts") or {}).get("review")
                or 0
            ),
            "unsupported_materialized": unsupported_materialized,
            "graph_recompute_error": graph_error or None,
            "model_calls": 0,
            "fast_artifacts_mutated": False,
        },
        "constraints": {
            "human_priority": True,
            "ai_creates_parameters": False,
            "engineer_approved": False,
            "partial_materialized": False,
            "unsupported_materialized": False,
            "cache_replay_supported": True,
            "fast_unchanged": True,
        },
    }
    artifact["input_signature"] = content_signature({
        "schema": MATERIALIZATION_SCHEMA_VERSION,
        "source_run": run.get("input_signature"),
        "verified_relations": relation_artifact.get("input_signature"),
        "baseline_synthesis": content_signature(baseline_synthesis),
        "manual_audit": _audit_index(manual_audit),
    })
    return artifact


__all__ = [
    "HUMAN_REQUIRED",
    "MATERIALIZATION_KIND",
    "MATERIALIZATION_SCHEMA_VERSION",
    "MATERIALIZED_FINDING",
    "NO_CHANGE",
    "OUTCOMES",
    "REJECTED_VERIFIER",
    "RELATION_KIND",
    "RELATION_SCHEMA_VERSION",
    "SOURCE",
    "build_verified_entity_relations",
    "materialize_verified_resolutions",
]
