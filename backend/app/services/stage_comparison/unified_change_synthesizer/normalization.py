"""Lossless adapters from existing TEXT and GRAPHIC production artifacts.

These adapters map closed upstream enums and explicit references only.  They do
not parse summaries, compare graphs, extract entities, or perform matching.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping

from ..graphic_comparison.contract import validate_ledger
from ..high_level_project_changes import KIND as TEXT_KIND
from ..high_level_project_changes import SCHEMA_VERSION as TEXT_SCHEMA_VERSION
from .contract import SynthesisValidationError, normalize_synthesis_atom
from .identity import digest


_MODE2_FACTS = {
    "SYSTEM_BACKBONE_CHANGED": ("STRUCTURE", "ALTERED", "MATERIAL_CHANGE"),
    "FUNCTIONAL_GROUP_CHANGED": ("STRUCTURE", "ALTERED", "MATERIAL_CHANGE"),
    "NODE_ADDED": ("STRUCTURE", "ADDED", "MATERIAL_CHANGE"),
    "NODE_REMOVED": ("STRUCTURE", "REMOVED", "MATERIAL_CHANGE"),
    "NODE_TYPE_CHANGED": ("TYPE", "REPLACED", "MATERIAL_CHANGE"),
    "CONNECTION_CHANGED": ("CONNECTION", "ALTERED", "MATERIAL_CHANGE"),
    "GROUP_COUNT_CHANGED": ("QUANTITY", "ALTERED", "MATERIAL_CHANGE"),
    "DETAIL_LEVEL_INCREASED": ("STRUCTURE", "ALTERED", "DETAIL_ONLY"),
    "UNCERTAIN_STRUCTURAL_CHANGE": (
        "STRUCTURE",
        "ALTERED",
        "REVIEW_REQUIRED",
    ),
}
_MODE1_FACTS = {
    "ADDED_GRAPHIC": ("STRUCTURE", "ADDED", "MATERIAL_CHANGE"),
    "REMOVED_GRAPHIC": ("STRUCTURE", "REMOVED", "MATERIAL_CHANGE"),
    "GEOMETRY_CHANGED": ("STRUCTURE", "ALTERED", "MATERIAL_CHANGE"),
    "UNCERTAIN_GRAPHIC_CHANGE": (
        "STRUCTURE",
        "ALTERED",
        "REVIEW_REQUIRED",
    ),
}
_TEXT_BUCKETS = (
    "high_level_changes",
    "detail_level_increased",
    "material_review",
    "non_material_review",
    "unresolved",
)


def _artifact_ref(prefix: str, payload: Mapping[str, Any], explicit: str | None) -> str:
    if explicit is not None:
        if not isinstance(explicit, str) or not explicit.strip():
            raise SynthesisValidationError("artifact_ref: non-empty string required")
        return explicit.strip()
    return prefix + digest(dict(payload))


def _entity_index(text_entities: Any) -> dict[str, list[str]]:
    if text_entities is None:
        return {}
    if not isinstance(text_entities, Mapping):
        raise SynthesisValidationError("text_entities: object required")
    if (
        text_entities.get("schema_version") != "text-entities.v1"
        or text_entities.get("kind") != "stage_comparison_text_entities"
        or not isinstance(text_entities.get("entities"), list)
    ):
        raise SynthesisValidationError("text_entities: unsupported contract")
    index: dict[str, list[str]] = defaultdict(list)
    for entity in text_entities["entities"]:
        if not isinstance(entity, Mapping):
            raise SynthesisValidationError("text_entities.entities: object required")
        entity_id = entity.get("entity_id")
        evidence_ids = entity.get("evidence_ids")
        if (
            not isinstance(entity_id, str)
            or not entity_id
            or not isinstance(evidence_ids, list)
        ):
            raise SynthesisValidationError("text_entities.entities: invalid references")
        for evidence_id in evidence_ids:
            if not isinstance(evidence_id, str) or not evidence_id:
                raise SynthesisValidationError(
                    "text_entities.entities.evidence_ids: invalid"
                )
            index[evidence_id].append(entity_id)
    return {key: sorted(set(value)) for key, value in index.items()}


def _stage53_details(stage53: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    details: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for bucket in _TEXT_BUCKETS:
        values = stage53.get(bucket) or []
        if not isinstance(values, list):
            raise SynthesisValidationError(f"stage53.{bucket}: array required")
        for change in values:
            if not isinstance(change, Mapping):
                raise SynthesisValidationError(f"stage53.{bucket}: object required")
            source_change_id = change.get("change_id")
            for detail in change.get("details") or []:
                if not isinstance(detail, Mapping):
                    raise SynthesisValidationError(
                        f"stage53.{bucket}.details: object required"
                    )
                evidence_id = detail.get("evidence_id")
                if not isinstance(evidence_id, str) or not evidence_id:
                    raise SynthesisValidationError(
                        f"stage53.{bucket}.details.evidence_id: invalid"
                    )
                if evidence_id in details:
                    raise SynthesisValidationError(
                        "stage53: duplicate evidence_id across result buckets"
                    )
                details[evidence_id] = (str(source_change_id or ""), detail)
    return [(key, *details[key]) for key in sorted(details)]


def stage53_to_text_atoms(
    stage53: Any,
    *,
    structured_facts: Mapping[str, Mapping[str, Any]],
    text_entities: Any = None,
    artifact_ref: str | None = None,
) -> dict[str, Any]:
    """Adapt Stage 5.3 evidence using explicit per-evidence policy facts.

    ``structured_facts`` is intentionally mandatory.  Missing dimensions or
    project entities are reported and never reconstructed from Stage 5.3 text.
    """
    if not isinstance(stage53, Mapping):
        raise SynthesisValidationError("stage53: object required")
    if (
        stage53.get("kind") != TEXT_KIND
        or stage53.get("schema_version") != TEXT_SCHEMA_VERSION
    ):
        raise SynthesisValidationError("stage53: unsupported contract")
    if not isinstance(structured_facts, Mapping):
        raise SynthesisValidationError("structured_facts: object required")

    entities_by_evidence = _entity_index(text_entities)
    source_ref = _artifact_ref("sha256:", stage53, artifact_ref)
    source_artifact = {
        "kind": TEXT_KIND,
        "schema_version": TEXT_SCHEMA_VERSION,
        "artifact_ref": source_ref,
    }
    atoms: list[dict[str, Any]] = []
    missing_structured_facts: list[str] = []
    document_only: list[str] = []
    ambiguous_entities: list[str] = []
    for evidence_id, source_change_id, detail in _stage53_details(stage53):
        fact = structured_facts.get(evidence_id)
        if not isinstance(fact, Mapping):
            missing_structured_facts.append(evidence_id)
            continue
        allowed = {
            "scope_ref",
            "subject_ref",
            "project_entity_ref",
            "dimension",
            "direction",
            "outcome",
            "confidence",
            "facet_ref",
            "before_value",
            "after_value",
            "review_status",
            "provenance",
        }
        if not set(fact) <= allowed:
            raise SynthesisValidationError(
                f"structured_facts.{evidence_id}: invalid fields"
            )
        entity_ids = entities_by_evidence.get(evidence_id, [])
        explicit_project = fact.get("project_entity_ref")
        if explicit_project is not None:
            project_entity_ref = explicit_project
        elif len(entity_ids) == 1:
            project_entity_ref = entity_ids[0]
        else:
            project_entity_ref = None
            (ambiguous_entities if entity_ids else document_only).append(evidence_id)
        # Keep the explicit document subject when one exists, but do not
        # invent a project subject when engineering identity is unresolved.
        # The synthesizer preserves either case as review evidence.
        subject_ref = fact.get("subject_ref") or project_entity_ref
        before_value = (
            fact["before_value"]
            if "before_value" in fact
            else detail.get("before")
        )
        after_value = (
            fact["after_value"]
            if "after_value" in fact
            else detail.get("after")
        )
        atom = {
            "atom_id": f"text:{evidence_id}",
            "source": "TEXT",
            "scope_ref": fact.get("scope_ref"),
            "subject_ref": subject_ref,
            "project_entity_ref": project_entity_ref,
            "dimension": fact.get("dimension"),
            "direction": fact.get("direction"),
            "outcome": fact.get("outcome"),
            "confidence": fact.get("confidence"),
            "evidence_ref": evidence_id,
            "source_artifact": source_artifact,
            "provenance": {
                "producer": "stage53-structured-evidence-adapter-v1",
                "source_change_id": source_change_id,
                "source_evidence_id": evidence_id,
                "entity_refs": entity_ids,
                **dict(fact.get("provenance") or {}),
            },
            "facet_ref": fact.get("facet_ref"),
            "before_value": before_value,
            "after_value": after_value,
        }
        if "review_status" in fact:
            atom["review_status"] = fact["review_status"]
        atoms.append(normalize_synthesis_atom(atom))
    return {
        "atoms": sorted(atoms, key=lambda item: item["atom_id"]),
        "diagnostics": {
            "source_evidence": len(_stage53_details(stage53)),
            "adapted": len(atoms),
            "missing_structured_facts": sorted(missing_structured_facts),
            "document_only": sorted(document_only),
            "ambiguous_entities": sorted(ambiguous_entities),
        },
    }


def _scope_from_ledger(ledger: Mapping[str, Any]) -> str:
    scope = ledger["comparison_scope"]
    blocks = {
        side: [
            {
                "block_id": block.get("block_id"),
                "page_index": block.get("page_index"),
            }
            for block in scope[f"{side.lower()}_blocks"]
        ]
        for side in ("LEFT", "RIGHT")
    }
    # Direct PAGE identity is based only on the pages the user selected.  No
    # parent-document relation participates in the comparison entitlement.
    return "direct_page_scope_" + digest(blocks)[:20]


def _graphic_values(change: Mapping[str, Any]) -> tuple[Any, Any]:
    structural = change.get("structural")
    if not isinstance(structural, Mapping):
        return None, None
    relation = structural.get("relation")
    relation = relation if isinstance(relation, Mapping) else {}
    change_type = change.get("type")
    if change_type in {"GROUP_COUNT_CHANGED", "UNCERTAIN_STRUCTURAL_CHANGE"}:
        return relation.get("left_count"), relation.get("right_count")
    if change_type == "NODE_TYPE_CHANGED":
        tokens_by_side: dict[str, list[str]] = {"LEFT": [], "RIGHT": []}
        for evidence in change.get("evidence") or []:
            if not isinstance(evidence, Mapping):
                continue
            source = evidence.get("source_graph")
            side = source.get("side") if isinstance(source, Mapping) else None
            tokens = evidence.get("source_tokens")
            if side in tokens_by_side and isinstance(tokens, list):
                tokens_by_side[side].extend(
                    token for token in tokens if isinstance(token, str) and token
                )
        left_tokens = sorted(set(tokens_by_side["LEFT"]))
        right_tokens = sorted(set(tokens_by_side["RIGHT"]))
        return (
            left_tokens[0]
            if len(left_tokens) == 1
            else relation.get("left_effective_type"),
            right_tokens[0]
            if len(right_tokens) == 1
            else relation.get("right_effective_type"),
        )
    if change_type == "NODE_ADDED":
        return None, structural.get("subject")
    if change_type == "NODE_REMOVED":
        return structural.get("subject"), None
    return None, None


def _quantity_direction(
    direction: str,
    before_value: Any,
    after_value: Any,
) -> str:
    if (
        direction == "ALTERED"
        and isinstance(before_value, int)
        and not isinstance(before_value, bool)
        and isinstance(after_value, int)
        and not isinstance(after_value, bool)
    ):
        return "INCREASED" if after_value > before_value else "DECREASED"
    return direction


def _graphic_subject(change: Mapping[str, Any]) -> str:
    structural = change.get("structural")
    if isinstance(structural, Mapping):
        identity = {
            "subject": structural.get("subject"),
            "left_nodes": structural.get("left_nodes"),
            "right_nodes": structural.get("right_nodes"),
            "left_edges": structural.get("left_edges"),
            "right_edges": structural.get("right_edges"),
        }
    else:
        identity = {"source_change_id": change.get("change_id")}
    return "graphic_subject_" + digest(identity)[:20]


def ledger_to_graphic_atoms(
    ledger: Any,
    *,
    scope_ref: str | None = None,
    change_entities: Mapping[str, Mapping[str, Any]] | None = None,
    artifact_ref: str | None = None,
) -> dict[str, Any]:
    """Adapt a validated GraphicChangeLedger without rerunning comparison."""
    try:
        validated = validate_ledger(ledger)
    except (TypeError, ValueError) as error:
        raise SynthesisValidationError(f"graphic ledger: {error}") from error
    resolved_scope = scope_ref or _scope_from_ledger(validated)
    if not isinstance(resolved_scope, str) or not resolved_scope.strip():
        raise SynthesisValidationError("scope_ref: non-empty string required")
    if change_entities is not None and not isinstance(change_entities, Mapping):
        raise SynthesisValidationError("change_entities: object required")
    source_artifact = {
        "kind": "graphic_change_ledger",
        "schema_version": validated["schema_version"],
        "artifact_ref": _artifact_ref("sha256:", validated, artifact_ref),
    }

    facts = _MODE2_FACTS if validated["schema_version"].endswith("v2") else _MODE1_FACTS
    atoms: list[dict[str, Any]] = []
    derived_subjects: list[str] = []
    for change in sorted(validated["changes"], key=lambda item: item["change_id"]):
        change_id = change["change_id"]
        dimension, direction, outcome = facts[change["type"]]
        before_value, after_value = _graphic_values(change)
        direction = _quantity_direction(direction, before_value, after_value)
        entity = (change_entities or {}).get(change_id) or {}
        if not isinstance(entity, Mapping) or not set(entity) <= {
            "subject_ref",
            "project_entity_ref",
            "facet_ref",
            "provenance",
        }:
            raise SynthesisValidationError(
                f"change_entities.{change_id}: invalid fields"
            )
        subject_ref = entity.get("subject_ref") or _graphic_subject(change)
        if "subject_ref" not in entity:
            derived_subjects.append(change_id)
        atom = {
            "atom_id": f"graphic:{change_id}",
            "source": "GRAPHIC",
            "scope_ref": resolved_scope,
            "subject_ref": subject_ref,
            "project_entity_ref": entity.get("project_entity_ref"),
            "dimension": dimension,
            "direction": direction,
            "outcome": outcome,
            "confidence": change["confidence"],
            "evidence_ref": change_id,
            "source_artifact": source_artifact,
            "provenance": {
                "producer": "graphic-change-ledger-adapter-v1",
                "source_change_id": change_id,
                "route": validated.get("route"),
                "mode": validated.get("mode"),
                "source_type": change["type"],
                "structured": change.get("structural"),
                "entity": dict(entity.get("provenance") or {}),
            },
            "facet_ref": entity.get("facet_ref"),
            "before_value": before_value,
            "after_value": after_value,
        }
        atoms.append(normalize_synthesis_atom(atom))
    return {
        "atoms": atoms,
        "diagnostics": {
            "source_changes": len(validated["changes"]),
            "adapted": len(atoms),
            "scope_source": "EXPLICIT" if scope_ref is not None else "DIRECT_PAGE",
            "parent_relation_required": False,
            "derived_structured_subjects": sorted(derived_subjects),
        },
    }


def normalize_atoms(values: Iterable[Any], expected_source: str) -> list[dict[str, Any]]:
    if expected_source not in {"TEXT", "GRAPHIC"}:
        raise SynthesisValidationError("expected_source: TEXT or GRAPHIC required")
    atoms = [normalize_synthesis_atom(value) for value in values]
    if any(atom["source"] != expected_source for atom in atoms):
        raise SynthesisValidationError("atom source does not match input channel")
    ids = [atom["atom_id"] for atom in atoms]
    if len(ids) != len(set(ids)):
        raise SynthesisValidationError("atoms: duplicate atom_id")
    return sorted(atoms, key=lambda item: item["atom_id"])


__all__ = [
    "ledger_to_graphic_atoms",
    "normalize_atoms",
    "stage53_to_text_atoms",
]
