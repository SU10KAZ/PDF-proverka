"""Explicit LEFT/RIGHT wrappers for graph entities and entity links.

The wrappers keep the G2.4.3 entity artifacts immutable.  Side is carried by
the named ``LEFT``/``RIGHT`` branch, never by array position, filenames, or an
inference from block metadata.  Each side is bridged independently so
cardinality conflicts cannot leak across comparison stages.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from .entity_bridge import (
    BRIDGE_VERSION,
    build_entity_links_from_artifacts,
    entity_links_are_stale,
    validate_entity_links_artifact,
)
from .graph_entity_adapter import (
    build_graph_entities,
    is_stale as graph_entities_are_stale,
    validate_graph_entities,
)
from .page_identity import text_pdf_page_1based_to_canonical_index
from .text_entity_producer import (
    is_stale as text_entities_are_stale,
    validate_text_entities,
)


SIDES = ("LEFT", "RIGHT")
GRAPH_SCHEMA_VERSION = "side-graph-entities.v2"
GRAPH_KIND = "side_aware_system_graph_entities"
SIDE_GRAPH_ADAPTER_VERSION = "side-graph-entity-wrapper-v2"
LINK_SCHEMA_VERSION = "side-entity-links.v1"
LINK_KIND = "side_aware_text_graphic_entity_links"
SIDE_BRIDGE_VERSION = "side-partitioned-entity-bridge-v1"

PRESENCE_STATES = frozenset({"PRESENT", "ABSENT", "UNKNOWN"})
MATCH_STATES = frozenset({"HIGH", "MEDIUM", "UNKNOWN", "NOT_MATCHED"})


class SideEntityValidationError(ValueError):
    """A side-aware input or artifact is incomplete or inconsistent."""


def _digest(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _side_mapping(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(SIDES):
        raise SideEntityValidationError(f"{where}: explicit LEFT/RIGHT mapping required")
    return value


def _graph_source_summary(side_artifacts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        side: {
            "schema_version": side_artifacts[side]["schema_version"],
            "source_signature": side_artifacts[side]["source_signature"],
            "source_graphs": side_artifacts[side]["source_graphs"],
        }
        for side in SIDES
    }


def _side_graph_signature(side_artifacts: dict[str, dict[str, Any]]) -> str:
    return _digest(
        {
            "adapter_version": SIDE_GRAPH_ADAPTER_VERSION,
            "sides": _graph_source_summary(side_artifacts),
        }
    )


def build_side_graph_entities(
    *, left_graphs: Iterable[Any], right_graphs: Iterable[Any]
) -> dict[str, Any]:
    """Build independent G2.4.3 GRAPH_ENTITIES artifacts for explicit sides."""
    artifacts = {
        "LEFT": build_graph_entities(list(left_graphs)),
        "RIGHT": build_graph_entities(list(right_graphs)),
    }
    payload = {
        "schema_version": GRAPH_SCHEMA_VERSION,
        "kind": GRAPH_KIND,
        "adapter_version": SIDE_GRAPH_ADAPTER_VERSION,
        "source_signature": _side_graph_signature(artifacts),
        "sides": artifacts,
        "diagnostics": {
            side: {
                "source_graphs": artifacts[side]["quality_report"]["total_source_graphs"],
                "entities": artifacts[side]["quality_report"]["produced_entities"],
                "invalid_source_graphs": artifacts[side]["quality_report"][
                    "invalid_source_graphs"
                ],
            }
            for side in SIDES
        },
    }
    return validate_side_graph_entities(payload)


def validate_side_graph_entities(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "kind",
        "adapter_version",
        "source_signature",
        "sides",
        "diagnostics",
    }:
        raise SideEntityValidationError("side GRAPH_ENTITIES: invalid envelope")
    if (
        payload["schema_version"] != GRAPH_SCHEMA_VERSION
        or payload["kind"] != GRAPH_KIND
        or payload["adapter_version"] != SIDE_GRAPH_ADAPTER_VERSION
    ):
        raise SideEntityValidationError("side GRAPH_ENTITIES: unsupported contract")
    sides = _side_mapping(payload["sides"], "side GRAPH_ENTITIES.sides")
    for side in SIDES:
        validate_graph_entities(sides[side])
    if payload["source_signature"] != _side_graph_signature(sides):
        raise SideEntityValidationError("side GRAPH_ENTITIES.source_signature: invalid")
    expected_diagnostics = {
        side: {
            "source_graphs": sides[side]["quality_report"]["total_source_graphs"],
            "entities": sides[side]["quality_report"]["produced_entities"],
            "invalid_source_graphs": sides[side]["quality_report"][
                "invalid_source_graphs"
            ],
        }
        for side in SIDES
    }
    if payload["diagnostics"] != expected_diagnostics:
        raise SideEntityValidationError("side GRAPH_ENTITIES.diagnostics: invalid")
    return payload


def side_graph_entities_are_stale(
    artifact: Any, *, left_graphs: Iterable[Any], right_graphs: Iterable[Any]
) -> bool:
    if not isinstance(artifact, dict):
        return True
    try:
        validated = validate_side_graph_entities(artifact)
    except (SideEntityValidationError, TypeError, ValueError):
        return True
    current = {"LEFT": list(left_graphs), "RIGHT": list(right_graphs)}
    return any(
        graph_entities_are_stale(validated["sides"][side], current[side])
        for side in SIDES
    )


def _side_links_signature(
    text_entities: dict[str, Any], side_graph_entities: dict[str, Any]
) -> str:
    return _digest(
        {
            "side_bridge_version": SIDE_BRIDGE_VERSION,
            "entity_bridge_version": BRIDGE_VERSION,
            "text_entities_signature": text_entities["source_signature"],
            "side_graph_entities_signature": side_graph_entities["source_signature"],
        }
    )


def build_side_entity_links(
    text_entities: Any,
    side_graph_entities: Any,
    *,
    current_stage53_artifact: Any = None,
    current_text_evidence_index: Any = None,
    current_system_graphs: dict[str, Iterable[Any]] | None = None,
) -> dict[str, Any]:
    """Bridge TEXT against LEFT and RIGHT candidates as two separate problems."""
    text = validate_text_entities(text_entities)
    graphics = validate_side_graph_entities(side_graph_entities)
    if current_stage53_artifact is not None and text_entities_are_stale(
        text, current_stage53_artifact, current_text_evidence_index
    ):
        raise SideEntityValidationError("TEXT_ENTITIES stale for current Stage 5.3")
    if current_system_graphs is not None:
        graph_sources = _side_mapping(current_system_graphs, "current_system_graphs")
        if side_graph_entities_are_stale(
            graphics,
            left_graphs=list(graph_sources["LEFT"]),
            right_graphs=list(graph_sources["RIGHT"]),
        ):
            raise SideEntityValidationError(
                "side GRAPH_ENTITIES stale for current SYSTEM_GRAPH inputs"
            )

    side_links = {
        side: build_entity_links_from_artifacts(text, graphics["sides"][side])
        for side in SIDES
    }
    payload = {
        "schema_version": LINK_SCHEMA_VERSION,
        "kind": LINK_KIND,
        "side_bridge_version": SIDE_BRIDGE_VERSION,
        "entity_bridge_version": BRIDGE_VERSION,
        "source_signature": _side_links_signature(text, graphics),
        "source_signatures": {
            "text_entities": text["source_signature"],
            "side_graph_entities": graphics["source_signature"],
        },
        "sides": side_links,
        "diagnostics": {
            side: side_links[side]["diagnostics"] for side in SIDES
        },
    }
    return validate_side_entity_links(payload)


def validate_side_entity_links(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "kind",
        "side_bridge_version",
        "entity_bridge_version",
        "source_signature",
        "source_signatures",
        "sides",
        "diagnostics",
    }:
        raise SideEntityValidationError("side entity links: invalid envelope")
    if (
        payload["schema_version"] != LINK_SCHEMA_VERSION
        or payload["kind"] != LINK_KIND
        or payload["side_bridge_version"] != SIDE_BRIDGE_VERSION
        or payload["entity_bridge_version"] != BRIDGE_VERSION
    ):
        raise SideEntityValidationError("side entity links: unsupported contract")
    sides = _side_mapping(payload["sides"], "side entity links.sides")
    for side in SIDES:
        validate_entity_links_artifact(sides[side])
    signatures = payload.get("source_signatures")
    if not isinstance(signatures, dict) or set(signatures) != {
        "text_entities",
        "side_graph_entities",
    }:
        raise SideEntityValidationError("side entity links.source_signatures: invalid")
    expected_signature = _digest(
        {
            "side_bridge_version": SIDE_BRIDGE_VERSION,
            "entity_bridge_version": BRIDGE_VERSION,
            "text_entities_signature": signatures["text_entities"],
            "side_graph_entities_signature": signatures["side_graph_entities"],
        }
    )
    if payload["source_signature"] != expected_signature:
        raise SideEntityValidationError("side entity links.source_signature: invalid")
    if payload["diagnostics"] != {
        side: sides[side]["diagnostics"] for side in SIDES
    }:
        raise SideEntityValidationError("side entity links.diagnostics: invalid")
    return payload


def side_entity_links_are_stale(
    links: Any, text_entities: Any, side_graph_entities: Any
) -> bool:
    if not all(isinstance(item, dict) for item in (links, text_entities, side_graph_entities)):
        return True
    try:
        validated = validate_side_entity_links(links)
        text = validate_text_entities(text_entities)
        graphics = validate_side_graph_entities(side_graph_entities)
    except (SideEntityValidationError, TypeError, ValueError):
        return True
    if validated["source_signature"] != _side_links_signature(text, graphics):
        return True
    return any(
        entity_links_are_stale(
            validated["sides"][side], text, graphics["sides"][side]
        )
        for side in SIDES
    )


def _stage_details(stage53: dict[str, Any]) -> dict[str, dict[str, Any]]:
    buckets = (
        "high_level_changes",
        "detail_level_increased",
        "material_review",
        "non_material_review",
        "unresolved",
    )
    found: dict[str, dict[str, Any]] = {}
    changes = [
        change
        for bucket in buckets
        for change in stage53.get(bucket) or []
        if isinstance(change, dict)
    ]
    changes.extend(
        change
        for change in (stage53.get("service_structure_summary") or {}).get("items") or []
        if isinstance(change, dict)
    )
    for change in changes:
        for detail in change.get("details") or []:
            if not isinstance(detail, dict):
                continue
            evidence_id = str(detail.get("evidence_id") or "").strip()
            if evidence_id and evidence_id not in found:
                found[evidence_id] = detail
    return found


def _presence_for_side(
    entity: dict[str, Any], details: dict[str, dict[str, Any]], side: str
) -> dict[str, Any]:
    key = side.lower()
    fragments_key = f"{key}_fragment_ids"
    pages_key = f"{key}_pages"
    text_key = "before" if side == "LEFT" else "after"
    present_evidence: list[str] = []
    absent_evidence: list[str] = []
    unknown_evidence: list[str] = []
    fragment_ids: set[str] = set()
    pdf_pages: set[int] = set()
    entity_fragments = set(entity["fragment_ids"])
    for evidence_id in entity["evidence_ids"]:
        detail = details.get(evidence_id)
        if detail is None:
            unknown_evidence.append(evidence_id)
            continue
        side_fragments = {
            str(item)
            for item in detail.get(fragments_key) or []
            if str(item or "").strip()
        }
        fragment_ids.update(side_fragments & entity_fragments)
        pdf_pages.update(
            int(page)
            for page in detail.get(pages_key) or []
            if isinstance(page, int) and not isinstance(page, bool) and page >= 1
        )
        has_text = isinstance(detail.get(text_key), str) and bool(detail[text_key].strip())
        if side_fragments or has_text:
            present_evidence.append(evidence_id)
            continue
        source_status = str(detail.get("source_status") or "").upper()
        explicitly_absent = (side == "LEFT" and source_status == "ADDED") or (
            side == "RIGHT" and source_status == "REMOVED"
        )
        if explicitly_absent:
            absent_evidence.append(evidence_id)
        else:
            unknown_evidence.append(evidence_id)
    if present_evidence:
        state = "PRESENT"
        reasons = ["side_fragment_or_text_present"]
    elif absent_evidence and not unknown_evidence:
        state = "ABSENT"
        reasons = ["stage53_source_status_proves_side_absence"]
    else:
        state = "UNKNOWN"
        reasons = ["stage53_evidence_does_not_prove_side_presence"]
    return {
        "presence": state,
        "reason_codes": reasons,
        "evidence_ids": sorted(set(present_evidence + absent_evidence + unknown_evidence)),
        "fragment_ids": sorted(fragment_ids),
        "pdf_pages_1based": sorted(pdf_pages),
        "canonical_page_indexes_0based": sorted(
            text_pdf_page_1based_to_canonical_index(page) for page in pdf_pages
        ),
    }


def query_text_entity_side(
    stage53: Any,
    text_entities: Any,
    side_entity_links: Any,
    text_entity_id: str,
    side: str,
    *,
    current_text_evidence_index: Any = None,
) -> dict[str, Any]:
    """Return evidence-backed TEXT presence and matching for one explicit side."""
    if side not in SIDES:
        raise SideEntityValidationError("side: LEFT or RIGHT required")
    if not isinstance(stage53, dict):
        raise SideEntityValidationError("Stage 5.3 artifact: object required")
    text = validate_text_entities(text_entities)
    links = validate_side_entity_links(side_entity_links)
    if text["source_artifact"]["artifact_digest"] != _digest(stage53):
        raise SideEntityValidationError("TEXT_ENTITIES stale for current Stage 5.3")
    if current_text_evidence_index is not None and text_entities_are_stale(
        text, stage53, current_text_evidence_index
    ):
        raise SideEntityValidationError("TEXT_ENTITIES stale for current Stage 5.3")
    if links["source_signatures"]["text_entities"] != text["source_signature"]:
        raise SideEntityValidationError("side entity links stale for TEXT_ENTITIES")
    entity = next(
        (item for item in text["entities"] if item["entity_id"] == text_entity_id), None
    )
    if entity is None:
        raise SideEntityValidationError("unknown text_entity_id")
    presence = _presence_for_side(entity, _stage_details(stage53), side)
    candidates = [
        link
        for link in links["sides"][side]["links"]
        if link["text_entity_id"] == text_entity_id
    ]
    high = [
        item
        for item in candidates
        if item["relation"] == "SAME_ENTITY" and item["confidence"] == "HIGH"
    ]
    possible = [item for item in candidates if item["relation"] == "POSSIBLE_ENTITY"]
    if high:
        match_state, match_reasons = "HIGH", ["same_entity_high_on_explicit_side"]
    elif possible:
        match_state, match_reasons = "MEDIUM", ["possible_entity_on_explicit_side"]
    elif candidates:
        match_state, match_reasons = "UNKNOWN", ["side_entity_link_unresolved"]
    else:
        match_state, match_reasons = "NOT_MATCHED", ["no_side_entity_link_candidate"]
    return {
        "text_entity_id": text_entity_id,
        "side": side,
        **presence,
        "match": match_state,
        "match_reason_codes": match_reasons,
        "entity_link_ids": sorted(item["entity_link_id"] for item in candidates),
        "high_graphic_entity_ids": sorted(item["graphic_entity_id"] for item in high),
    }


def graph_schema_path() -> Path:
    return Path(__file__).with_name("side_graph_entities.schema.json")


def links_schema_path() -> Path:
    return Path(__file__).with_name("side_entity_links.schema.json")


__all__ = [
    "GRAPH_KIND",
    "GRAPH_SCHEMA_VERSION",
    "LINK_KIND",
    "LINK_SCHEMA_VERSION",
    "MATCH_STATES",
    "PRESENCE_STATES",
    "SIDES",
    "SIDE_BRIDGE_VERSION",
    "SIDE_GRAPH_ADAPTER_VERSION",
    "SideEntityValidationError",
    "build_side_entity_links",
    "build_side_graph_entities",
    "graph_schema_path",
    "links_schema_path",
    "query_text_entity_side",
    "side_entity_links_are_stale",
    "side_graph_entities_are_stale",
    "validate_side_entity_links",
    "validate_side_graph_entities",
]
