"""Adapt one or more ready SYSTEM_GRAPH objects into stable GRAPH_ENTITIES.

Nodes are aggregated only when SYSTEM_GRAPH itself proves a representation
pair.  Equal labels are never a merge rule.
"""
from __future__ import annotations

import copy
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from backend.app.pipeline.stages.block_grounding.system_graph import (
    SCHEMA_VERSION as SYSTEM_GRAPH_SCHEMA_VERSION,
    validate_system_graph,
)

from .entity_normalizer import NORMALIZER_VERSION, canonical_entity_name


SCHEMA_VERSION = "graph-entities.v1"
KIND = "system_graph_entities"
ADAPTER_VERSION = "system-graph-entity-adapter-v1"

ENTITY_TYPES = frozenset(
    {"SYSTEM", "EQUIPMENT", "FUNCTIONAL_NODE", "ROOM", "MATERIAL", "GROUP", "OTHER"}
)
CONFIDENCE_LEVELS = frozenset({"HIGH", "MEDIUM", "LOW", "UNKNOWN"})

_REPRESENTATION_ROLE_PAIRS = frozenset(
    {frozenset({"OUTGOING_DEVICE", "LOAD"})}
)
_GROUP_NODE_TYPES = frozenset(
    {"METERING_GROUP", "COMPENSATION_GROUP", "SERVICE_GROUP"}
)
_SYSTEM_NODE_TYPES = frozenset({"SOURCE", "BUS_SECTION"})


class GraphEntityValidationError(ValueError):
    """The SYSTEM_GRAPH input or produced GRAPH_ENTITIES artifact is invalid."""


def _digest(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: Any) -> str:
    return prefix + _digest(parts)[:20]


def _values(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value)
    return [value]


def _unique_strings(values: Iterable[Any]) -> list[str]:
    output: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in output:
            output.append(text)
    return output


def _profile_id(graph: dict[str, Any]) -> str | None:
    profile = graph.get("profile")
    value = graph.get("profile_id") or (
        profile.get("id") if isinstance(profile, dict) else None
    )
    return str(value).strip() if str(value or "").strip() else None


def _source_metadata(graph: Any, graph_index: int) -> dict[str, Any]:
    validation = validate_system_graph(graph)
    graph_dict = graph if isinstance(graph, dict) else {}
    block = graph_dict.get("block") if isinstance(graph_dict.get("block"), dict) else {}
    provenance = (
        graph_dict.get("provenance")
        if isinstance(graph_dict.get("provenance"), dict)
        else {}
    )
    vector = (
        provenance.get("vector_evidence")
        if isinstance(provenance.get("vector_evidence"), dict)
        else {}
    )
    return {
        "graph_index": graph_index,
        "kind": "SYSTEM_GRAPH",
        "schema_version": graph_dict.get("schema_version"),
        "graph_digest": _digest(graph),
        "profile_id": _profile_id(graph_dict),
        "profile_version": provenance.get("profile_version"),
        "extractor_version": vector.get("extraction_version"),
        "block_id": block.get("block_id"),
        "page_index": block.get("page_index"),
        "discipline": graph_dict.get("discipline"),
        "valid": validation["valid"],
        "validation_errors": list(validation["errors"]),
    }


def source_signature(graphs: Iterable[Any]) -> str:
    """Return the signature for the exact graph/profile/normalizer inputs."""
    graph_list = list(graphs)
    return _digest(
        {
            "adapter_version": ADAPTER_VERSION,
            "normalizer_version": NORMALIZER_VERSION,
            "source_graphs": [
                _source_metadata(graph, index) for index, graph in enumerate(graph_list)
            ],
        }
    )


def is_stale(artifact: Any, graphs: Iterable[Any]) -> bool:
    """Return true when any graph, profile, extractor, adapter, or normalizer changed."""
    if not isinstance(artifact, dict):
        return True
    try:
        return artifact.get("source_signature") != source_signature(list(graphs))
    except (TypeError, ValueError):
        return True


def _node_labels(node: dict[str, Any]) -> list[str]:
    attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
    labels = _unique_strings(
        [
            node.get("display_label"),
            node.get("label"),
            *_values(attrs.get("identity_set")),
        ]
    )
    return labels


def _node_canonical(node: dict[str, Any]) -> str:
    for value in (node.get("canonical_identity"), *_node_labels(node), node.get("id")):
        canonical = canonical_entity_name(value)
        if canonical:
            return canonical
    return ""


def _entity_type(node_types: set[str]) -> str:
    if node_types <= _SYSTEM_NODE_TYPES:
        return "SYSTEM"
    if node_types <= _GROUP_NODE_TYPES:
        return "GROUP"
    if node_types == {"UNKNOWN_NODE"}:
        return "OTHER"
    if node_types & {
        "INPUT_DEVICE",
        "SECTION_DEVICE",
        "OUTGOING_DEVICE",
        "LOAD",
    }:
        return "EQUIPMENT"
    return "FUNCTIONAL_NODE"


def _base_context(
    graph: dict[str, Any], nodes: dict[str, dict[str, Any]], edges: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    context = {
        node_id: {"sections": set(), "parent_node_ids": set()}
        for node_id in nodes
    }
    for node_id, node in nodes.items():
        attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
        for value in (node.get("section"), attrs.get("section")):
            if str(value or "").strip():
                context[node_id]["sections"].add(str(value).strip())
    for edge in edges:
        source_id = str(edge.get("from") or "")
        target_id = str(edge.get("to") or "")
        if source_id not in context or target_id not in context:
            continue
        if edge.get("type") == "BELONGS_TO_SECTION":
            context[source_id]["sections"].add(target_id)
            context[source_id]["parent_node_ids"].add(target_id)
        elif edge.get("type") == "FEEDS":
            source = nodes[target_id]
            if source.get("type") not in {"BUS_SECTION", "SOURCE"}:
                context[target_id]["parent_node_ids"].add(source_id)
                if nodes[source_id].get("type") == "BUS_SECTION":
                    context[target_id]["sections"].add(source_id)
    # A terminal representation inherits a section only through its explicit
    # TERMINATES_AT relation, never through geometry or label similarity.
    for edge in edges:
        if edge.get("type") != "TERMINATES_AT":
            continue
        source_id = str(edge.get("from") or "")
        target_id = str(edge.get("to") or "")
        if source_id not in context or target_id not in context:
            continue
        source_sections = context[source_id]["sections"]
        target_sections = context[target_id]["sections"]
        if source_sections and not target_sections:
            target_sections.update(source_sections)
            context[target_id]["parent_node_ids"].update(
                context[source_id]["parent_node_ids"]
            )
        elif target_sections and not source_sections:
            source_sections.update(target_sections)
            context[source_id]["parent_node_ids"].update(
                context[target_id]["parent_node_ids"]
            )
    return context


def _source_path(
    node_ids: set[str], nodes: dict[str, dict[str, Any]], edges: list[dict[str, Any]]
) -> list[str]:
    incoming: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        if edge.get("type") == "FEEDS":
            source_id, target_id = str(edge.get("from") or ""), str(edge.get("to") or "")
            if source_id in nodes and target_id in nodes:
                incoming[target_id].append(source_id)
    found: set[str] = set()
    pending = list(node_ids)
    while pending:
        current = pending.pop()
        for parent in incoming.get(current, []):
            if parent not in found and parent not in node_ids:
                found.add(parent)
                pending.append(parent)
    return sorted(found)


def _merge_pairs(
    nodes: dict[str, dict[str, Any]],
    edges: list[dict[str, Any]],
    context: dict[str, dict[str, Any]],
) -> tuple[list[set[str]], dict[frozenset[str], str]]:
    possible: list[tuple[str, str, str]] = []
    degree: dict[str, int] = defaultdict(int)
    for edge in edges:
        if edge.get("type") != "TERMINATES_AT":
            continue
        source_id, target_id = str(edge.get("from") or ""), str(edge.get("to") or "")
        if source_id not in nodes or target_id not in nodes:
            continue
        source, target = nodes[source_id], nodes[target_id]
        roles = frozenset({str(source.get("type")), str(target.get("type"))})
        if roles not in _REPRESENTATION_ROLE_PAIRS:
            continue
        if not _node_canonical(source) or _node_canonical(source) != _node_canonical(target):
            continue
        source_sections = context[source_id]["sections"]
        target_sections = context[target_id]["sections"]
        if not source_sections or source_sections != target_sections or len(source_sections) != 1:
            continue
        if source.get("conflicts") or target.get("conflicts"):
            continue
        possible.append((source_id, target_id, str(edge.get("id") or "")))
        degree[source_id] += 1
        degree[target_id] += 1

    paired: set[str] = set()
    groups: list[set[str]] = []
    merge_edge: dict[frozenset[str], str] = {}
    for source_id, target_id, edge_id in possible:
        if degree[source_id] != 1 or degree[target_id] != 1:
            continue
        if source_id in paired or target_id in paired:
            continue
        group = {source_id, target_id}
        groups.append(group)
        paired.update(group)
        merge_edge[frozenset(group)] = edge_id
    groups.extend({node_id} for node_id in sorted(set(nodes) - paired))
    groups.sort(key=lambda group: tuple(sorted(group)))
    return groups, merge_edge


def _confidence(
    members: list[dict[str, Any]], merge_edge: dict[str, Any] | None
) -> str:
    if any(node.get("conflicts") for node in members):
        return "UNKNOWN"
    raw_values = [float(node.get("confidence") or 0.0) for node in members]
    if merge_edge is not None:
        raw_values.append(float(merge_edge.get("confidence") or 0.0))
    raw = min(raw_values, default=0.0)
    if raw >= 0.85:
        return "HIGH"
    if raw >= 0.65:
        return "MEDIUM"
    return "LOW"


def _build_graph_entity(
    *,
    graph: dict[str, Any],
    metadata: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
    edges: list[dict[str, Any]],
    context: dict[str, dict[str, Any]],
    group: set[str],
    merge_edge_id: str | None,
) -> dict[str, Any]:
    member_ids = sorted(group)
    members = [nodes[node_id] for node_id in member_ids]
    canonical_names = {_node_canonical(node) for node in members}
    canonical_names.discard("")
    canonical = (
        sorted(canonical_names)[0]
        if canonical_names
        else canonical_entity_name(member_ids[0])
    )
    sections = {
        section for node_id in member_ids for section in context[node_id]["sections"]
    }
    section = next(iter(sections)) if len(sections) == 1 else None
    parent_ids = sorted(
        {
            parent
            for node_id in member_ids
            for parent in context[node_id]["parent_node_ids"]
        }
    )
    incident = [
        edge
        for edge in edges
        if edge.get("from") in group or edge.get("to") in group
    ]
    edge_ids = sorted(
        {str(edge.get("id")) for edge in incident if str(edge.get("id") or "").strip()}
    )
    merge_edge = next(
        (edge for edge in edges if str(edge.get("id") or "") == merge_edge_id), None
    )
    node_types = {str(node.get("type") or "") for node in members}
    source_tokens = _unique_strings(
        [
            *(token for node in members for token in node.get("source_tokens") or []),
            *(token for edge in incident for token in edge.get("source_tokens") or []),
        ]
    )
    display_labels = _unique_strings(
        [label for node in members for label in _node_labels(node)]
    )
    if not display_labels:
        display_labels = [canonical]
    source_path = _source_path(group, nodes, edges)
    functional_role = (
        "CONSUMER_FEEDER"
        if node_types == {"OUTGOING_DEVICE", "LOAD"}
        else sorted(node_types)[0]
    )
    entity_id = _stable_id(
        "gfx_ent_",
        metadata["graph_digest"],
        metadata["graph_index"],
        canonical,
        section,
        parent_ids,
        sorted(node_types),
        member_ids,
        edge_ids,
    )
    return {
        "entity_id": entity_id,
        "graph_node_ids": member_ids,
        "canonical_name": canonical,
        "display_labels": display_labels,
        "entity_type": _entity_type(node_types),
        "domain_subtype": "+".join(sorted(node_types)),
        "functional_role": functional_role,
        "system": str(graph.get("system") or graph.get("discipline") or "").strip() or None,
        "parent_context": {
            "parent_group": section,
            "parent_node_ids": parent_ids,
        },
        "section_context": section,
        "graph_scope": {
            "source_graph_index": metadata["graph_index"],
            "graph_digest": metadata["graph_digest"],
            "profile_id": metadata["profile_id"],
            "block_id": metadata["block_id"],
            "page_index": metadata["page_index"],
            "discipline": metadata["discipline"],
            "source_path": source_path,
        },
        "edge_ids": edge_ids,
        "source_tokens": source_tokens,
        "locations": [
            {
                "node_id": node_id,
                "block_id": metadata["block_id"],
                "page_index": metadata["page_index"],
                "bbox": copy.deepcopy(nodes[node_id].get("bbox")),
            }
            for node_id in member_ids
        ],
        "evidence_refs": [
            {"kind": "NODE", "id": node_id} for node_id in member_ids
        ]
        + [{"kind": "EDGE", "id": edge_id} for edge_id in edge_ids],
        "confidence": _confidence(members, merge_edge),
        "provenance": {
            "source_graph_index": metadata["graph_index"],
            "graph_digest": metadata["graph_digest"],
            "profile_version": metadata["profile_version"],
            "extractor_version": metadata["extractor_version"],
            "adapter_version": ADAPTER_VERSION,
            "normalizer_version": NORMALIZER_VERSION,
            "aggregation_rule": (
                "TERMINATES_AT_REPRESENTATION_PAIR"
                if len(member_ids) > 1
                else "SINGLE_GRAPH_NODE"
            ),
            "aggregation_edge_id": merge_edge_id,
        },
    }


def build_graph_entities(graphs: Iterable[Any]) -> dict[str, Any]:
    """Produce GRAPH_ENTITIES, failing invalid SYSTEM_GRAPH inputs closed to zero."""
    graph_list = list(graphs)
    source_graphs = [
        _source_metadata(graph, index) for index, graph in enumerate(graph_list)
    ]
    entities: list[dict[str, Any]] = []
    total_nodes = invalid_nodes = unresolved_context = 0
    for graph, metadata in zip(graph_list, source_graphs):
        if isinstance(graph, dict) and isinstance(graph.get("nodes"), list):
            total_nodes += len(graph["nodes"])
        if not metadata["valid"]:
            invalid_nodes += len(graph.get("nodes") or []) if isinstance(graph, dict) else 0
            continue
        nodes = {str(node["id"]): node for node in graph["nodes"]}
        edges = list(graph["edges"])
        context = _base_context(graph, nodes, edges)
        groups, merge_edges = _merge_pairs(nodes, edges, context)
        for group in groups:
            entity = _build_graph_entity(
                graph=graph,
                metadata=metadata,
                nodes=nodes,
                edges=edges,
                context=context,
                group=group,
                merge_edge_id=merge_edges.get(frozenset(group)),
            )
            unresolved_context += int(
                entity["functional_role"] in {"OUTGOING_DEVICE", "LOAD", "CONSUMER_FEEDER"}
                and not entity["section_context"]
            )
            entities.append(entity)
    entities.sort(key=lambda item: item["entity_id"])
    valid_graphs = sum(metadata["valid"] for metadata in source_graphs)
    produced = len(entities)
    duplicated = max(0, total_nodes - invalid_nodes - produced)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "adapter_version": ADAPTER_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "source_signature": _digest(
            {
                "adapter_version": ADAPTER_VERSION,
                "normalizer_version": NORMALIZER_VERSION,
                "source_graphs": source_graphs,
            }
        ),
        "source_graphs": source_graphs,
        "entities": entities,
        "quality_report": {
            "total_source_graphs": len(graph_list),
            "valid_source_graphs": valid_graphs,
            "invalid_source_graphs": len(graph_list) - valid_graphs,
            "total_source_candidates": total_nodes,
            "source_nodes": total_nodes,
            "produced_entities": produced,
            "dropped_noise": invalid_nodes,
            "ambiguous": unresolved_context,
            "duplicated": duplicated,
            "unresolved_context": unresolved_context,
        },
    }
    return validate_graph_entities(artifact)


def validate_graph_entities(payload: Any) -> dict[str, Any]:
    """Validate GRAPH_ENTITIES and its source/provenance references."""
    if not isinstance(payload, dict):
        raise GraphEntityValidationError("GRAPH_ENTITIES: object required")
    required = {
        "schema_version",
        "kind",
        "adapter_version",
        "normalizer_version",
        "source_signature",
        "source_graphs",
        "entities",
        "quality_report",
    }
    if set(payload) != required:
        raise GraphEntityValidationError("GRAPH_ENTITIES: invalid envelope fields")
    if (
        payload["schema_version"] != SCHEMA_VERSION
        or payload["kind"] != KIND
        or payload["adapter_version"] != ADAPTER_VERSION
        or payload["normalizer_version"] != NORMALIZER_VERSION
    ):
        raise GraphEntityValidationError("GRAPH_ENTITIES: unsupported contract")
    if not isinstance(payload["source_graphs"], list):
        raise GraphEntityValidationError("GRAPH_ENTITIES.source_graphs: array required")
    expected_signature = _digest(
        {
            "adapter_version": ADAPTER_VERSION,
            "normalizer_version": NORMALIZER_VERSION,
            "source_graphs": payload["source_graphs"],
        }
    )
    if payload["source_signature"] != expected_signature:
        raise GraphEntityValidationError("GRAPH_ENTITIES.source_signature: invalid")
    graph_indexes: set[int] = set()
    valid_graph_indexes: set[int] = set()
    metadata_by_index: dict[int, dict[str, Any]] = {}
    for metadata in payload["source_graphs"]:
        if not isinstance(metadata, dict):
            raise GraphEntityValidationError("GRAPH_ENTITIES.source_graphs: object required")
        required_metadata = {
            "graph_index",
            "kind",
            "schema_version",
            "graph_digest",
            "profile_id",
            "profile_version",
            "extractor_version",
            "block_id",
            "page_index",
            "discipline",
            "valid",
            "validation_errors",
        }
        if set(metadata) != required_metadata or metadata.get("kind") != "SYSTEM_GRAPH":
            raise GraphEntityValidationError("GRAPH_ENTITIES.source_graphs: invalid fields")
        graph_index = metadata.get("graph_index")
        if (
            not isinstance(graph_index, int)
            or isinstance(graph_index, bool)
            or graph_index in graph_indexes
        ):
            raise GraphEntityValidationError("GRAPH_ENTITIES.source_graphs: invalid graph_index")
        graph_indexes.add(graph_index)
        metadata_by_index[graph_index] = metadata
        if metadata.get("valid") is True:
            valid_graph_indexes.add(graph_index)
        if metadata.get("valid") and metadata.get("schema_version") != SYSTEM_GRAPH_SCHEMA_VERSION:
            raise GraphEntityValidationError("GRAPH_ENTITIES.source_graphs: invalid schema")
        if not isinstance(metadata.get("validation_errors"), list):
            raise GraphEntityValidationError(
                "GRAPH_ENTITIES.source_graphs: validation_errors required"
            )
    if not isinstance(payload["entities"], list):
        raise GraphEntityValidationError("GRAPH_ENTITIES.entities: array required")
    entity_ids: set[str] = set()
    seen_nodes: set[tuple[int, str]] = set()
    for index, entity in enumerate(payload["entities"]):
        where = f"GRAPH_ENTITIES.entities[{index}]"
        required_entity = {
            "entity_id",
            "graph_node_ids",
            "canonical_name",
            "display_labels",
            "entity_type",
            "domain_subtype",
            "functional_role",
            "system",
            "parent_context",
            "section_context",
            "graph_scope",
            "edge_ids",
            "source_tokens",
            "locations",
            "evidence_refs",
            "confidence",
            "provenance",
        }
        if not isinstance(entity, dict) or set(entity) != required_entity:
            raise GraphEntityValidationError(f"{where}: invalid fields")
        entity_id = entity.get("entity_id")
        if (
            not isinstance(entity_id, str)
            or not entity_id.startswith("gfx_ent_")
            or entity_id in entity_ids
        ):
            raise GraphEntityValidationError(f"{where}.entity_id: invalid")
        entity_ids.add(entity_id)
        scope = entity.get("graph_scope")
        graph_index = scope.get("source_graph_index") if isinstance(scope, dict) else None
        if graph_index not in valid_graph_indexes:
            raise GraphEntityValidationError(f"{where}.graph_scope: invalid source")
        source_metadata = metadata_by_index[graph_index]
        if scope.get("graph_digest") != source_metadata["graph_digest"]:
            raise GraphEntityValidationError(f"{where}.graph_scope: digest mismatch")
        node_ids = entity.get("graph_node_ids")
        if (
            not isinstance(node_ids, list)
            or not node_ids
            or any(not isinstance(value, str) or not value for value in node_ids)
            or len(node_ids) != len(set(node_ids))
        ):
            raise GraphEntityValidationError(f"{where}.graph_node_ids: invalid")
        for node_id in node_ids:
            key = (graph_index, node_id)
            if key in seen_nodes:
                raise GraphEntityValidationError(f"{where}.graph_node_ids: reused node")
            seen_nodes.add(key)
        if (
            not isinstance(entity.get("canonical_name"), str)
            or not entity["canonical_name"]
            or canonical_entity_name(entity["canonical_name"]) != entity["canonical_name"]
        ):
            raise GraphEntityValidationError(f"{where}.canonical_name: invalid")
        if entity.get("entity_type") not in ENTITY_TYPES:
            raise GraphEntityValidationError(f"{where}.entity_type: invalid")
        if entity.get("confidence") not in CONFIDENCE_LEVELS:
            raise GraphEntityValidationError(f"{where}.confidence: invalid")
        for key in ("display_labels", "edge_ids", "source_tokens"):
            values = entity.get(key)
            if (
                not isinstance(values, list)
                or (key == "display_labels" and not values)
                or any(not isinstance(value, str) or not value for value in values)
                or len(values) != len(set(values))
            ):
                raise GraphEntityValidationError(f"{where}.{key}: invalid")
        if not isinstance(entity.get("locations"), list) or not entity["locations"]:
            raise GraphEntityValidationError(f"{where}.locations: invalid")
        if not isinstance(entity.get("evidence_refs"), list) or not entity["evidence_refs"]:
            raise GraphEntityValidationError(f"{where}.evidence_refs: invalid")
        if not isinstance(entity.get("provenance"), dict) or not entity["provenance"]:
            raise GraphEntityValidationError(f"{where}.provenance: invalid")
        parent = entity.get("parent_context")
        if (
            not isinstance(parent, dict)
            or set(parent) != {"parent_group", "parent_node_ids"}
            or not isinstance(parent["parent_node_ids"], list)
            or any(
                not isinstance(value, str) or not value
                for value in parent["parent_node_ids"]
            )
            or len(parent["parent_node_ids"])
            != len(set(parent["parent_node_ids"]))
        ):
            raise GraphEntityValidationError(f"{where}.parent_context: invalid")
        node_types = sorted(str(entity["domain_subtype"]).split("+"))
        expected_entity_id = _stable_id(
            "gfx_ent_",
            source_metadata["graph_digest"],
            graph_index,
            entity["canonical_name"],
            entity["section_context"],
            parent["parent_node_ids"],
            node_types,
            node_ids,
            entity["edge_ids"],
        )
        if entity_id != expected_entity_id:
            raise GraphEntityValidationError(f"{where}.entity_id: not stable")
    quality = payload["quality_report"]
    quality_keys = {
        "total_source_graphs",
        "valid_source_graphs",
        "invalid_source_graphs",
        "total_source_candidates",
        "source_nodes",
        "produced_entities",
        "dropped_noise",
        "ambiguous",
        "duplicated",
        "unresolved_context",
    }
    if (
        not isinstance(quality, dict)
        or set(quality) != quality_keys
        or any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in quality.values()
        )
        or quality["produced_entities"] != len(payload["entities"])
        or quality["total_source_graphs"] != len(payload["source_graphs"])
        or quality["valid_source_graphs"] + quality["invalid_source_graphs"]
        != quality["total_source_graphs"]
    ):
        raise GraphEntityValidationError("GRAPH_ENTITIES.quality_report: invalid")
    try:
        json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise GraphEntityValidationError("GRAPH_ENTITIES: not JSON-compatible") from error
    return payload


def schema_path() -> Path:
    return Path(__file__).with_name("graph_entities.schema.json")


__all__ = [
    "ADAPTER_VERSION",
    "GraphEntityValidationError",
    "KIND",
    "SCHEMA_VERSION",
    "build_graph_entities",
    "is_stale",
    "schema_path",
    "source_signature",
    "validate_graph_entities",
]
