"""Discipline-neutral SYSTEM_GRAPH contract for prepared graphic blocks.

This module defines the graph envelope and structural validation only.  It has
no knowledge of QF, buses, electrical disciplines, comparison, or change types.
"""
from __future__ import annotations

import math
from typing import Iterable, Optional


SCHEMA_VERSION = "system-graph.v1"

NODE_TYPES = frozenset(
    {
        "SOURCE",
        "INPUT_DEVICE",
        "BUS_SECTION",
        "SECTION_DEVICE",
        "OUTGOING_DEVICE",
        "LOAD",
        "METERING_GROUP",
        "COMPENSATION_GROUP",
        "SERVICE_GROUP",
        "UNKNOWN_NODE",
    }
)

EDGE_TYPES = frozenset(
    {
        "FEEDS",
        "BELONGS_TO_SECTION",
        "TIES_SECTIONS",
        "MEASURES",
        "PROTECTS_OR_SWITCHES",
        "TERMINATES_AT",
    }
)


def normalized_bbox(value: Optional[Iterable[float]]) -> list[float]:
    """Return a finite four-number bbox shape without interpreting its meaning."""
    try:
        bbox = [round(float(item), 3) for item in value]
    except (TypeError, ValueError):
        return [0.0, 0.0, 0.0, 0.0]
    if len(bbox) != 4 or not all(math.isfinite(value) for value in bbox):
        return [0.0, 0.0, 0.0, 0.0]
    x0, y0, x1, y1 = bbox
    return [min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)]


def union_bbox(*values: Optional[Iterable[float]]) -> list[float]:
    boxes = [normalized_bbox(value) for value in values if value is not None]
    boxes = [box for box in boxes if box != [0.0, 0.0, 0.0, 0.0]]
    if not boxes:
        return [0.0, 0.0, 0.0, 0.0]
    return [
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    ]


def evidence_tokens(evidence: list[dict]) -> list[str]:
    tokens = []
    for item in evidence:
        for token in item.get("source_tokens") or []:
            token = str(token).strip()
            if token and token not in tokens:
                tokens.append(token)
        if item.get("kind") == "token" and item.get("value") is not None:
            token = str(item["value"]).strip()
            if token and token not in tokens:
                tokens.append(token)
    return tokens


def make_node(
    node_id: str,
    node_type: str,
    *,
    confidence: float,
    evidence: list[dict],
    bbox: Optional[Iterable[float]],
    source_tokens: Optional[Iterable[str]] = None,
    **attributes,
) -> dict:
    tokens = [str(token) for token in (source_tokens or []) if str(token).strip()]
    for token in evidence_tokens(evidence):
        if token not in tokens:
            tokens.append(token)
    return {
        "id": str(node_id),
        "type": str(node_type),
        "confidence": round(max(0.0, min(1.0, float(confidence))), 3),
        "evidence": list(evidence),
        "bbox": normalized_bbox(bbox),
        "source_tokens": tokens,
        **attributes,
    }


def make_edge(
    edge_id: str,
    edge_type: str,
    source: str,
    target: str,
    *,
    confidence: float,
    evidence: list[dict],
    source_bbox: Optional[Iterable[float]],
    target_bbox: Optional[Iterable[float]],
    source_tokens: Optional[Iterable[str]] = None,
    **attributes,
) -> dict:
    tokens = [str(token) for token in (source_tokens or []) if str(token).strip()]
    for token in evidence_tokens(evidence):
        if token not in tokens:
            tokens.append(token)
    return {
        "id": str(edge_id),
        "type": str(edge_type),
        "from": str(source),
        "to": str(target),
        "confidence": round(max(0.0, min(1.0, float(confidence))), 3),
        "evidence": list(evidence),
        "bbox": union_bbox(source_bbox, target_bbox),
        "source_tokens": tokens,
        **attributes,
    }


def validate_system_graph(graph: Optional[dict]) -> dict:
    """Validate the common contract and referential integrity, fail-closed."""
    errors = []
    if not isinstance(graph, dict):
        return {"valid": False, "errors": ["graph_not_object"]}
    if graph.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version_invalid")
    for field in ("block", "profile", "nodes", "edges", "quality", "provenance"):
        if field not in graph:
            errors.append(f"missing_graph_field:{field}")

    nodes = graph.get("nodes") if isinstance(graph.get("nodes"), list) else []
    edges = graph.get("edges") if isinstance(graph.get("edges"), list) else []
    node_ids = []
    for index, node in enumerate(nodes):
        prefix = f"node:{index}"
        if not isinstance(node, dict):
            errors.append(f"{prefix}:not_object")
            continue
        node_id = str(node.get("id") or "")
        if not node_id:
            errors.append(f"{prefix}:missing_id")
        elif node_id in node_ids:
            errors.append(f"{prefix}:duplicate_id:{node_id}")
        node_ids.append(node_id)
        if node.get("type") not in NODE_TYPES:
            errors.append(f"{prefix}:node_type_invalid")
        _validate_grounded_item(node, prefix, errors)

    known = set(node_ids)
    edge_ids = set()
    for index, edge in enumerate(edges):
        prefix = f"edge:{index}"
        if not isinstance(edge, dict):
            errors.append(f"{prefix}:not_object")
            continue
        edge_id = str(edge.get("id") or "")
        if not edge_id:
            errors.append(f"{prefix}:missing_id")
        elif edge_id in edge_ids:
            errors.append(f"{prefix}:duplicate_id:{edge_id}")
        edge_ids.add(edge_id)
        if edge.get("type") not in EDGE_TYPES:
            errors.append(f"{prefix}:edge_type_invalid")
        if edge.get("from") not in known:
            errors.append(f"{prefix}:unknown_from")
        if edge.get("to") not in known:
            errors.append(f"{prefix}:unknown_to")
        _validate_grounded_item(edge, prefix, errors)
    return {"valid": not errors, "errors": errors}


def _validate_grounded_item(item: dict, prefix: str, errors: list[str]) -> None:
    confidence = item.get("confidence")
    if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        errors.append(f"{prefix}:confidence_invalid")
    bbox = item.get("bbox")
    if not isinstance(bbox, list) or len(bbox) != 4:
        errors.append(f"{prefix}:bbox_invalid")
    elif not all(
        isinstance(value, (int, float)) and math.isfinite(float(value))
        for value in bbox
    ):
        errors.append(f"{prefix}:bbox_non_finite")
    evidence = item.get("evidence")
    if not isinstance(evidence, list):
        errors.append(f"{prefix}:evidence_invalid")
    elif not evidence:
        errors.append(f"{prefix}:evidence_empty")
    source_tokens = item.get("source_tokens")
    if not isinstance(source_tokens, list):
        errors.append(f"{prefix}:source_tokens_invalid")
    elif not source_tokens:
        errors.append(f"{prefix}:source_tokens_empty")


__all__ = [
    "EDGE_TYPES",
    "NODE_TYPES",
    "SCHEMA_VERSION",
    "make_edge",
    "make_node",
    "normalized_bbox",
    "union_bbox",
    "validate_system_graph",
]
