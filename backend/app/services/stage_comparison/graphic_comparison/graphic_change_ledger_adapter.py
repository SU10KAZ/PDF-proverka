"""Map a decided SYSTEM_GRAPH comparison into GraphicChangeLedger Mode 2.

The adapter does not compare graphs or classify changes. It verifies the source
contracts, copies the comparator decision, and grounds Ledger addresses/evidence
from the original SYSTEM_GRAPH objects.
"""
from __future__ import annotations

import copy
import math
from typing import Any, Iterable

from backend.app.pipeline.stages.block_grounding.system_graph import (
    union_bbox,
    validate_system_graph,
)
from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (
    validate_comparison_result,
)

from .confidence_policy import (
    MODE2_CONFIDENCE_POLICY_V1,
    LedgerConfidencePolicy,
)
from .contract import MODE2_SCHEMA_VERSION, LedgerValidationError, validate_ledger


ADAPTER_ID = "system-graph-comparison-to-graphic-change-ledger"
ADAPTER_VERSION = "system-graph-ledger-adapter-v1"


class LedgerAdapterError(ValueError):
    """The source comparison/graphs cannot be mapped without guessing."""


def _unique_strings(value: Any, where: str) -> list[str]:
    if not isinstance(value, list):
        raise LedgerAdapterError(f"{where}: array required")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise LedgerAdapterError(f"{where}: non-empty strings required")
    if len(value) != len(set(value)):
        raise LedgerAdapterError(f"{where}: duplicate ids")
    return list(value)


def _graph_address(graph: dict, side: str) -> dict:
    block = graph.get("block")
    if not isinstance(block, dict):
        raise LedgerAdapterError(f"{side}_graph.block: object required")
    block_id = block.get("block_id")
    page_index = block.get("page_index")
    bbox = block.get("bbox_visual_pt")
    if not isinstance(block_id, str) or not block_id.strip():
        raise LedgerAdapterError(f"{side}_graph.block.block_id: non-empty string required")
    if (
        not isinstance(page_index, int)
        or isinstance(page_index, bool)
        or page_index < 0
    ):
        raise LedgerAdapterError(f"{side}_graph.block.page_index: invalid")
    if (
        not isinstance(bbox, list)
        or len(bbox) != 4
        or any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            for value in bbox
        )
        or bbox[2] < bbox[0]
        or bbox[3] < bbox[1]
    ):
        raise LedgerAdapterError(f"{side}_graph.block.bbox_visual_pt: invalid")
    profile_id = graph.get("profile_id") or (graph.get("profile") or {}).get("id")
    if not isinstance(profile_id, str) or not profile_id:
        raise LedgerAdapterError(f"{side}_graph.profile_id: non-empty string required")
    provenance = graph.get("provenance")
    if not isinstance(provenance, dict):
        raise LedgerAdapterError(f"{side}_graph.provenance: object required")
    return {
        "block_id": block_id,
        "page_index": page_index,
        "bbox_visual_pt": [float(value) for value in bbox],
        "profile_id": profile_id,
    }


def _validate_graph(graph: Any, side: str) -> dict:
    if not isinstance(graph, dict):
        raise LedgerAdapterError(f"{side}_graph: object required")
    validation = validate_system_graph(graph)
    if not validation["valid"]:
        raise LedgerAdapterError(
            f"{side}_graph: invalid SYSTEM_GRAPH: {validation['errors']}"
        )
    return _graph_address(graph, side)


def _validate_comparison(comparison: Any) -> dict:
    if not isinstance(comparison, dict):
        raise LedgerAdapterError("comparison_result: object required")
    try:
        validation = validate_comparison_result(comparison)
    except (AttributeError, TypeError, ValueError) as error:
        raise LedgerAdapterError("comparison_result: malformed") from error
    if not validation["valid"]:
        raise LedgerAdapterError(
            f"comparison_result: invalid: {validation['errors']}"
        )
    if not isinstance(comparison.get("changes"), list):
        raise LedgerAdapterError("comparison_result.changes: array required")
    return validation


def _validate_graph_link(
    comparison: dict,
    graph: dict,
    address: dict,
    side: str,
) -> None:
    reference = comparison.get(f"{side}_graph")
    if not isinstance(reference, dict):
        raise LedgerAdapterError(f"comparison_result.{side}_graph: object required")
    expected = {
        "schema_version": graph.get("schema_version"),
        "profile_id": graph.get("profile_id"),
        "block_id": address["block_id"],
        "page_index": address["page_index"],
    }
    mismatched = [key for key, value in expected.items() if reference.get(key) != value]
    if mismatched:
        raise LedgerAdapterError(
            f"comparison_result.{side}_graph: source mismatch: {','.join(mismatched)}"
        )


def _index(graph: dict, field: str, side: str) -> dict[str, dict]:
    output: dict[str, dict] = {}
    for item in graph[field]:
        item_id = str(item["id"])
        if item_id in output:
            raise LedgerAdapterError(f"{side}_graph.{field}: duplicate id {item_id}")
        output[item_id] = item
    return output


def _comparison_evidence_side(
    change: dict,
    side: str,
) -> tuple[list[str], list[str]]:
    evidence = change.get("evidence")
    if not isinstance(evidence, dict):
        raise LedgerAdapterError(f"change {change.get('change_id')}: evidence object required")
    side_evidence = evidence.get(side)
    if not isinstance(side_evidence, dict):
        raise LedgerAdapterError(
            f"change {change.get('change_id')}: evidence.{side} object required"
        )
    node_ids = _unique_strings(
        side_evidence.get("node_ids"),
        f"change {change.get('change_id')}.evidence.{side}.node_ids",
    )
    edge_ids = _unique_strings(
        side_evidence.get("edge_ids"),
        f"change {change.get('change_id')}.evidence.{side}.edge_ids",
    )
    declared_nodes = _unique_strings(
        change.get(f"{side}_nodes"),
        f"change {change.get('change_id')}.{side}_nodes",
    )
    if node_ids != declared_nodes:
        raise LedgerAdapterError(
            f"change {change.get('change_id')}: {side} node evidence mismatch"
        )
    return node_ids, edge_ids


def _grounded_item(item: dict, kind: str) -> dict:
    source_tokens = []
    for token in item["source_tokens"]:
        token = str(token).strip()
        if token and token not in source_tokens:
            source_tokens.append(token)
    if not source_tokens:
        raise LedgerAdapterError(f"grounding {kind}:{item['id']}: source tokens required")
    return {
        "kind": kind,
        "id": str(item["id"]),
        "type": item["type"],
        "bbox_visual_pt": [float(value) for value in item["bbox"]],
        "source_tokens": source_tokens,
        "confidence": float(item["confidence"]),
        "evidence": copy.deepcopy(item["evidence"]),
    }


def _unique_tokens(items: Iterable[dict]) -> list[str]:
    output: list[str] = []
    for item in items:
        for token in item["source_tokens"]:
            if token not in output:
                output.append(token)
    return output


def _source_side_payload(
    graph: dict,
    address: dict,
    side: str,
    node_ids: list[str],
    edge_ids: list[str],
) -> tuple[dict | None, dict | None, dict | None]:
    if not node_ids and not edge_ids:
        return None, None, None
    nodes = _index(graph, "nodes", side)
    edges = _index(graph, "edges", side)
    missing_nodes = [item_id for item_id in node_ids if item_id not in nodes]
    missing_edges = [item_id for item_id in edge_ids if item_id not in edges]
    if missing_nodes or missing_edges:
        raise LedgerAdapterError(
            f"{side}_graph: missing referenced nodes={missing_nodes}, edges={missing_edges}"
        )
    grounding = [
        *[_grounded_item(nodes[item_id], "NODE") for item_id in node_ids],
        *[_grounded_item(edges[item_id], "EDGE") for item_id in edge_ids],
    ]
    confidence = round(min(item["confidence"] for item in grounding), 3)
    bbox = union_bbox(*(item["bbox_visual_pt"] for item in grounding))
    if bbox == [0.0, 0.0, 0.0, 0.0]:
        bbox = list(address["bbox_visual_pt"])
    evidence = {
        "kind": "SYSTEM_GRAPH",
        "source_graph": {
            "side": side.upper(),
            "schema_version": graph["schema_version"],
            "profile_id": address["profile_id"],
            "block_id": address["block_id"],
            "page_index": address["page_index"],
        },
        "node_ids": list(node_ids),
        "edge_ids": list(edge_ids),
        "source_tokens": _unique_tokens(grounding),
        "confidence": confidence,
        "graph_provenance": copy.deepcopy(graph["provenance"]),
        "grounding": grounding,
    }
    region = {
        "block_id": address["block_id"],
        "page_index": address["page_index"],
        "bbox_visual_pt": bbox,
    }
    address_hint = {
        "kind": "SYSTEM_GRAPH_LOCATION",
        "side": side.upper(),
        "block_id": address["block_id"],
        "page_index": address["page_index"],
        "bbox_visual_pt": bbox,
        "node_ids": list(node_ids),
        "edge_ids": list(edge_ids),
    }
    return evidence, region, address_hint


def _structural_level(change_type: str, source_level: str) -> str:
    fixed = {
        "SYSTEM_BACKBONE_CHANGED": "SYSTEM",
        "FUNCTIONAL_GROUP_CHANGED": "GROUP",
        "GROUP_COUNT_CHANGED": "GROUP",
        "NODE_ADDED": "NODE",
        "NODE_REMOVED": "NODE",
        "NODE_TYPE_CHANGED": "NODE",
        "NODE_PARAMETER_CHANGED": "NODE",
        "CONNECTION_CHANGED": "EDGE",
    }
    return fixed.get(
        change_type,
        {"A": "SYSTEM", "B": "GROUP", "C": "NODE"}.get(source_level, ""),
    )


def _map_change(
    change: dict,
    left_graph: dict,
    right_graph: dict,
    left_address: dict,
    right_address: dict,
    confidence_policy: LedgerConfidencePolicy,
) -> dict:
    change_id = change.get("change_id")
    if not isinstance(change_id, str) or not change_id:
        raise LedgerAdapterError("comparison change_id: non-empty string required")
    raw_confidence = change.get("confidence")
    try:
        mapped_confidence = confidence_policy.map(raw_confidence)
    except ValueError as error:
        raise LedgerAdapterError(f"change {change_id}: invalid confidence") from error
    source_level = change.get("level")
    change_type = change.get("type")
    level = _structural_level(change_type, source_level)
    if not level:
        raise LedgerAdapterError(f"change {change_id}: unsupported structural level")
    evidence = change.get("evidence")
    reason = evidence.get("reason") if isinstance(evidence, dict) else None
    if not isinstance(reason, dict):
        raise LedgerAdapterError(f"change {change_id}: evidence.reason object required")

    left_nodes, left_edges = _comparison_evidence_side(change, "left")
    right_nodes, right_edges = _comparison_evidence_side(change, "right")
    left_payload = _source_side_payload(
        left_graph, left_address, "left", left_nodes, left_edges
    )
    right_payload = _source_side_payload(
        right_graph, right_address, "right", right_nodes, right_edges
    )
    source_evidence = [
        payload[0] for payload in (left_payload, right_payload) if payload[0] is not None
    ]
    regions = [left_payload[1], right_payload[1]]
    address_hints = [
        payload[2] for payload in (left_payload, right_payload) if payload[2] is not None
    ]
    structural = {
        "level": level,
        "source_level": source_level,
        "subject": copy.deepcopy(change.get("subject")),
        "left_nodes": left_nodes,
        "right_nodes": right_nodes,
        "left_edges": left_edges,
        "right_edges": right_edges,
        "relation": copy.deepcopy(reason),
    }
    if change_type == "DETAIL_LEVEL_INCREASED":
        structural["equivalence"] = "representation_expansion"
    return {
        "change_id": change_id,
        "mode": "MODE_2",
        "type": change_type,
        "summary": change.get("summary"),
        "raw_confidence": float(raw_confidence),
        "mapped_confidence": mapped_confidence,
        "left_region": regions[0],
        "right_region": regions[1],
        "evidence": source_evidence,
        "address_hints": address_hints,
        "confidence": mapped_confidence,
        "provenance": ["VECTOR"],
        "structural": structural,
    }


def _scope(graph: dict, address: dict) -> dict:
    return {
        "block_id": address["block_id"],
        "page_index": address["page_index"],
        "block_type": "system_graph",
        "bbox_visual_pt": list(address["bbox_visual_pt"]),
        "source": {
            "graph_schema_version": graph["schema_version"],
            "profile_id": address["profile_id"],
            "profile_version": graph["provenance"].get("profile_version"),
            "graph_provenance": copy.deepcopy(graph["provenance"]),
        },
    }


def adapt_system_graph_comparison_to_ledger(
    comparison_result: dict,
    left_graph: dict,
    right_graph: dict,
    *,
    confidence_policy: LedgerConfidencePolicy = MODE2_CONFIDENCE_POLICY_V1,
) -> dict:
    """Return one validated Mode 2 Ledger without re-running comparison logic."""
    comparison_validation = _validate_comparison(comparison_result)
    left_address = _validate_graph(left_graph, "left")
    right_address = _validate_graph(right_graph, "right")
    _validate_graph_link(
        comparison_result, left_graph, left_address, "left"
    )
    _validate_graph_link(
        comparison_result, right_graph, right_address, "right"
    )
    changes = [
        _map_change(
            change,
            left_graph,
            right_graph,
            left_address,
            right_address,
            confidence_policy,
        )
        for change in comparison_result["changes"]
    ]
    ledger = {
        "schema_version": MODE2_SCHEMA_VERSION,
        "comparison_scope": {
            "left_blocks": [_scope(left_graph, left_address)],
            "right_blocks": [_scope(right_graph, right_address)],
        },
        "route": "MODE_2_REQUIRED",
        "mode": "MODE_2",
        "policy": {
            "adapter": {
                "adapter_id": ADAPTER_ID,
                "adapter_version": ADAPTER_VERSION,
                "source_schema_version": comparison_result["schema_version"],
            },
            "confidence_mapping": confidence_policy.public_dict(),
        },
        "quality": {
            "comparison": copy.deepcopy(comparison_result.get("comparison_quality") or {}),
            "left_graph": copy.deepcopy(left_graph.get("quality") or {}),
            "right_graph": copy.deepcopy(right_graph.get("quality") or {}),
        },
        "changes": changes,
        "diagnostics": {
            "source_comparison_validation": comparison_validation,
            "source_comparison_provenance": copy.deepcopy(
                comparison_result.get("provenance") or {}
            ),
            "structural_status": {
                "overall": comparison_result.get("status"),
                "backbone": (comparison_result.get("backbone") or {}).get("status"),
                "functional_groups": (
                    comparison_result.get("functional_groups") or {}
                ).get("status"),
            },
            "source_summary": copy.deepcopy(comparison_result.get("summary") or {}),
        },
    }
    try:
        return validate_ledger(ledger)
    except LedgerValidationError as error:
        raise LedgerAdapterError(f"mapped ledger failed validation: {error}") from error


__all__ = [
    "ADAPTER_ID",
    "ADAPTER_VERSION",
    "LedgerAdapterError",
    "adapt_system_graph_comparison_to_ledger",
]
