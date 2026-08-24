"""Three-level, geometry-independent comparison of two SYSTEM_GRAPH objects.

The comparator consumes ready JSON-compatible graphs only.  It does not open a
PDF, inspect pixels, call an extractor, integrate with Stage Comparison, or
construct a GraphicChangeLedger.
"""
from __future__ import annotations

import collections
import hashlib
import json
from typing import Any, Iterable, Optional

from .graph_identity_matcher import (
    MATCHER_VERSION,
    functional_role,
    identity_values,
    match_graph_nodes,
    normalize_identity,
    section_identity,
)
from .system_graph import validate_system_graph


COMPARATOR_VERSION = "system-graph-comparator-v1"
COMPARISON_SCHEMA_VERSION = "system-graph-comparison.v1"

CHANGE_TYPES = frozenset(
    {
        "SYSTEM_BACKBONE_CHANGED",
        "FUNCTIONAL_GROUP_CHANGED",
        "NODE_ADDED",
        "NODE_REMOVED",
        "NODE_TYPE_CHANGED",
        "CONNECTION_CHANGED",
        "GROUP_COUNT_CHANGED",
        "DETAIL_LEVEL_INCREASED",
        "UNCERTAIN_STRUCTURAL_CHANGE",
    }
)
BACKBONE_STATUSES = frozenset(
    {"BACKBONE_PRESERVED", "BACKBONE_CHANGED", "UNCERTAIN_BACKBONE"}
)
FUNCTIONAL_GROUP_TYPES = frozenset(
    {"METERING_GROUP", "COMPENSATION_GROUP", "SERVICE_GROUP"}
)
_SOURCE_REPRESENTATION_RANK = {
    "UNKNOWN_SOURCE": 0,
    "EXTERNAL_FEEDER": 1,
    "UPSTREAM_TP_CONNECTION": 1,
    "TRANSFORMER_EXPLICIT": 2,
}


def _node_index(graph: dict) -> dict[str, dict]:
    return {
        str(node.get("id")): node
        for node in graph.get("nodes") or []
        if isinstance(node, dict) and node.get("id") is not None
    }


def _edge_index(graph: dict) -> dict[str, dict]:
    return {
        str(edge.get("id")): edge
        for edge in graph.get("edges") or []
        if isinstance(edge, dict) and edge.get("id") is not None
    }


def _unique_strings(values: Iterable[Any]) -> list[str]:
    output = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in output:
            output.append(text)
    return output


def _node_evidence(node: dict) -> dict:
    return {
        "node_id": str(node.get("id")),
        "node_type": str(node.get("type")),
        "canonical_identity": node.get("canonical_identity"),
        "label": node.get("label"),
        "confidence": float(node.get("confidence", 0.0) or 0.0),
        "source_tokens": _unique_strings(node.get("source_tokens") or []),
    }


def _edge_evidence(edge: dict) -> dict:
    return {
        "edge_id": str(edge.get("id")),
        "edge_type": str(edge.get("type")),
        "from": str(edge.get("from")),
        "to": str(edge.get("to")),
        "confidence": float(edge.get("confidence", 0.0) or 0.0),
        "source_tokens": _unique_strings(edge.get("source_tokens") or []),
    }


def _grounding(
    graph: dict,
    *,
    node_ids: Iterable[str] = (),
    edge_ids: Iterable[str] = (),
) -> dict:
    nodes, edges = _node_index(graph), _edge_index(graph)
    grounded_nodes = [
        _node_evidence(nodes[node_id])
        for node_id in _unique_strings(node_ids)
        if node_id in nodes
    ]
    grounded_edges = [
        _edge_evidence(edges[edge_id])
        for edge_id in _unique_strings(edge_ids)
        if edge_id in edges
    ]
    return {
        "block_id": str((graph.get("block") or {}).get("block_id") or ""),
        "node_ids": [item["node_id"] for item in grounded_nodes],
        "edge_ids": [item["edge_id"] for item in grounded_edges],
        "confidence": round(
            min(
                [item["confidence"] for item in grounded_nodes + grounded_edges]
                or [0.0]
            ),
            3,
        ),
        "source_tokens": _unique_strings(
            token
            for item in grounded_nodes + grounded_edges
            for token in item["source_tokens"]
        ),
        "nodes": grounded_nodes,
        "edges": grounded_edges,
    }


def _change_id(change_type: str, level: str, subject: Any, left_ids, right_ids) -> str:
    payload = json.dumps(
        [change_type, level, subject, sorted(left_ids), sorted(right_ids)],
        ensure_ascii=False,
        sort_keys=True,
    )
    return "chg_" + hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


class _Changes:
    def __init__(self, left: dict, right: dict):
        self.left = left
        self.right = right
        self.items: list[dict] = []

    def add(
        self,
        change_type: str,
        level: str,
        subject: Any,
        summary: str,
        *,
        left_nodes: Iterable[str] = (),
        right_nodes: Iterable[str] = (),
        left_edges: Iterable[str] = (),
        right_edges: Iterable[str] = (),
        confidence: float,
        reason: Optional[dict] = None,
    ) -> dict:
        if change_type not in CHANGE_TYPES:
            raise ValueError(f"unsupported change type: {change_type}")
        left_node_ids = _unique_strings(left_nodes)
        right_node_ids = _unique_strings(right_nodes)
        left_edge_ids = _unique_strings(left_edges)
        right_edge_ids = _unique_strings(right_edges)
        evidence = {
            "left": _grounding(
                self.left, node_ids=left_node_ids, edge_ids=left_edge_ids
            ),
            "right": _grounding(
                self.right, node_ids=right_node_ids, edge_ids=right_edge_ids
            ),
            "reason": reason or {},
        }
        item = {
            "change_id": _change_id(
                change_type, level, subject, left_node_ids, right_node_ids
            ),
            "type": change_type,
            "level": level,
            "subject": subject,
            "summary": summary,
            "confidence": round(max(0.0, min(1.0, float(confidence))), 3),
            "left_nodes": left_node_ids,
            "right_nodes": right_node_ids,
            "evidence": evidence,
        }
        self.items.append(item)
        return item


def _nodes_by_type(graph: dict, node_type: str) -> list[dict]:
    return [
        node
        for node in graph.get("nodes") or []
        if isinstance(node, dict) and node.get("type") == node_type
    ]


def _feed_edges(graph: dict) -> list[dict]:
    return [edge for edge in graph.get("edges") or [] if edge.get("type") == "FEEDS"]


def _source_to_section_paths(graph: dict) -> list[dict]:
    nodes = _node_index(graph)
    incoming = collections.defaultdict(list)
    for edge in _feed_edges(graph):
        incoming[str(edge.get("to"))].append(edge)
    paths = []
    for bus in _nodes_by_type(graph, "BUS_SECTION"):
        bus_id = str(bus["id"])
        inputs = [
            nodes.get(str(edge.get("from")))
            for edge in incoming.get(bus_id, [])
            if (nodes.get(str(edge.get("from"))) or {}).get("type") == "INPUT_DEVICE"
        ]
        for input_node in [node for node in inputs if node]:
            visited, queue = set(), [(str(input_node["id"]), [], [])]
            while queue:
                current, path_nodes, path_edges = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)
                node = nodes.get(current) or {}
                next_nodes = path_nodes + [current]
                if node.get("type") == "SOURCE":
                    paths.append(
                        {
                            "source_id": current,
                            "input_id": str(input_node["id"]),
                            "section_id": bus_id,
                            "node_ids": list(reversed(next_nodes)) + [bus_id],
                            "edge_ids": list(reversed(path_edges))
                            + [
                                str(edge["id"])
                                for edge in incoming.get(bus_id, [])
                                if str(edge.get("from")) == str(input_node["id"])
                            ],
                        }
                    )
                    continue
                for edge in incoming.get(current, []):
                    queue.append(
                        (
                            str(edge.get("from")),
                            next_nodes,
                            path_edges + [str(edge.get("id"))],
                        )
                    )
    unique = {path["source_id"] + "->" + path["section_id"]: path for path in paths}
    return list(unique.values())


def _backbone_snapshot(graph: dict) -> dict:
    sources = _nodes_by_type(graph, "SOURCE")
    sections = _nodes_by_type(graph, "BUS_SECTION")
    inputs = _nodes_by_type(graph, "INPUT_DEVICE")
    ties = _nodes_by_type(graph, "SECTION_DEVICE")
    tie_edges = [edge for edge in graph.get("edges") or [] if edge.get("type") == "TIES_SECTIONS"]
    paths = _source_to_section_paths(graph)
    node_index = _node_index(graph)
    path_signatures = []
    for path in paths:
        source = node_index.get(path["source_id"]) or {}
        section = node_index.get(path["section_id"]) or {}
        section_key = normalize_identity(
            section.get("canonical_identity") or section.get("label") or path["section_id"]
        )
        path_signatures.append(
            f"{source.get('source_role') or 'SUPPLY'}->{section_key}"
        )
    tie_pairs = []
    for tie in ties:
        endpoints = []
        for edge in tie_edges:
            if str(edge.get("from")) != str(tie.get("id")):
                continue
            section = node_index.get(str(edge.get("to"))) or {}
            endpoints.append(
                normalize_identity(
                    section.get("canonical_identity")
                    or section.get("label")
                    or edge.get("to")
                )
            )
        if endpoints:
            tie_pairs.append(tuple(sorted(endpoints)))
    quality = graph.get("quality") or {}
    confidence_values = [
        quality.get("source_confidence"),
        quality.get("bus_confidence"),
        quality.get("section_confidence"),
    ]
    numeric_confidence = [
        float(value) for value in confidence_values if isinstance(value, (int, float))
    ]
    if not numeric_confidence:
        numeric_confidence = [
            float(node.get("confidence", 0.0) or 0.0)
            for node in sources + sections + inputs + ties
        ]
    return {
        "counts": {
            "sources": len(sources),
            "inputs": len(inputs),
            "sections": len(sections),
            "section_devices": len(ties),
            "source_to_section_paths": len(paths),
            "tied_section_endpoints": len({str(edge.get("to")) for edge in tie_edges}),
        },
        "node_ids": [str(node["id"]) for node in sources + inputs + sections + ties],
        "edge_ids": [
            str(edge["id"])
            for edge in graph.get("edges") or []
            if edge.get("type") in {"FEEDS", "TIES_SECTIONS"}
            and (
                str(edge.get("from"))
                in {str(node["id"]) for node in sources + inputs + ties}
                or str(edge.get("to")) in {str(node["id"]) for node in sections + inputs}
            )
        ],
        "paths": paths,
        "functional_signature": {
            "sections": sorted(
                normalize_identity(node.get("canonical_identity") or node.get("label"))
                for node in sections
            ),
            "main_paths": sorted(path_signatures),
            "section_ties": sorted(tie_pairs),
        },
        "confidence": round(min(numeric_confidence or [0.0]), 3),
        "contract_valid": validate_system_graph(graph)["valid"],
    }


def _compare_backbone(left: dict, right: dict, changes: _Changes) -> dict:
    left_snapshot, right_snapshot = _backbone_snapshot(left), _backbone_snapshot(right)
    reliable = (
        left_snapshot["contract_valid"]
        and right_snapshot["contract_valid"]
        and min(left_snapshot["confidence"], right_snapshot["confidence"]) >= 0.5
    )
    preserved = (
        left_snapshot["counts"] == right_snapshot["counts"]
        and left_snapshot["functional_signature"]
        == right_snapshot["functional_signature"]
    )
    if not reliable:
        status = "UNCERTAIN_BACKBONE"
        changes.add(
            "UNCERTAIN_STRUCTURAL_CHANGE",
            "A",
            {"kind": "system_backbone"},
            "Достоверности входных графов недостаточно для вывода о сохранности backbone.",
            left_nodes=left_snapshot["node_ids"],
            right_nodes=right_snapshot["node_ids"],
            left_edges=left_snapshot["edge_ids"],
            right_edges=right_snapshot["edge_ids"],
            confidence=min(left_snapshot["confidence"], right_snapshot["confidence"]),
            reason={
                "left_contract_valid": left_snapshot["contract_valid"],
                "right_contract_valid": right_snapshot["contract_valid"],
                "minimum_backbone_confidence": 0.5,
            },
        )
    elif preserved:
        status = "BACKBONE_PRESERVED"
    else:
        status = "BACKBONE_CHANGED"
        changes.add(
            "SYSTEM_BACKBONE_CHANGED",
            "A",
            {"kind": "system_backbone"},
            "Основная структура питания изменилась: "
            f"{left_snapshot['counts']} → {right_snapshot['counts']}.",
            left_nodes=left_snapshot["node_ids"],
            right_nodes=right_snapshot["node_ids"],
            left_edges=left_snapshot["edge_ids"],
            right_edges=right_snapshot["edge_ids"],
            confidence=min(left_snapshot["confidence"], right_snapshot["confidence"]),
            reason={
                "left_counts": left_snapshot["counts"],
                "right_counts": right_snapshot["counts"],
                "left_signature": left_snapshot["functional_signature"],
                "right_signature": right_snapshot["functional_signature"],
            },
        )
    return {
        "status": status,
        "left": left_snapshot,
        "right": right_snapshot,
        "comparison_basis": [
            "source_count",
            "input_count",
            "bus_section_count",
            "source_to_section_paths",
            "section_tie_connectivity",
        ],
        "geometry_used": False,
    }


def _functional_anchor(node: dict) -> bool:
    node_type = str(node.get("type") or "")
    if node_type in {"METERING_GROUP", "COMPENSATION_GROUP"}:
        return True
    return node_type == "SERVICE_GROUP" and (node.get("attrs") or {}).get("member_count") is not None


def _functional_snapshot(graph: dict) -> dict:
    groups = collections.defaultdict(list)
    for node in graph.get("nodes") or []:
        if not isinstance(node, dict) or not _functional_anchor(node):
            continue
        groups[str(node["type"])].append(
            {
                "node_id": str(node["id"]),
                "section": section_identity(graph, node),
                "confidence": float(node.get("confidence", 0.0) or 0.0),
            }
        )
    reserve = [
        node
        for node in _nodes_by_type(graph, "OUTGOING_DEVICE")
        if str((node.get("attrs") or {}).get("status") or "") == "RESERVE"
    ]
    return {
        "groups": {
            key: sorted(
                value, key=lambda item: (item["section"] or "", item["node_id"])
            )
            for key, value in groups.items()
        },
        "reserve": {
            "count": len(reserve),
            "node_ids": [str(node["id"]) for node in reserve],
            "confidence": round(
                min([float(node.get("confidence", 0.0) or 0.0) for node in reserve] or [1.0]),
                3,
            ),
        },
        "functional_zones": sorted(
            normalize_identity(node.get("canonical_identity") or node.get("label"))
            for node in _nodes_by_type(graph, "BUS_SECTION")
        ),
    }


def _compare_functional_groups(left: dict, right: dict, changes: _Changes) -> dict:
    left_snapshot, right_snapshot = _functional_snapshot(left), _functional_snapshot(right)
    preserved, changed = [], []
    for group_type in sorted(FUNCTIONAL_GROUP_TYPES):
        left_groups = left_snapshot["groups"].get(group_type, [])
        right_groups = right_snapshot["groups"].get(group_type, [])
        left_sections = [item["section"] for item in left_groups]
        right_sections = [item["section"] for item in right_groups]
        if left_sections == right_sections:
            preserved.append(
                {
                    "function": group_type,
                    "sections": left_sections,
                    "labels_ignored": True,
                }
            )
            continue
        changed.append(group_type)
        changes.add(
            "FUNCTIONAL_GROUP_CHANGED",
            "B",
            {"kind": "functional_group", "function": group_type},
            f"Функциональная группа {group_type} изменилась: "
            f"{left_sections} → {right_sections}.",
            left_nodes=[item["node_id"] for item in left_groups],
            right_nodes=[item["node_id"] for item in right_groups],
            confidence=min(
                [item["confidence"] for item in left_groups + right_groups] or [0.0]
            ),
            reason={
                "left_sections": left_sections,
                "right_sections": right_sections,
                "label_changes_are_not_function_changes": True,
            },
        )

    left_reserve, right_reserve = left_snapshot["reserve"], right_snapshot["reserve"]
    uncertain = []
    if left_reserve["count"] != right_reserve["count"]:
        graph_quality = min(
            float((left.get("quality") or {}).get("identity_coverage", 0.0) or 0.0),
            float((right.get("quality") or {}).get("identity_coverage", 0.0) or 0.0),
        )
        reserve_confidence = min(
            left_reserve["confidence"], right_reserve["confidence"], graph_quality
        )
        if reserve_confidence >= 0.6:
            changed.append("RESERVE")
            change_type = "FUNCTIONAL_GROUP_CHANGED"
            summary = (
                "Количество явно обозначенных резервных отходящих изменилось: "
                f"{left_reserve['count']} → {right_reserve['count']}."
            )
        else:
            uncertain.append("RESERVE")
            change_type = "UNCERTAIN_STRUCTURAL_CHANGE"
            summary = (
                "Число распознанных резервных отходящих различается, но уверенности "
                "идентификации недостаточно для утверждения об изменении: "
                f"{left_reserve['count']} → {right_reserve['count']}."
            )
        changes.add(
            change_type,
            "B",
            {"kind": "reserve_function"},
            summary,
            left_nodes=left_reserve["node_ids"],
            right_nodes=right_reserve["node_ids"],
            confidence=reserve_confidence,
            reason={
                "left_count": left_reserve["count"],
                "right_count": right_reserve["count"],
                "absence_is_bounded_by_identity_coverage": graph_quality,
                "minimum_change_confidence": 0.6,
            },
        )
    else:
        preserved.append({"function": "RESERVE", "count": left_reserve["count"]})

    zones_preserved = left_snapshot["functional_zones"] == right_snapshot["functional_zones"]
    return {
        "status": (
            "FUNCTIONS_CHANGED"
            if changed
            else "FUNCTIONS_UNCERTAIN"
            if uncertain
            else "FUNCTIONS_PRESERVED"
        ),
        "preserved": preserved,
        "changed": changed,
        "uncertain": uncertain,
        "functional_zones_preserved": zones_preserved,
        "left": left_snapshot,
        "right": right_snapshot,
        "geometry_used": False,
    }


def _feed_path_from_source(graph: dict, source_id: str) -> dict:
    nodes = _node_index(graph)
    outgoing = collections.defaultdict(list)
    for edge in _feed_edges(graph):
        outgoing[str(edge.get("from"))].append(edge)
    queue = [(source_id, [source_id], [])]
    candidates = []
    while queue:
        current, node_ids, edge_ids = queue.pop(0)
        if len(node_ids) > 12:
            continue
        if current != source_id and (nodes.get(current) or {}).get("type") == "BUS_SECTION":
            candidates.append({"node_ids": node_ids, "edge_ids": edge_ids})
            continue
        for edge in outgoing.get(current, []):
            target = str(edge.get("to"))
            if target in node_ids:
                continue
            queue.append((target, node_ids + [target], edge_ids + [str(edge.get("id"))]))
    if not candidates:
        return {"node_ids": [source_id], "edge_ids": []}
    return min(candidates, key=lambda item: len(item["node_ids"]))


def _detail_level_pass(
    left: dict,
    right: dict,
    matching: dict,
    changes: _Changes,
) -> tuple[list[dict], set[str], set[str]]:
    left_nodes, right_nodes = _node_index(left), _node_index(right)
    detail_matches, consumed_left, consumed_right = [], set(), set()
    for match in matching["matches"]:
        left_node = left_nodes[match["left_id"]]
        right_node = right_nodes[match["right_id"]]
        if left_node.get("type") != "SOURCE" or right_node.get("type") != "SOURCE":
            continue
        left_path = _feed_path_from_source(left, match["left_id"])
        right_path = _feed_path_from_source(right, match["right_id"])
        left_rank = _SOURCE_REPRESENTATION_RANK.get(
            str(left_node.get("source_representation") or "UNKNOWN_SOURCE"), 0
        )
        right_rank = _SOURCE_REPRESENTATION_RANK.get(
            str(right_node.get("source_representation") or "UNKNOWN_SOURCE"), 0
        )
        matched_left_ids = {item["left_id"] for item in matching["matches"]}
        matched_right_ids = {item["right_id"] for item in matching["matches"]}
        left_expansion = [node_id for node_id in left_path["node_ids"][1:-1] if node_id not in matched_left_ids]
        right_expansion = [node_id for node_id in right_path["node_ids"][1:-1] if node_id not in matched_right_ids]
        increased = right_rank > left_rank or len(right_expansion) > len(left_expansion)
        if not increased:
            continue
        left_ids = left_path["node_ids"]
        right_ids = right_path["node_ids"]
        consumed_left.update(left_expansion)
        consumed_right.update(right_expansion)
        record = {
            "left_nodes": left_ids,
            "right_nodes": right_ids,
            "coarse_left_nodes": [match["left_id"]],
            "expanded_right_nodes": right_ids[:-1],
            "cardinality": "one_to_many" if len(right_ids[:-1]) > 1 else "one_to_one",
            "left_representation": left_node.get("source_representation"),
            "right_representation": right_node.get("source_representation"),
            "match_confidence": match["confidence"],
        }
        detail_matches.append(record)
        changes.add(
            "DETAIL_LEVEL_INCREASED",
            "A",
            {
                "kind": "source_path",
                "functional_role": functional_role(left, left_node),
            },
            "Источник показан подробнее без смены функционального пути: "
            f"{left_node.get('label') or left_node['id']} "
            f"({left_node.get('source_representation')}) → "
            f"{right_node.get('label') or right_node['id']} "
            f"({right_node.get('source_representation')}).",
            left_nodes=left_ids,
            right_nodes=right_ids,
            left_edges=left_path["edge_ids"],
            right_edges=right_path["edge_ids"],
            confidence=match["confidence"],
            reason={
                "classification": "coarse_node_to_expanded_subgraph",
                "left_representation_rank": left_rank,
                "right_representation_rank": right_rank,
                "not_node_added": True,
            },
        )
    return detail_matches, consumed_left, consumed_right


def _effective_node_type(node: dict) -> str:
    return str((node.get("attrs") or {}).get("type_candidate") or node.get("type") or "")


def _compare_matched_node_types(
    left: dict, right: dict, matching: dict, changes: _Changes
) -> None:
    left_nodes, right_nodes = _node_index(left), _node_index(right)
    for match in matching["matches"]:
        if float(match.get("confidence", 0.0)) < 0.68:
            continue
        left_node, right_node = left_nodes[match["left_id"]], right_nodes[match["right_id"]]
        left_type, right_type = _effective_node_type(left_node), _effective_node_type(right_node)
        if not left_type or not right_type or left_type == right_type:
            continue
        changes.add(
            "NODE_TYPE_CHANGED",
            "C",
            {
                "kind": "individual_node",
                "identity": sorted(identity_values(left_node) & identity_values(right_node)),
            },
            "Тип сопоставленного узла изменился: "
            f"{left_node.get('label') or left_node['id']} ({left_type}) → "
            f"{right_node.get('label') or right_node['id']} ({right_type}).",
            left_nodes=[match["left_id"]],
            right_nodes=[match["right_id"]],
            confidence=match["confidence"],
            reason={
                "left_effective_type": left_type,
                "right_effective_type": right_type,
                "identity_match_method": match["method"],
                "identity_signals": match["signals"],
            },
        )


def _edge_signature(edge: dict) -> tuple[str, str, str]:
    return str(edge.get("type")), str(edge.get("from")), str(edge.get("to"))


def _compare_connections(
    left: dict,
    right: dict,
    matching: dict,
    changes: _Changes,
    *,
    detail_matches: list[dict],
) -> None:
    mapping = {item["left_id"]: item["right_id"] for item in matching["matches"]}
    reverse_mapping = {right_id: left_id for left_id, right_id in mapping.items()}
    match_confidence = {item["left_id"]: item["confidence"] for item in matching["matches"]}
    right_edges = {
        _edge_signature(edge): edge for edge in right.get("edges") or [] if isinstance(edge, dict)
    }
    left_edges = {
        _edge_signature(edge): edge for edge in left.get("edges") or [] if isinstance(edge, dict)
    }
    left_by_pair = collections.defaultdict(list)
    for edge in left.get("edges") or []:
        if isinstance(edge, dict):
            left_by_pair[(str(edge.get("from")), str(edge.get("to")))].append(edge)
    right_by_pair = collections.defaultdict(list)
    for edge in right.get("edges") or []:
        if isinstance(edge, dict):
            right_by_pair[(str(edge.get("from")), str(edge.get("to")))].append(edge)
    seen_pairs = set()
    detail_left_pairs = set()
    for detail in detail_matches:
        node_ids = detail.get("left_nodes") or []
        detail_left_pairs.update(zip(node_ids, node_ids[1:]))
    detail_right_pairs = set()
    for detail in detail_matches:
        node_ids = detail.get("right_nodes") or []
        detail_right_pairs.update(zip(node_ids, node_ids[1:]))
    for left_edge in left.get("edges") or []:
        left_from, left_to = str(left_edge.get("from")), str(left_edge.get("to"))
        if left_from not in mapping or left_to not in mapping:
            continue
        if (left_from, left_to) in detail_left_pairs:
            continue
        right_from, right_to = mapping[left_from], mapping[left_to]
        expected = (str(left_edge.get("type")), right_from, right_to)
        if expected in right_edges:
            continue
        pair = (right_from, right_to)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        alternatives = right_by_pair.get(pair, [])
        right_edge_ids = [str(edge["id"]) for edge in alternatives]
        right_types = sorted({str(edge.get("type")) for edge in alternatives})
        changes.add(
            "CONNECTION_CHANGED",
            "C",
            {
                "kind": "matched_node_relation",
                "left_relation": left_edge.get("type"),
                "right_relations": right_types,
            },
            "Связь между сопоставленными узлами изменилась: "
            f"{left_edge.get('type')} → {right_types or ['ABSENT']}.",
            left_nodes=[left_from, left_to],
            right_nodes=[right_from, right_to],
            left_edges=[str(left_edge["id"])],
            right_edges=right_edge_ids,
            confidence=min(match_confidence[left_from], match_confidence[left_to]),
            reason={
                "endpoint_identity_is_matched": True,
                "left_edge_type": left_edge.get("type"),
                "right_edge_types": right_types,
            },
        )

    # Symmetric pass: a newly introduced relation between two already matched
    # functions is also a connection change.
    for right_edge in right.get("edges") or []:
        right_from, right_to = str(right_edge.get("from")), str(right_edge.get("to"))
        if right_from not in reverse_mapping or right_to not in reverse_mapping:
            continue
        if (right_from, right_to) in detail_right_pairs:
            continue
        left_from, left_to = reverse_mapping[right_from], reverse_mapping[right_to]
        expected = (str(right_edge.get("type")), left_from, left_to)
        if expected in left_edges or (right_from, right_to) in seen_pairs:
            continue
        seen_pairs.add((right_from, right_to))
        alternatives = left_by_pair.get((left_from, left_to), [])
        left_edge_ids = [str(edge["id"]) for edge in alternatives]
        left_types = sorted({str(edge.get("type")) for edge in alternatives})
        changes.add(
            "CONNECTION_CHANGED",
            "C",
            {
                "kind": "matched_node_relation",
                "left_relations": left_types,
                "right_relation": right_edge.get("type"),
            },
            "Связь между сопоставленными узлами изменилась: "
            f"{left_types or ['ABSENT']} → {right_edge.get('type')}.",
            left_nodes=[left_from, left_to],
            right_nodes=[right_from, right_to],
            left_edges=left_edge_ids,
            right_edges=[str(right_edge["id"])],
            confidence=min(match_confidence[left_from], match_confidence[left_to]),
            reason={
                "endpoint_identity_is_matched": True,
                "left_edge_types": left_types,
                "right_edge_type": right_edge.get("type"),
            },
        )


def _repeated_outgoing_scope(left: dict, right: dict, changes: _Changes) -> dict:
    left_nodes = _nodes_by_type(left, "OUTGOING_DEVICE")
    right_nodes = _nodes_by_type(right, "OUTGOING_DEVICE")
    repeated = min(len(left_nodes), len(right_nodes)) >= 3
    if repeated and len(left_nodes) != len(right_nodes):
        identity_confidence = min(
            float((left.get("quality") or {}).get("identity_coverage", 0.0) or 0.0),
            float((right.get("quality") or {}).get("identity_coverage", 0.0) or 0.0),
        )
        changes.add(
            "GROUP_COUNT_CHANGED",
            "C",
            {"kind": "repeated_node_group", "node_type": "OUTGOING_DEVICE"},
            "Количество отходящих аппаратов изменилось: "
            f"{len(left_nodes)} → {len(right_nodes)}.",
            left_nodes=[str(node["id"]) for node in left_nodes],
            right_nodes=[str(node["id"]) for node in right_nodes],
            confidence=identity_confidence,
            reason={
                "left_count": len(left_nodes),
                "right_count": len(right_nodes),
                "individual_unmatched_nodes_are_not_automatically_added_or_removed": True,
            },
        )
    return {
        "repeated": repeated,
        "left_count": len(left_nodes),
        "right_count": len(right_nodes),
        "left_ids": {str(node["id"]) for node in left_nodes},
        "right_ids": {str(node["id"]) for node in right_nodes},
    }


def _terminal_ids(graph: dict, outgoing_ids: set[str]) -> set[str]:
    return {
        str(edge.get("to"))
        for edge in graph.get("edges") or []
        if edge.get("type") == "TERMINATES_AT" and str(edge.get("from")) in outgoing_ids
    }


def _strong_individual_identity(node: dict) -> bool:
    return bool(identity_values(node)) and float(node.get("confidence", 0.0) or 0.0) >= 0.7


def _compare_unmatched_nodes(
    left: dict,
    right: dict,
    matching: dict,
    changes: _Changes,
    *,
    consumed_left: set[str],
    consumed_right: set[str],
    outgoing_scope: dict,
) -> None:
    left_nodes, right_nodes = _node_index(left), _node_index(right)
    managed_left, managed_right = set(consumed_left), set(consumed_right)
    if outgoing_scope["repeated"]:
        managed_left.update(outgoing_scope["left_ids"])
        managed_right.update(outgoing_scope["right_ids"])
        managed_left.update(_terminal_ids(left, outgoing_scope["left_ids"]))
        managed_right.update(_terminal_ids(right, outgoing_scope["right_ids"]))
    for node_id, node in left_nodes.items():
        if _functional_anchor(node):
            managed_left.add(node_id)
    for node_id, node in right_nodes.items():
        if _functional_anchor(node):
            managed_right.add(node_id)

    remaining_left = [
        node_id
        for node_id in matching["unmatched_left"]
        if node_id not in managed_left
    ]
    remaining_right = [
        node_id
        for node_id in matching["unmatched_right"]
        if node_id not in managed_right
    ]
    weak_left, weak_right = [], []
    for node_id in remaining_left:
        node = left_nodes[node_id]
        if _strong_individual_identity(node):
            changes.add(
                "NODE_REMOVED",
                "C",
                {"kind": "individual_node", "identity": sorted(identity_values(node))},
                f"Узел {node.get('label') or node_id} отсутствует справа.",
                left_nodes=[node_id],
                confidence=float(node.get("confidence", 0.0) or 0.0),
                reason={"strong_identity": True, "no_right_match": True},
            )
        else:
            weak_left.append(node_id)
    for node_id in remaining_right:
        node = right_nodes[node_id]
        if _strong_individual_identity(node):
            changes.add(
                "NODE_ADDED",
                "C",
                {"kind": "individual_node", "identity": sorted(identity_values(node))},
                f"Справа появился узел {node.get('label') or node_id}.",
                right_nodes=[node_id],
                confidence=float(node.get("confidence", 0.0) or 0.0),
                reason={"strong_identity": True, "no_left_match": True},
            )
        else:
            weak_right.append(node_id)

    repeated_unmatched_left = sorted(
        outgoing_scope["left_ids"] & set(matching["unmatched_left"])
    )
    repeated_unmatched_right = sorted(
        outgoing_scope["right_ids"] & set(matching["unmatched_right"])
    )
    uncertain_left = _unique_strings(weak_left + repeated_unmatched_left)
    uncertain_right = _unique_strings(weak_right + repeated_unmatched_right)
    if uncertain_left or uncertain_right:
        changes.add(
            "UNCERTAIN_STRUCTURAL_CHANGE",
            "C",
            {"kind": "unresolved_correspondence"},
            "Для части узлов соответствие недостаточно надёжно; "
            "удаление или добавление не утверждается.",
            left_nodes=uncertain_left,
            right_nodes=uncertain_right,
            confidence=min(
                float((left.get("quality") or {}).get("identity_coverage", 0.0) or 0.0),
                float((right.get("quality") or {}).get("identity_coverage", 0.0) or 0.0),
                0.49,
            ),
            reason={
                "ambiguous_pairs": matching["ambiguous"],
                "left_unresolved": len(uncertain_left),
                "right_unresolved": len(uncertain_right),
                "not_converted_to_removed_added": True,
            },
        )


def _graph_ref(graph: dict) -> dict:
    return {
        "schema_version": graph.get("schema_version"),
        "profile_id": graph.get("profile_id"),
        "block_id": str((graph.get("block") or {}).get("block_id") or ""),
        "page_index": (graph.get("block") or {}).get("page_index"),
        "validation": validate_system_graph(graph),
        "quality": graph.get("quality") or {},
    }


def validate_comparison_result(result: Any) -> dict:
    errors = []
    if not isinstance(result, dict):
        return {"valid": False, "errors": ["result_not_object"]}
    if result.get("schema_version") != COMPARISON_SCHEMA_VERSION:
        errors.append("schema_version_invalid")
    if (result.get("backbone") or {}).get("status") not in BACKBONE_STATUSES:
        errors.append("backbone_status_invalid")
    seen = set()
    for index, change in enumerate(result.get("changes") or []):
        prefix = f"change:{index}"
        if change.get("type") not in CHANGE_TYPES:
            errors.append(f"{prefix}:type_invalid")
        for field in (
            "change_id",
            "level",
            "subject",
            "summary",
            "confidence",
            "left_nodes",
            "right_nodes",
            "evidence",
        ):
            if field not in change:
                errors.append(f"{prefix}:missing_{field}")
        change_id = str(change.get("change_id") or "")
        if not change_id or change_id in seen:
            errors.append(f"{prefix}:change_id_invalid")
        seen.add(change_id)
        confidence = change.get("confidence")
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            errors.append(f"{prefix}:confidence_invalid")
        evidence = change.get("evidence") or {}
        if not isinstance(evidence, dict) or "left" not in evidence or "right" not in evidence:
            errors.append(f"{prefix}:evidence_invalid")
    provenance = result.get("provenance") or {}
    if provenance.get("bbox_identity") is not False:
        errors.append("bbox_identity_must_be_false")
    if provenance.get("manual_cases") is not False:
        errors.append("manual_cases_must_be_false")
    return {"valid": not errors, "errors": errors}


def compare_system_graphs(left: dict, right: dict) -> dict:
    """Compare two ready SYSTEM_GRAPH dictionaries without geometric identity."""
    changes = _Changes(left, right)
    matching = match_graph_nodes(left, right)
    backbone = _compare_backbone(left, right, changes)
    functional = _compare_functional_groups(left, right, changes)
    detail_matches, consumed_left, consumed_right = _detail_level_pass(
        left, right, matching, changes
    )
    matching["detail_matches"] = detail_matches
    _compare_matched_node_types(left, right, matching, changes)
    _compare_connections(
        left,
        right,
        matching,
        changes,
        detail_matches=detail_matches,
    )
    outgoing_scope = _repeated_outgoing_scope(left, right, changes)
    _compare_unmatched_nodes(
        left,
        right,
        matching,
        changes,
        consumed_left=consumed_left,
        consumed_right=consumed_right,
        outgoing_scope=outgoing_scope,
    )

    ordered = sorted(
        changes.items,
        key=lambda item: (
            {"A": 0, "B": 1, "C": 2}.get(item["level"], 9),
            item["type"],
            item["change_id"],
        ),
    )
    certain = [item for item in ordered if item["type"] != "UNCERTAIN_STRUCTURAL_CHANGE"]
    status = "CHANGED" if certain else "UNCERTAIN" if ordered else "NO_CHANGE"
    result = {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "status": status,
        "left_graph": _graph_ref(left),
        "right_graph": _graph_ref(right),
        "backbone": backbone,
        "functional_groups": functional,
        "matching": matching,
        "changes": ordered,
        "summary": {
            "changes_total": len(ordered),
            "certain_changes": len(certain),
            "uncertain_changes": len(ordered) - len(certain),
            "by_type": dict(collections.Counter(item["type"] for item in ordered)),
            "false_removed_added_suppressed": sum(
                item["type"] == "UNCERTAIN_STRUCTURAL_CHANGE" for item in ordered
            ),
        },
        "provenance": {
            "comparator_version": COMPARATOR_VERSION,
            "matcher_version": MATCHER_VERSION,
            "input_kind": "ready_system_graph_json",
            "bbox_identity": False,
            "geometry_identity_weight": 0.0,
            "manual_cases": False,
            "pdf_opened": False,
            "vision_used": False,
            "stage_comparison_integration": False,
            "graphic_change_ledger_integration": False,
        },
    }
    result["validation"] = validate_comparison_result(result)
    return result


__all__ = [
    "BACKBONE_STATUSES",
    "CHANGE_TYPES",
    "COMPARATOR_VERSION",
    "COMPARISON_SCHEMA_VERSION",
    "compare_system_graphs",
    "validate_comparison_result",
]
