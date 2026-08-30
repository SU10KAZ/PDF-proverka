"""Three-level, geometry-independent comparison of two SYSTEM_GRAPH objects.

The comparator consumes ready JSON-compatible graphs only.  It does not open a
PDF, inspect pixels, call an extractor, integrate with Stage Comparison, or
construct a GraphicChangeLedger.
"""
from __future__ import annotations

import collections
import hashlib
import json
from typing import Any, Iterable, Mapping, Optional

from backend.app.services.common import electrical_values

from .graph_identity_matcher import (
    MATCHER_VERSION,
    empty_matching_result,
    functional_role,
    identity_values,
    match_graph_nodes,
    normalize_identity,
    section_identity,
)
from .system_graph import validate_system_graph
from .system_graph_comparison_policy import (
    COMPARED_ATTRIBUTE_KEYS,
    DEFAULT_COMPARISON_POLICY,
    SystemGraphComparisonPolicy,
)


#: v3 сравнивает свойства надёжно сопоставленных узлов, а не только их тип.
COMPARATOR_VERSION = "system-graph-comparator-v3"
COMPARISON_SCHEMA_VERSION = "system-graph-comparison.v1"

CHANGE_TYPES = frozenset(
    {
        "SYSTEM_BACKBONE_CHANGED",
        "FUNCTIONAL_GROUP_CHANGED",
        "NODE_ADDED",
        "NODE_REMOVED",
        "NODE_TYPE_CHANGED",
        "NODE_PARAMETER_CHANGED",
        "CONNECTION_CHANGED",
        "GROUP_COUNT_CHANGED",
        "DETAIL_LEVEL_INCREASED",
        "UNCERTAIN_STRUCTURAL_CHANGE",
    }
)
BACKBONE_STATUSES = frozenset(
    {"BACKBONE_PRESERVED", "BACKBONE_CHANGED", "UNCERTAIN_BACKBONE"}
)
FUNCTIONAL_GROUP_TYPES = frozenset(DEFAULT_COMPARISON_POLICY.functional_group_types)


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


def _bounded_confidence(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value or 0.0)))
    except (TypeError, ValueError):
        return 0.0


def _node_evidence(node: dict) -> dict:
    return {
        "node_id": str(node.get("id")),
        "node_type": str(node.get("type")),
        "canonical_identity": node.get("canonical_identity"),
        "label": node.get("label"),
        "confidence": _bounded_confidence(node.get("confidence")),
        "source_tokens": _unique_strings(node.get("source_tokens") or []),
    }


def _edge_evidence(edge: dict) -> dict:
    return {
        "edge_id": str(edge.get("id")),
        "edge_type": str(edge.get("type")),
        "from": str(edge.get("from")),
        "to": str(edge.get("to")),
        "confidence": _bounded_confidence(edge.get("confidence")),
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
            "confidence": round(_bounded_confidence(confidence), 3),
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


def _confidence_stats(items: Iterable[dict]) -> dict:
    values = []
    for item in items:
        try:
            values.append(_bounded_confidence(item.get("confidence")))
        except AttributeError:
            values.append(0.0)
    return {
        "minimum": round(min(values or [1.0]), 3),
        "average": round(sum(values) / len(values), 3) if values else 1.0,
        "items": len(values),
        "below_0_5": sum(value < 0.5 for value in values),
    }


def _evidence_complete(graph: dict) -> bool:
    for item in list(graph.get("nodes") or []) + list(graph.get("edges") or []):
        if not isinstance(item, dict):
            return False
        if not isinstance(item.get("evidence"), list) or not item.get("evidence"):
            return False
        if not isinstance(item.get("source_tokens"), list) or not item.get("source_tokens"):
            return False
    return True


def _comparison_quality_precheck(
    left: dict,
    right: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict:
    left_validation = validate_system_graph(left)
    right_validation = validate_system_graph(right)
    left_coverage = _bounded_confidence(
        (left.get("quality") or {}).get("identity_coverage")
    )
    right_coverage = _bounded_confidence(
        (right.get("quality") or {}).get("identity_coverage")
    )
    left_node_confidence = _confidence_stats(left.get("nodes") or [])
    right_node_confidence = _confidence_stats(right.get("nodes") or [])
    left_edge_confidence = _confidence_stats(left.get("edges") or [])
    right_edge_confidence = _confidence_stats(right.get("edges") or [])
    left_evidence_complete = _evidence_complete(left)
    right_evidence_complete = _evidence_complete(right)
    reasons = []
    if not left_validation["valid"]:
        reasons.append("left_graph_invalid")
    if not right_validation["valid"]:
        reasons.append("right_graph_invalid")
    if left_coverage < comparison_policy.minimum_identity_coverage:
        reasons.append("left_identity_coverage_below_threshold")
    if right_coverage < comparison_policy.minimum_identity_coverage:
        reasons.append("right_identity_coverage_below_threshold")
    if left_node_confidence["average"] < comparison_policy.minimum_average_node_confidence:
        reasons.append("left_node_confidence_below_threshold")
    if right_node_confidence["average"] < comparison_policy.minimum_average_node_confidence:
        reasons.append("right_node_confidence_below_threshold")
    if left_edge_confidence["average"] < comparison_policy.minimum_average_edge_confidence:
        reasons.append("left_edge_confidence_below_threshold")
    if right_edge_confidence["average"] < comparison_policy.minimum_average_edge_confidence:
        reasons.append("right_edge_confidence_below_threshold")
    if not left_evidence_complete:
        reasons.append("left_evidence_incomplete")
    if not right_evidence_complete:
        reasons.append("right_evidence_incomplete")
    return {
        "left_graph_valid": left_validation["valid"],
        "right_graph_valid": right_validation["valid"],
        "left_identity_coverage": round(left_coverage, 3),
        "right_identity_coverage": round(right_coverage, 3),
        "left_node_confidence": left_node_confidence,
        "right_node_confidence": right_node_confidence,
        "left_edge_confidence": left_edge_confidence,
        "right_edge_confidence": right_edge_confidence,
        "left_evidence_complete": left_evidence_complete,
        "right_evidence_complete": right_evidence_complete,
        "blocked_changes_reason": reasons,
        "certain_changes_allowed": not reasons,
        "policy": comparison_policy.public_contract(),
    }


def _complete_comparison_quality(quality: dict, matching: dict) -> dict:
    return {
        **quality,
        "matched_nodes": int((matching.get("metrics") or {}).get("matched_pairs", 0)),
        "ambiguous_nodes": int(
            (matching.get("metrics") or {}).get("ambiguous_left_nodes", 0)
        ),
        "ambiguous_right_nodes": int(
            (matching.get("metrics") or {}).get("ambiguous_right_nodes", 0)
        ),
    }


def _feed_edges(
    graph: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> list[dict]:
    return [
        edge
        for edge in graph.get("edges") or []
        if edge.get("type") == comparison_policy.feed_edge_type
    ]


def _source_to_section_paths(
    graph: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> list[dict]:
    nodes = _node_index(graph)
    incoming = collections.defaultdict(list)
    for edge in _feed_edges(graph, comparison_policy):
        incoming[str(edge.get("to"))].append(edge)
    paths = []
    for bus in _nodes_by_type(graph, comparison_policy.section_node_type):
        bus_id = str(bus["id"])
        inputs = [
            nodes.get(str(edge.get("from")))
            for edge in incoming.get(bus_id, [])
            if (nodes.get(str(edge.get("from"))) or {}).get("type")
            == comparison_policy.input_node_type
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
                if node.get("type") == comparison_policy.source_node_type:
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


def _backbone_snapshot(
    graph: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict:
    sources = _nodes_by_type(graph, comparison_policy.source_node_type)
    sections = _nodes_by_type(graph, comparison_policy.section_node_type)
    inputs = _nodes_by_type(graph, comparison_policy.input_node_type)
    ties = _nodes_by_type(graph, comparison_policy.section_device_node_type)
    tie_edges = [
        edge
        for edge in graph.get("edges") or []
        if edge.get("type") == comparison_policy.tie_edge_type
    ]
    paths = _source_to_section_paths(graph, comparison_policy)
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
            _bounded_confidence(node.get("confidence"))
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
            if edge.get("type")
            in {comparison_policy.feed_edge_type, comparison_policy.tie_edge_type}
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


def _compare_backbone(
    left: dict,
    right: dict,
    changes: _Changes,
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict:
    left_snapshot = _backbone_snapshot(left, comparison_policy)
    right_snapshot = _backbone_snapshot(right, comparison_policy)
    reliable = (
        left_snapshot["contract_valid"]
        and right_snapshot["contract_valid"]
        and min(left_snapshot["confidence"], right_snapshot["confidence"])
        >= comparison_policy.certain_change_threshold
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
                "minimum_backbone_confidence": comparison_policy.certain_change_threshold,
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


def _functional_anchor(
    node: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> bool:
    return comparison_policy.functional_anchor(node)


def _functional_snapshot(
    graph: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict:
    groups = collections.defaultdict(list)
    for node in graph.get("nodes") or []:
        if not isinstance(node, dict) or not _functional_anchor(node, comparison_policy):
            continue
        groups[str(node["type"])].append(
            {
                "node_id": str(node["id"]),
                "section": section_identity(graph, node),
                "confidence": _bounded_confidence(node.get("confidence")),
            }
        )
    reserve = [
        node
        for node in _nodes_by_type(graph, comparison_policy.repeated_node_type)
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
                min([_bounded_confidence(node.get("confidence")) for node in reserve] or [1.0]),
                3,
            ),
        },
        "functional_zones": sorted(
            normalize_identity(node.get("canonical_identity") or node.get("label"))
            for node in _nodes_by_type(graph, comparison_policy.section_node_type)
        ),
    }


def _compare_functional_groups(
    left: dict,
    right: dict,
    changes: _Changes,
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict:
    left_snapshot = _functional_snapshot(left, comparison_policy)
    right_snapshot = _functional_snapshot(right, comparison_policy)
    preserved, changed, uncertain = [], [], []
    graph_quality = min(
        _bounded_confidence((left.get("quality") or {}).get("identity_coverage")),
        _bounded_confidence((right.get("quality") or {}).get("identity_coverage")),
    )
    for group_type in sorted(comparison_policy.functional_group_types):
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
        group_confidence = min(
            [item["confidence"] for item in left_groups + right_groups]
            + [graph_quality]
        )
        certain = group_confidence >= comparison_policy.certain_change_threshold
        if certain:
            changed.append(group_type)
            change_type = "FUNCTIONAL_GROUP_CHANGED"
            summary = (
                f"Функциональная группа {group_type} изменилась: "
                f"{left_sections} → {right_sections}."
            )
        else:
            uncertain.append(group_type)
            change_type = "UNCERTAIN_STRUCTURAL_CHANGE"
            summary = (
                f"Функциональная группа {group_type} различается, но качество "
                f"identity недостаточно для уверенного вывода: "
                f"{left_sections} → {right_sections}."
            )
        changes.add(
            change_type,
            "B",
            {"kind": "functional_group", "function": group_type},
            summary,
            left_nodes=[item["node_id"] for item in left_groups],
            right_nodes=[item["node_id"] for item in right_groups],
            confidence=group_confidence,
            reason={
                "left_sections": left_sections,
                "right_sections": right_sections,
                "label_changes_are_not_function_changes": True,
                "minimum_change_confidence": comparison_policy.certain_change_threshold,
            },
        )

    left_reserve, right_reserve = left_snapshot["reserve"], right_snapshot["reserve"]
    if left_reserve["count"] != right_reserve["count"]:
        reserve_confidence = min(
            left_reserve["confidence"], right_reserve["confidence"], graph_quality
        )
        if reserve_confidence >= comparison_policy.certain_change_threshold:
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
                "minimum_change_confidence": comparison_policy.certain_change_threshold,
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


def _feed_path_from_source(
    graph: dict,
    source_id: str,
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict:
    nodes = _node_index(graph)
    outgoing = collections.defaultdict(list)
    for edge in _feed_edges(graph, comparison_policy):
        outgoing[str(edge.get("from"))].append(edge)
    queue = [(source_id, [source_id], [])]
    candidates = []
    while queue:
        current, node_ids, edge_ids = queue.pop(0)
        if len(node_ids) > 12:
            continue
        if (
            current != source_id
            and (nodes.get(current) or {}).get("type")
            == comparison_policy.section_node_type
        ):
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
    comparison_policy: SystemGraphComparisonPolicy,
) -> tuple[list[dict], set[str], set[str], list[dict]]:
    left_nodes, right_nodes = _node_index(left), _node_index(right)
    left_edges, right_edges = _edge_index(left), _edge_index(right)
    detail_matches, consumed_left, consumed_right, rejections = [], set(), set(), []
    mapping = {item["left_id"]: item["right_id"] for item in matching["matches"]}
    for match in matching["matches"]:
        left_node = left_nodes[match["left_id"]]
        right_node = right_nodes[match["right_id"]]
        if (
            left_node.get("type") != comparison_policy.source_node_type
            or right_node.get("type") != comparison_policy.source_node_type
        ):
            continue
        left_path = _feed_path_from_source(left, match["left_id"], comparison_policy)
        right_path = _feed_path_from_source(right, match["right_id"], comparison_policy)
        left_representation = left_node.get("source_representation")
        right_representation = right_node.get("source_representation")
        left_rank = comparison_policy.representation_rank(left_representation)
        right_rank = comparison_policy.representation_rank(right_representation)
        matched_left_ids = {item["left_id"] for item in matching["matches"]}
        matched_right_ids = {item["right_id"] for item in matching["matches"]}
        left_expansion = [node_id for node_id in left_path["node_ids"][1:-1] if node_id not in matched_left_ids]
        right_expansion = [node_id for node_id in right_path["node_ids"][1:-1] if node_id not in matched_right_ids]
        transition_allowed = comparison_policy.detail_transition_allowed(
            left_representation, right_representation
        )
        increased = transition_allowed or len(right_expansion) > len(left_expansion)
        if not increased:
            continue
        boundary_preserved = (
            bool(left_path["node_ids"])
            and bool(right_path["node_ids"])
            and mapping.get(left_path["node_ids"][0]) == right_path["node_ids"][0]
            and mapping.get(left_path["node_ids"][-1]) == right_path["node_ids"][-1]
        )
        left_inputs = [
            node_id
            for node_id in left_path["node_ids"]
            if (left_nodes.get(node_id) or {}).get("type")
            == comparison_policy.input_node_type
        ]
        right_inputs = [
            node_id
            for node_id in right_path["node_ids"]
            if (right_nodes.get(node_id) or {}).get("type")
            == comparison_policy.input_node_type
        ]
        input_boundary_preserved = (
            len(left_inputs) == 1
            and len(right_inputs) == 1
            and mapping.get(left_inputs[0]) == right_inputs[0]
        )
        relations_preserved = all(
            (left_edges.get(edge_id) or {}).get("type")
            == comparison_policy.feed_edge_type
            for edge_id in left_path["edge_ids"]
        ) and all(
            (right_edges.get(edge_id) or {}).get("type")
            == comparison_policy.feed_edge_type
            for edge_id in right_path["edge_ids"]
        )
        unsafe_right_nodes = [
            node_id
            for node_id in right_expansion
            if not comparison_policy.representation_detail_node(right_nodes[node_id])
        ]
        semantic_equivalence = (
            boundary_preserved
            and input_boundary_preserved
            and relations_preserved
            and not unsafe_right_nodes
            and (transition_allowed or bool(right_expansion))
        )
        if not semantic_equivalence:
            rejections.append(
                {
                    "left_source": match["left_id"],
                    "right_source": match["right_id"],
                    "boundary_preserved": boundary_preserved,
                    "input_boundary_preserved": input_boundary_preserved,
                    "relations_preserved": relations_preserved,
                    "unsafe_right_nodes": unsafe_right_nodes,
                    "reason": "detail_equivalence_not_proven",
                }
            )
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
            "left_representation": left_representation,
            "right_representation": right_representation,
            "match_confidence": match["confidence"],
        }
        detail_matches.append(record)
        changes.add(
            "DETAIL_LEVEL_INCREASED",
            "A",
            {
                "kind": "source_path",
                "functional_role": functional_role(left, left_node, comparison_policy),
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
                "boundary_preserved": boundary_preserved,
                "input_boundary_preserved": input_boundary_preserved,
                "relations_preserved": relations_preserved,
                "unsafe_right_nodes": [],
                "not_node_added": True,
            },
        )
    return detail_matches, consumed_left, consumed_right, rejections


def _effective_node_type(node: dict) -> str:
    return str((node.get("attrs") or {}).get("type_candidate") or node.get("type") or "")


def _compare_matched_node_types(
    left: dict,
    right: dict,
    matching: dict,
    changes: _Changes,
    comparison_policy: SystemGraphComparisonPolicy,
) -> None:
    left_nodes, right_nodes = _node_index(left), _node_index(right)
    for match in matching["matches"]:
        if (
            match.get("decision") != "HIGH_MATCH"
            or float(match.get("confidence", 0.0))
            < comparison_policy.high_match_threshold
        ):
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


#: Свойства узла, о которых сравнение имеет право говорить, и как их называть.
#: Список ЗАКРЫТ. Всё, чего в нём нет — геометрия, служебные счётчики, окрестный
#: текст, — не свойство аппарата, и различие в нём ничего не значит для проекта.
_COMPARABLE_ATTRIBUTES = {
    "rating_a": {
        "facet_ref": "rated_current_a",
        "title": "Номинальный ток",
        "unit": "А",
        "kind": "NUMBER",
    },
    "status": {
        "facet_ref": "device_status",
        "title": "Состояние линии",
        "unit": None,
        "kind": "ENUM",
        # UNKNOWN означает «не прочитано», а не «состояние такое». Сравнивать
        # можно только два прочитанных состояния.
        "values": {"ACTIVE", "RESERVE"},
    },
}

_STATUS_TITLES = {"ACTIVE": "рабочая", "RESERVE": "резервная"}

# Тождество не имеет права опираться на свойства, о которых сравнение
# объявляет изменения, — иначе аппарат с изменившимся номиналом хуже
# опознаётся как тот же самый. Списки обязаны совпадать, поэтому расхождение
# ловится здесь, при импорте модуля, а не через полгода на боевом листе.
# `type_candidate` сравнивает _compare_matched_node_types, остальное — этот
# модуль.
assert COMPARED_ATTRIBUTE_KEYS == set(_COMPARABLE_ATTRIBUTES) | {"type_candidate"}, (
    "перечень сравниваемых свойств разошёлся с перечнем, исключённым из тождества"
)


def _attribute_pair(left_node: dict, right_node: dict, key: str) -> tuple[Any, Any] | None:
    """Значения свойства с обеих сторон — только если прочитаны обе.

    Непрочитанное свойство не сравнивается ни с чем: «слева номинала нет»
    доказывает, что мы его не прочитали, а не что его не стало.
    """
    before = (left_node.get("attrs") or {}).get(key)
    after = (right_node.get("attrs") or {}).get(key)
    if before is None or after is None:
        return None
    if isinstance(before, str) and not before.strip():
        return None
    if isinstance(after, str) and not after.strip():
        return None
    return before, after


def _single_cable(node: dict) -> Any | None:
    """Единственная марка кабеля узла.

    Две записи против одной — не изменение марки, а другой состав линии;
    сопоставлять их попарно нечем, и молчание здесь честнее догадки.
    """
    cables = (node.get("attrs") or {}).get("cables")
    if not isinstance(cables, list) or len(cables) != 1:
        return None
    return cables[0]


def _parameter_changes_for_pair(
    left_node: dict, right_node: dict
) -> list[dict[str, Any]]:
    """Доказанные различия свойств пары — по одному на свойство.

    Атомарность обязательна: номинал, отключающая способность и привод — три
    разных свойства одного аппарата, и склеивать их в одно изменение значит
    лишить инженера возможности согласиться с одним и оспорить другое.
    """
    output: list[dict[str, Any]] = []
    for key, spec in _COMPARABLE_ATTRIBUTES.items():
        pair = _attribute_pair(left_node, right_node, key)
        if pair is None:
            continue
        before, after = pair
        if spec["kind"] == "ENUM":
            allowed = spec.get("values") or set()
            if str(before) not in allowed or str(after) not in allowed:
                continue
            if str(before) == str(after):
                continue
            output.append({
                "facet_ref": spec["facet_ref"],
                "title": spec["title"],
                "unit": spec["unit"],
                "before": before,
                "after": after,
                "status": electrical_values.PROVEN,
                "before_label": _STATUS_TITLES.get(str(before), str(before)),
                "after_label": _STATUS_TITLES.get(str(after), str(after)),
            })
            continue
        numeric = electrical_values.numeric_change(before, after)
        if numeric is None:
            continue
        output.append({
            "facet_ref": spec["facet_ref"],
            "title": spec["title"],
            "unit": spec["unit"],
            "before": before,
            "after": after,
            "status": electrical_values.PROVEN,
            "direction": numeric["direction"],
        })

    left_cable, right_cable = _single_cable(left_node), _single_cable(right_node)
    if left_cable is not None and right_cable is not None:
        for item in electrical_values.compare_cables(left_cable, right_cable):
            output.append({
                "facet_ref": f"cable_{item['facet']}",
                "title": item["title"],
                "unit": "мм²" if item["facet"] == "section_mm2" else None,
                "before": item["before"],
                "after": item["after"],
                "status": item["status"],
                "left_raw": item["left_raw"],
                "right_raw": item["right_raw"],
            })
    return output


def _subject_name(left_node: dict, right_node: dict) -> str:
    """Как назвать аппарат, если слева и справа он обозначен по-разному.

    Молча взять левое обозначение нельзя: инженер пойдёт искать «1QF7» на
    правом листе и не найдёт его, потому что там этот же аппарат называется
    «1QF2». Обе стороны обязаны быть в тексте.
    """
    left_name = str(left_node.get("label") or left_node["id"])
    right_name = str(right_node.get("label") or right_node["id"])
    if left_name == right_name:
        return left_name
    return f"{left_name} → {right_name}"


def _parameter_summary(
    left_node: dict, right_node: dict, change: Mapping[str, Any]
) -> str:
    unit = f" {change['unit']}" if change.get("unit") else ""
    before = change.get("before_label", change["before"])
    after = change.get("after_label", change["after"])
    return (
        f"{_subject_name(left_node, right_node)}: {change['title'].lower()} "
        f"{before}{unit} → {after}{unit}."
    )


def _compare_matched_node_attributes(
    left: dict,
    right: dict,
    matching: dict,
    changes: _Changes,
    comparison_policy: SystemGraphComparisonPolicy,
) -> None:
    """Свойства надёжно сопоставленных узлов.

    Значения уже извлечены и привязаны к аппарату геометрией колонки — это не
    догадка сравнения, а прочитанный документ. Сравнение лишь спрашивает, стали
    ли два прочитанных значения разными, и говорит об этом только там, где узлы
    сопоставлены надёжно.
    """
    left_nodes, right_nodes = _node_index(left), _node_index(right)
    for match in matching["matches"]:
        if (
            match.get("decision") != "HIGH_MATCH"
            or float(match.get("confidence", 0.0))
            < comparison_policy.high_match_threshold
        ):
            continue
        left_node = left_nodes[match["left_id"]]
        right_node = right_nodes[match["right_id"]]
        for item in _parameter_changes_for_pair(left_node, right_node):
            proven = item["status"] == electrical_values.PROVEN
            changes.add(
                "NODE_PARAMETER_CHANGED" if proven else "UNCERTAIN_STRUCTURAL_CHANGE",
                "C",
                {
                    "kind": "individual_node" if proven else "unresolved_parameter",
                    "identity": sorted(
                        identity_values(left_node) & identity_values(right_node)
                    ),
                    "facet_ref": item["facet_ref"],
                },
                _parameter_summary(left_node, right_node, item)
                if proven
                else (
                    f"{_subject_name(left_node, right_node)}: "
                    f"{item['title'].lower()} читается как "
                    f"{item['before']} → {item['after']}, но одна из сторон не"
                    " объявила это значение явно."
                ),
                left_nodes=[match["left_id"]],
                right_nodes=[match["right_id"]],
                confidence=match["confidence"] if proven else 0.5,
                reason={
                    "facet_ref": item["facet_ref"],
                    "facet_title": item["title"],
                    "unit": item.get("unit"),
                    "left_value": item["before"],
                    "right_value": item["after"],
                    "value_status": item["status"],
                    "identity_match_method": match["method"],
                    "identity_signals": match["signals"],
                    **(
                        {"left_raw": item["left_raw"], "right_raw": item["right_raw"]}
                        if "left_raw" in item
                        else {}
                    ),
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


def _repeated_outgoing_scope(
    left: dict,
    right: dict,
    changes: _Changes,
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict:
    left_nodes = _nodes_by_type(left, comparison_policy.repeated_node_type)
    right_nodes = _nodes_by_type(right, comparison_policy.repeated_node_type)
    repeated = (
        min(len(left_nodes), len(right_nodes))
        >= comparison_policy.repeated_group_min_size
    )
    if repeated and len(left_nodes) != len(right_nodes):
        identity_confidence = min(
            _bounded_confidence((left.get("quality") or {}).get("identity_coverage")),
            _bounded_confidence((right.get("quality") or {}).get("identity_coverage")),
        )
        certain = identity_confidence >= comparison_policy.certain_change_threshold
        changes.add(
            "GROUP_COUNT_CHANGED" if certain else "UNCERTAIN_STRUCTURAL_CHANGE",
            "C",
            {
                "kind": "repeated_node_group",
                "node_type": comparison_policy.repeated_node_type,
            },
            (
                "Количество элементов повторяющейся группы изменилось: "
                if certain
                else "Количество распознанных элементов группы различается, "
                "но identity quality недостаточно для уверенного изменения: "
            )
            + f"{len(left_nodes)} → {len(right_nodes)}.",
            left_nodes=[str(node["id"]) for node in left_nodes],
            right_nodes=[str(node["id"]) for node in right_nodes],
            confidence=identity_confidence,
            reason={
                "left_count": len(left_nodes),
                "right_count": len(right_nodes),
                "individual_unmatched_nodes_are_not_automatically_added_or_removed": True,
                "minimum_change_confidence": comparison_policy.certain_change_threshold,
            },
        )
    return {
        "repeated": repeated,
        "left_count": len(left_nodes),
        "right_count": len(right_nodes),
        "left_ids": {str(node["id"]) for node in left_nodes},
        "right_ids": {str(node["id"]) for node in right_nodes},
    }


def _terminal_ids(
    graph: dict,
    outgoing_ids: set[str],
    comparison_policy: SystemGraphComparisonPolicy,
) -> set[str]:
    return {
        str(edge.get("to"))
        for edge in graph.get("edges") or []
        if edge.get("type") == comparison_policy.terminal_edge_type
        and str(edge.get("from")) in outgoing_ids
    }


def _strong_individual_identity(node: dict) -> bool:
    return bool(identity_values(node)) and _bounded_confidence(node.get("confidence")) >= 0.7


def _compare_unmatched_nodes(
    left: dict,
    right: dict,
    matching: dict,
    changes: _Changes,
    *,
    consumed_left: set[str],
    consumed_right: set[str],
    outgoing_scope: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> None:
    left_nodes, right_nodes = _node_index(left), _node_index(right)
    managed_left, managed_right = set(consumed_left), set(consumed_right)
    if outgoing_scope["repeated"]:
        managed_left.update(outgoing_scope["left_ids"])
        managed_right.update(outgoing_scope["right_ids"])
        managed_left.update(
            _terminal_ids(left, outgoing_scope["left_ids"], comparison_policy)
        )
        managed_right.update(
            _terminal_ids(right, outgoing_scope["right_ids"], comparison_policy)
        )
    for node_id, node in left_nodes.items():
        if _functional_anchor(node, comparison_policy):
            managed_left.add(node_id)
    for node_id, node in right_nodes.items():
        if _functional_anchor(node, comparison_policy):
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
    ambiguous_left = set(matching.get("ambiguous_left_ids") or [])
    ambiguous_right = set(matching.get("ambiguous_right_ids") or [])
    weak_left, weak_right = [], []
    for node_id in remaining_left:
        node = left_nodes[node_id]
        if _strong_individual_identity(node) and node_id not in ambiguous_left:
            changes.add(
                "NODE_REMOVED",
                "C",
                {"kind": "individual_node", "identity": sorted(identity_values(node))},
                f"Узел {node.get('label') or node_id} отсутствует справа.",
                left_nodes=[node_id],
                confidence=_bounded_confidence(node.get("confidence")),
                reason={"strong_identity": True, "no_right_match": True},
            )
        else:
            weak_left.append(node_id)
    for node_id in remaining_right:
        node = right_nodes[node_id]
        if _strong_individual_identity(node) and node_id not in ambiguous_right:
            changes.add(
                "NODE_ADDED",
                "C",
                {"kind": "individual_node", "identity": sorted(identity_values(node))},
                f"Справа появился узел {node.get('label') or node_id}.",
                right_nodes=[node_id],
                confidence=_bounded_confidence(node.get("confidence")),
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
                _bounded_confidence((left.get("quality") or {}).get("identity_coverage")),
                _bounded_confidence((right.get("quality") or {}).get("identity_coverage")),
                0.49,
            ),
            reason={
                "ambiguous_pairs": matching["ambiguous"],
                "left_unresolved": len(uncertain_left),
                "right_unresolved": len(uncertain_right),
                "not_converted_to_removed_added": True,
            },
        )


def _fail_closed_levels(
    left: dict,
    right: dict,
    changes: _Changes,
    comparison_quality: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> tuple[dict, dict]:
    left_backbone = _backbone_snapshot(left, comparison_policy)
    right_backbone = _backbone_snapshot(right, comparison_policy)
    left_functional = _functional_snapshot(left, comparison_policy)
    right_functional = _functional_snapshot(right, comparison_policy)
    left_ids = [
        str(node.get("id"))
        for node in left.get("nodes") or []
        if isinstance(node, dict) and node.get("id") is not None
    ]
    right_ids = [
        str(node.get("id"))
        for node in right.get("nodes") or []
        if isinstance(node, dict) and node.get("id") is not None
    ]
    left_edge_ids = [
        str(edge.get("id"))
        for edge in left.get("edges") or []
        if isinstance(edge, dict) and edge.get("id") is not None
    ]
    right_edge_ids = [
        str(edge.get("id"))
        for edge in right.get("edges") or []
        if isinstance(edge, dict) and edge.get("id") is not None
    ]
    quality_confidence = min(
        comparison_quality["left_identity_coverage"],
        comparison_quality["right_identity_coverage"],
        comparison_quality["left_node_confidence"]["average"],
        comparison_quality["right_node_confidence"]["average"],
        comparison_quality["left_edge_confidence"]["average"],
        comparison_quality["right_edge_confidence"]["average"],
    )
    changes.add(
        "UNCERTAIN_STRUCTURAL_CHANGE",
        "A",
        {"kind": "comparison_quality_gate"},
        "Качество входных SYSTEM_GRAPH недостаточно для уверенных структурных изменений.",
        left_nodes=left_ids,
        right_nodes=right_ids,
        left_edges=left_edge_ids,
        right_edges=right_edge_ids,
        confidence=quality_confidence,
        reason={
            "blocked_changes_reason": comparison_quality["blocked_changes_reason"],
            "certain_changes_suppressed": True,
        },
    )
    backbone = {
        "status": "UNCERTAIN_BACKBONE",
        "left": left_backbone,
        "right": right_backbone,
        "comparison_basis": [
            "source_count",
            "input_count",
            "section_count",
            "source_to_section_paths",
            "section_link_connectivity",
        ],
        "geometry_used": False,
        "blocked_by_quality_gate": True,
    }
    functional = {
        "status": "FUNCTIONS_UNCERTAIN",
        "preserved": [],
        "changed": [],
        "uncertain": ["comparison_quality_gate"],
        "functional_zones_preserved": None,
        "left": left_functional,
        "right": right_functional,
        "geometry_used": False,
        "blocked_by_quality_gate": True,
    }
    return backbone, functional


#: Виды внутренних противоречий одного листа, которые вектор-слой доказывает
#: сам, без всякого сравнения редакций.
INCONSISTENCY_KINDS = frozenset({"SECTION_LABEL_CONFLICT"})


def _inconsistency_id(side: str, kind: str, payload: Any) -> str:
    encoded = json.dumps([side, kind, payload], ensure_ascii=False, sort_keys=True)
    return "dinc_" + hashlib.sha1(encoded.encode("utf-8")).hexdigest()[:12]


def _side_inconsistencies(graph: dict, side: str) -> list[dict]:
    """Противоречия внутри ОДНОГО листа. Это не расхождение редакций.

    «1QF1 стоит во второй секции» — ошибка самого чертежа: обозначение
    относит аппарат к первой секции, а геометрия шин — ко второй. Такое
    нельзя показывать как изменение «было → стало»: слева этого аппарата в
    таком виде не было вовсе, и любая пара значений здесь была бы выдумана.

    Поэтому у находки нет ни второй стороны, ни «до» и «после» — только
    сторона, лист, подпись и рамка, по которой это видно.
    """
    analysis = graph.get("analysis")
    if not isinstance(analysis, dict):
        return []
    block_id = str((graph.get("block") or {}).get("block_id") or "")
    output = []
    for conflict in analysis.get("section_label_conflicts") or []:
        if not isinstance(conflict, dict):
            continue
        label = str(conflict.get("label") or "")
        label_section = str(conflict.get("label_section") or "")
        geometric_section = str(conflict.get("geometric_section") or "")
        if not label or not label_section or not geometric_section:
            continue
        output.append({
            "inconsistency_id": _inconsistency_id(
                side, "SECTION_LABEL_CONFLICT", [block_id, label, label_section, geometric_section]
            ),
            "kind": "SECTION_LABEL_CONFLICT",
            "side": side,
            "block_id": block_id,
            "subject": label,
            "summary": (
                f"Аппарат {label} обозначен как относящийся к секции"
                f" {label_section}, но геометрически стоит в секции"
                f" {geometric_section}."
            ),
            "evidence": {
                "bbox": conflict.get("bbox"),
                "label_section": label_section,
                "geometric_section": geometric_section,
                "reason": conflict.get("reason"),
            },
        })
    return sorted(output, key=lambda item: item["inconsistency_id"])


def document_inconsistencies(left: dict, right: dict) -> list[dict]:
    """Внутренние противоречия обоих листов, каждое со своей стороной."""
    return _side_inconsistencies(left, "LEFT") + _side_inconsistencies(right, "RIGHT")


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
    comparison_quality = result.get("comparison_quality") or {}
    for field in (
        "left_graph_valid",
        "right_graph_valid",
        "left_identity_coverage",
        "right_identity_coverage",
        "matched_nodes",
        "ambiguous_nodes",
        "blocked_changes_reason",
        "certain_changes_allowed",
    ):
        if field not in comparison_quality:
            errors.append(f"comparison_quality:missing_{field}")
    if comparison_quality.get("blocked_changes_reason"):
        certain_types = [
            change.get("type")
            for change in result.get("changes") or []
            if change.get("type") != "UNCERTAIN_STRUCTURAL_CHANGE"
        ]
        if certain_types:
            errors.append("comparison_quality:certain_change_not_fail_closed")
    return {"valid": not errors, "errors": errors}


def compare_system_graphs(
    left: dict,
    right: dict,
    comparison_policy: SystemGraphComparisonPolicy = DEFAULT_COMPARISON_POLICY,
) -> dict:
    """Compare two ready SYSTEM_GRAPH dictionaries without geometric identity."""
    changes = _Changes(left, right)
    quality_precheck = _comparison_quality_precheck(left, right, comparison_policy)
    if quality_precheck["left_graph_valid"] and quality_precheck["right_graph_valid"]:
        matching = match_graph_nodes(left, right, comparison_policy)
    else:
        matching = empty_matching_result(left, right)
    comparison_quality = _complete_comparison_quality(quality_precheck, matching)

    if comparison_quality["blocked_changes_reason"]:
        backbone, functional = _fail_closed_levels(
            left,
            right,
            changes,
            comparison_quality,
            comparison_policy,
        )
        matching["detail_matches"] = []
        matching["detail_rejections"] = []
    else:
        backbone = _compare_backbone(left, right, changes, comparison_policy)
        functional = _compare_functional_groups(
            left, right, changes, comparison_policy
        )
        detail_matches, consumed_left, consumed_right, detail_rejections = (
            _detail_level_pass(
                left,
                right,
                matching,
                changes,
                comparison_policy,
            )
        )
        matching["detail_matches"] = detail_matches
        matching["detail_rejections"] = detail_rejections
        _compare_matched_node_types(
            left, right, matching, changes, comparison_policy
        )
        _compare_matched_node_attributes(
            left, right, matching, changes, comparison_policy
        )
        _compare_connections(
            left,
            right,
            matching,
            changes,
            detail_matches=detail_matches,
        )
        outgoing_scope = _repeated_outgoing_scope(
            left, right, changes, comparison_policy
        )
        _compare_unmatched_nodes(
            left,
            right,
            matching,
            changes,
            consumed_left=consumed_left,
            consumed_right=consumed_right,
            outgoing_scope=outgoing_scope,
            comparison_policy=comparison_policy,
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
        "comparison_quality": comparison_quality,
        "changes": ordered,
        # Отдельный список, а НЕ ещё одна запись в changes: у внутреннего
        # противоречия нет второй стороны, и попав в изменения оно
        # подделало бы «было → стало».
        "document_inconsistencies": document_inconsistencies(left, right),
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
            "comparison_policy_id": comparison_policy.policy_id,
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
    "INCONSISTENCY_KINDS",
    "document_inconsistencies",
    "COMPARATOR_VERSION",
    "COMPARISON_SCHEMA_VERSION",
    "DEFAULT_COMPARISON_POLICY",
    "SystemGraphComparisonPolicy",
    "compare_system_graphs",
    "validate_comparison_result",
]
