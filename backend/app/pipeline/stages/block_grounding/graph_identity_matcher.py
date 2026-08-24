"""Functional identity matching for two grounded SYSTEM_GRAPH objects.

The matcher deliberately treats geometry as navigation evidence, not identity.
It ranks canonical identity, functional role, labels, relations and stable
attributes.  ``bbox`` and drawing coordinates are never used to accept a pair.
"""
from __future__ import annotations

import collections
import math
import re
from typing import Any, Optional


MATCHER_VERSION = "graph-identity-matcher-v1"
STRONG_MATCH_THRESHOLD = 0.68
CANONICAL_MATCH_THRESHOLD = 0.55
AMBIGUOUS_MATCH_THRESHOLD = 0.38

_NON_UNIQUE_ROLE_TYPES = frozenset(
    {"OUTGOING_DEVICE", "LOAD", "UNKNOWN_NODE"}
)
_GEOMETRY_ATTRIBUTE_KEYS = frozenset(
    {"bbox", "column", "connected_geometry", "geometry", "x", "x_range", "y"}
)
_VOLATILE_ATTRIBUTE_KEYS = frozenset(
    {"device_count", "member_count", "nearby_text"}
)


def normalize_identity(value: Any) -> str:
    text = str(value or "").strip().upper().replace("Ё", "Е")
    text = text.replace("–", "-").replace("—", "-").replace("_", "-")
    return re.sub(r"\s+", "", text)


def _node_confidence(node: dict) -> float:
    try:
        return max(0.0, min(1.0, float(node.get("confidence", 0.0))))
    except (TypeError, ValueError):
        return 0.0


def _node_index(graph: dict) -> dict[str, dict]:
    return {
        str(node.get("id")): node
        for node in graph.get("nodes") or []
        if isinstance(node, dict) and node.get("id") is not None
    }


def _section_reference(node: dict) -> Optional[str]:
    section = node.get("section")
    if section is None:
        section = (node.get("attrs") or {}).get("section")
    return str(section) if section is not None else None


def section_identity(graph: dict, node: dict) -> Optional[str]:
    section = _section_reference(node)
    if not section:
        return None
    section_node = _node_index(graph).get(section)
    if not section_node:
        return normalize_identity(section)
    return normalize_identity(
        section_node.get("canonical_identity") or section_node.get("label") or section
    )


def identity_values(node: dict) -> set[str]:
    values = set()
    canonical = normalize_identity(node.get("canonical_identity"))
    if canonical:
        values.add(canonical)
    for value in (node.get("attrs") or {}).get("identity_set") or []:
        normalized = normalize_identity(value)
        if normalized:
            values.add(normalized)
    return values


def label_values(node: dict) -> set[str]:
    values = set()
    for field in ("label", "display_label"):
        normalized = normalize_identity(node.get(field))
        if normalized:
            values.add(normalized)
    return values


def relation_signature(graph: dict, node_id: str) -> set[tuple[str, str, str]]:
    index = _node_index(graph)
    signature = set()
    for edge in graph.get("edges") or []:
        if not isinstance(edge, dict):
            continue
        if str(edge.get("from")) == node_id:
            neighbour = index.get(str(edge.get("to"))) or {}
            signature.add((str(edge.get("type")), "OUT", str(neighbour.get("type") or "")))
        if str(edge.get("to")) == node_id:
            neighbour = index.get(str(edge.get("from"))) or {}
            signature.add((str(edge.get("type")), "IN", str(neighbour.get("type") or "")))
    return signature


def _tie_sections(graph: dict, node: dict) -> tuple[str, ...]:
    index = _node_index(graph)
    values = []
    for edge in graph.get("edges") or []:
        if edge.get("type") != "TIES_SECTIONS" or str(edge.get("from")) != str(node.get("id")):
            continue
        target = index.get(str(edge.get("to"))) or {}
        identity = normalize_identity(
            target.get("canonical_identity") or target.get("label") or edge.get("to")
        )
        if identity:
            values.append(identity)
    return tuple(sorted(values))


def functional_role(graph: dict, node: dict) -> str:
    node_type = str(node.get("type") or "UNKNOWN_NODE")
    section = section_identity(graph, node) or "NO_SECTION"
    attrs = node.get("attrs") or {}
    if node_type == "SOURCE":
        return f"SOURCE:{node.get('source_role') or 'SUPPLY'}:{section}"
    if node_type == "INPUT_DEVICE":
        return f"INPUT_DEVICE:{section}"
    if node_type == "BUS_SECTION":
        identity = normalize_identity(node.get("canonical_identity") or node.get("label"))
        return f"BUS_SECTION:{identity or 'SECTION'}"
    if node_type == "SECTION_DEVICE":
        return "SECTION_DEVICE:" + ",".join(_tie_sections(graph, node))
    if node_type in {"METERING_GROUP", "COMPENSATION_GROUP"}:
        return f"{node_type}:{section}"
    if node_type == "SERVICE_GROUP":
        subclass = normalize_identity(attrs.get("subclass") or attrs.get("type_candidate"))
        if attrs.get("member_count") is not None:
            subclass = "FUNCTIONAL_SERVICE"
        return f"SERVICE_GROUP:{subclass or 'SERVICE'}:{section}"
    if node_type == "OUTGOING_DEVICE":
        return f"OUTGOING_DEVICE:{section}"
    if node_type == "LOAD":
        return f"LOAD:{section}"
    return f"{node_type}:{section}"


def _stable_attributes(node: dict) -> dict[str, str]:
    output = {}
    for key, value in sorted((node.get("attrs") or {}).items()):
        if key in _GEOMETRY_ATTRIBUTE_KEYS or key in _VOLATILE_ATTRIBUTE_KEYS:
            continue
        if value is None or isinstance(value, (dict, list, tuple, set)):
            continue
        output[str(key)] = normalize_identity(value)
    return output


def _jaccard(left: set, right: set) -> float:
    if not left and not right:
        return 1.0
    return len(left & right) / max(len(left | right), 1)


def score_node_pair(left_graph: dict, right_graph: dict, left: dict, right: dict) -> dict:
    """Return a transparent identity score; geometry is diagnostic-only."""
    score = 0.0
    signals = []
    left_identity, right_identity = identity_values(left), identity_values(right)
    canonical_overlap = sorted(left_identity & right_identity)
    if canonical_overlap:
        score += 0.62
        signals.append({"signal": "canonical_identity", "weight": 0.62, "values": canonical_overlap})

    left_role = functional_role(left_graph, left)
    right_role = functional_role(right_graph, right)
    if str(left.get("type")) == str(right.get("type")):
        score += 0.16
        signals.append({"signal": "node_type", "weight": 0.16, "value": left.get("type")})
    if left_role == right_role:
        score += 0.10
        signals.append({"signal": "functional_role", "weight": 0.10, "value": left_role})

    labels = sorted(label_values(left) & label_values(right))
    if labels:
        score += 0.06
        signals.append({"signal": "label", "weight": 0.06, "values": labels})

    left_relations = relation_signature(left_graph, str(left.get("id")))
    right_relations = relation_signature(right_graph, str(right.get("id")))
    relation_similarity = _jaccard(left_relations, right_relations)
    if relation_similarity:
        weight = round(0.04 * relation_similarity, 4)
        score += weight
        signals.append(
            {"signal": "relations", "weight": weight, "similarity": round(relation_similarity, 3)}
        )

    left_attrs, right_attrs = _stable_attributes(left), _stable_attributes(right)
    common_keys = set(left_attrs) & set(right_attrs)
    attribute_similarity = (
        sum(left_attrs[key] == right_attrs[key] for key in common_keys) / len(common_keys)
        if common_keys
        else 0.0
    )
    if attribute_similarity:
        weight = round(0.02 * attribute_similarity, 4)
        score += weight
        signals.append(
            {"signal": "attributes", "weight": weight, "similarity": round(attribute_similarity, 3)}
        )

    evidence_confidence = math.sqrt(_node_confidence(left) * _node_confidence(right))
    confidence = round(min(1.0, score, evidence_confidence), 3)
    return {
        "score": round(min(score, 1.0), 3),
        "confidence": confidence,
        "canonical_overlap": canonical_overlap,
        "functional_role_equal": left_role == right_role,
        "signals": signals,
        "geometry": {"used_for_identity": False, "weight": 0.0},
    }


def _record_match(left: dict, right: dict, assessment: dict, method: str) -> dict:
    return {
        "left_id": str(left["id"]),
        "right_id": str(right["id"]),
        "confidence": assessment["confidence"],
        "method": method,
        "signals": assessment["signals"],
        "geometry": assessment["geometry"],
    }


def _terminal_target(graph: dict, outgoing_id: str) -> Optional[str]:
    for edge in graph.get("edges") or []:
        if (
            edge.get("type") == "TERMINATES_AT"
            and str(edge.get("from")) == outgoing_id
        ):
            return str(edge.get("to"))
    return None


def _align_terminals_from_matched_parents(
    left_graph: dict,
    right_graph: dict,
    matches: list[dict],
    unused_left: dict[str, dict],
    unused_right: dict[str, dict],
) -> None:
    """Resolve duplicate LOAD identities through already matched feeder relations."""
    left_nodes, right_nodes = _node_index(left_graph), _node_index(right_graph)
    outgoing_matches = [
        item
        for item in matches
        if (left_nodes.get(item["left_id"]) or {}).get("type") == "OUTGOING_DEVICE"
        and (right_nodes.get(item["right_id"]) or {}).get("type") == "OUTGOING_DEVICE"
    ]
    terminal_ids = set()
    for item in outgoing_matches:
        left_terminal = _terminal_target(left_graph, item["left_id"])
        right_terminal = _terminal_target(right_graph, item["right_id"])
        if left_terminal:
            terminal_ids.add(("left", left_terminal))
        if right_terminal:
            terminal_ids.add(("right", right_terminal))

    retained = []
    for item in matches:
        if (
            ("left", item["left_id"]) in terminal_ids
            or ("right", item["right_id"]) in terminal_ids
        ):
            if item["left_id"] in left_nodes:
                unused_left[item["left_id"]] = left_nodes[item["left_id"]]
            if item["right_id"] in right_nodes:
                unused_right[item["right_id"]] = right_nodes[item["right_id"]]
            continue
        retained.append(item)
    matches[:] = retained

    for parent_match in outgoing_matches:
        left_terminal = _terminal_target(left_graph, parent_match["left_id"])
        right_terminal = _terminal_target(right_graph, parent_match["right_id"])
        if not left_terminal or not right_terminal:
            continue
        if left_terminal not in unused_left or right_terminal not in unused_right:
            continue
        left_node, right_node = unused_left[left_terminal], unused_right[right_terminal]
        assessment = score_node_pair(
            left_graph, right_graph, left_node, right_node
        )
        assessment = {
            **assessment,
            "confidence": round(
                min(
                    parent_match["confidence"],
                    _node_confidence(left_node),
                    _node_confidence(right_node),
                ),
                3,
            ),
            "signals": assessment["signals"]
            + [
                {
                    "signal": "matched_parent_relation",
                    "weight": 0.0,
                    "left_parent": parent_match["left_id"],
                    "right_parent": parent_match["right_id"],
                }
            ],
        }
        matches.append(
            _record_match(
                left_node,
                right_node,
                assessment,
                "matched_parent_relation",
            )
        )
        unused_left.pop(left_terminal)
        unused_right.pop(right_terminal)


def match_graph_nodes(left_graph: dict, right_graph: dict) -> dict:
    """Match one-to-one identities and retain weak alternatives as ambiguity."""
    left_nodes = [node for node in left_graph.get("nodes") or [] if isinstance(node, dict)]
    right_nodes = [node for node in right_graph.get("nodes") or [] if isinstance(node, dict)]
    unused_left = {str(node["id"]): node for node in left_nodes}
    unused_right = {str(node["id"]): node for node in right_nodes}
    matches = []

    # Priority 1: canonical identity / declared identity aliases.
    canonical_candidates = []
    for left in left_nodes:
        if not identity_values(left):
            continue
        for right in right_nodes:
            if not identity_values(left) & identity_values(right):
                continue
            assessment = score_node_pair(left_graph, right_graph, left, right)
            canonical_candidates.append((assessment["confidence"], assessment["score"], left, right, assessment))
    for _, _, left, right, assessment in sorted(
        canonical_candidates, key=lambda item: (item[0], item[1]), reverse=True
    ):
        left_id, right_id = str(left["id"]), str(right["id"])
        if left_id not in unused_left or right_id not in unused_right:
            continue
        if assessment["confidence"] < CANONICAL_MATCH_THRESHOLD:
            continue
        matches.append(_record_match(left, right, assessment, "canonical_identity"))
        unused_left.pop(left_id)
        unused_right.pop(right_id)

    # LOAD identities are often duplicated across several feeders.  Their
    # functional parent relation disambiguates them without using page order.
    _align_terminals_from_matched_parents(
        left_graph, right_graph, matches, unused_left, unused_right
    )

    # Priority 2: unique functional role. Generic repeated feeder roles are not unique identity.
    left_roles = collections.defaultdict(list)
    right_roles = collections.defaultdict(list)
    for node in unused_left.values():
        left_roles[functional_role(left_graph, node)].append(node)
    for node in unused_right.values():
        right_roles[functional_role(right_graph, node)].append(node)
    for role in sorted(set(left_roles) & set(right_roles)):
        left_group, right_group = left_roles[role], right_roles[role]
        if len(left_group) != 1 or len(right_group) != 1:
            continue
        left, right = left_group[0], right_group[0]
        if left.get("type") in _NON_UNIQUE_ROLE_TYPES or right.get("type") in _NON_UNIQUE_ROLE_TYPES:
            continue
        assessment = score_node_pair(left_graph, right_graph, left, right)
        functional_confidence = min(
            _node_confidence(left), _node_confidence(right), max(0.72, assessment["confidence"])
        )
        assessment = {**assessment, "confidence": round(functional_confidence, 3)}
        matches.append(_record_match(left, right, assessment, "functional_role"))
        unused_left.pop(str(left["id"]))
        unused_right.pop(str(right["id"]))

    # Priorities 3–5: label, relations and stable attributes, accepted only together.
    scored_candidates = []
    for left in unused_left.values():
        for right in unused_right.values():
            assessment = score_node_pair(left_graph, right_graph, left, right)
            if assessment["confidence"] >= STRONG_MATCH_THRESHOLD:
                scored_candidates.append(
                    (assessment["confidence"], assessment["score"], left, right, assessment)
                )
    for _, _, left, right, assessment in sorted(
        scored_candidates, key=lambda item: (item[0], item[1]), reverse=True
    ):
        left_id, right_id = str(left["id"]), str(right["id"])
        if left_id not in unused_left or right_id not in unused_right:
            continue
        matches.append(_record_match(left, right, assessment, "composite_identity"))
        unused_left.pop(left_id)
        unused_right.pop(right_id)

    ambiguous = []
    for left in unused_left.values():
        alternatives = []
        for right in unused_right.values():
            assessment = score_node_pair(left_graph, right_graph, left, right)
            if assessment["score"] < AMBIGUOUS_MATCH_THRESHOLD:
                continue
            alternatives.append(
                {
                    "right_id": str(right["id"]),
                    "score": assessment["score"],
                    "confidence": assessment["confidence"],
                    "signals": assessment["signals"],
                }
            )
        if alternatives:
            ambiguous.append(
                {
                    "left_id": str(left["id"]),
                    "right_candidates": sorted(
                        alternatives,
                        key=lambda item: (item["confidence"], item["score"]),
                        reverse=True,
                    )[:5],
                }
            )

    return {
        "matcher_version": MATCHER_VERSION,
        "matches": sorted(matches, key=lambda item: (item["left_id"], item["right_id"])),
        "unmatched_left": sorted(unused_left),
        "unmatched_right": sorted(unused_right),
        "ambiguous": ambiguous,
        "metrics": {
            "left_nodes": len(left_nodes),
            "right_nodes": len(right_nodes),
            "matched_pairs": len(matches),
            "left_match_rate": round(len(matches) / max(len(left_nodes), 1), 3),
            "right_match_rate": round(len(matches) / max(len(right_nodes), 1), 3),
            "ambiguous_left_nodes": len(ambiguous),
        },
        "policy": {
            "identity_priority": [
                "canonical_identity",
                "functional_role",
                "labels",
                "relations",
                "attributes",
                "geometry_diagnostic_only",
            ],
            "bbox_identity": False,
            "geometry_identity_weight": 0.0,
            "strong_match_threshold": STRONG_MATCH_THRESHOLD,
        },
    }


__all__ = [
    "AMBIGUOUS_MATCH_THRESHOLD",
    "CANONICAL_MATCH_THRESHOLD",
    "MATCHER_VERSION",
    "STRONG_MATCH_THRESHOLD",
    "functional_role",
    "identity_values",
    "label_values",
    "match_graph_nodes",
    "normalize_identity",
    "relation_signature",
    "score_node_pair",
    "section_identity",
]
