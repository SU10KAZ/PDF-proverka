"""Deterministic functional identity matching for two SYSTEM_GRAPH objects.

Geometry is navigation evidence, never identity. High-confidence matches are
selected with deterministic maximum-weight assignment and a two-sided margin.
Medium candidates remain ambiguity evidence and are never used to assert a
structural change.
"""
from __future__ import annotations

import collections
import math
import re
from typing import Any, Optional

from .system_graph_comparison_policy import (
    COMPARED_ATTRIBUTE_KEYS,
    DEFAULT_COMPARISON_POLICY,
    SystemGraphComparisonPolicy,
)


#: v3 считает отступ по признакам тождества, а не по уверенности: потолок
#: качества доказательств одинаков у всех кандидатов узла и обнулял отступ.
MATCHER_VERSION = "graph-identity-matcher-v3"
STRONG_MATCH_THRESHOLD = DEFAULT_COMPARISON_POLICY.high_match_threshold
CANONICAL_MATCH_THRESHOLD = DEFAULT_COMPARISON_POLICY.high_match_threshold
AMBIGUOUS_MATCH_THRESHOLD = DEFAULT_COMPARISON_POLICY.medium_match_threshold

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


def _tie_sections(
    graph: dict,
    node: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> tuple[str, ...]:
    index = _node_index(graph)
    values = []
    for edge in graph.get("edges") or []:
        if (
            edge.get("type") != comparison_policy.tie_edge_type
            or str(edge.get("from")) != str(node.get("id"))
        ):
            continue
        target = index.get(str(edge.get("to"))) or {}
        identity = normalize_identity(
            target.get("canonical_identity") or target.get("label") or edge.get("to")
        )
        if identity:
            values.append(identity)
    return tuple(sorted(values))


def functional_role(
    graph: dict,
    node: dict,
    comparison_policy: SystemGraphComparisonPolicy = DEFAULT_COMPARISON_POLICY,
) -> str:
    node_type = str(node.get("type") or comparison_policy.unknown_node_type)
    section = section_identity(graph, node) or "NO_SECTION"
    attrs = node.get("attrs") or {}
    if node_type == comparison_policy.source_node_type:
        return f"SOURCE:{node.get('source_role') or 'SUPPLY'}:{section}"
    if node_type == comparison_policy.input_node_type:
        return f"INPUT:{section}"
    if node_type == comparison_policy.section_node_type:
        identity = normalize_identity(node.get("canonical_identity") or node.get("label"))
        return f"SECTION:{identity or 'SECTION'}"
    if node_type == comparison_policy.section_device_node_type:
        return "SECTION_LINK:" + ",".join(_tie_sections(graph, node, comparison_policy))
    if node_type in comparison_policy.always_anchored_group_types:
        return f"FUNCTION:{node_type}:{section}"
    if node_type == comparison_policy.aggregate_group_type:
        subclass = normalize_identity(attrs.get("subclass") or attrs.get("type_candidate"))
        if attrs.get(comparison_policy.aggregate_count_attribute) is not None:
            subclass = "AGGREGATE_SERVICE"
        return f"FUNCTION:{node_type}:{subclass or 'SERVICE'}:{section}"
    if node_type == comparison_policy.repeated_node_type:
        return f"REPEATED_NODE:{section}"
    if node_type == comparison_policy.terminal_node_type:
        return f"TERMINAL:{section}"
    return f"{node_type}:{section}"


def _stable_attributes(node: dict) -> dict[str, str]:
    output = {}
    for key, value in sorted((node.get("attrs") or {}).items()):
        if (
            key in _GEOMETRY_ATTRIBUTE_KEYS
            or key in _VOLATILE_ATTRIBUTE_KEYS
            or key in COMPARED_ATTRIBUTE_KEYS
        ):
            continue
        if value is None or isinstance(value, (dict, list, tuple, set)):
            continue
        output[str(key)] = normalize_identity(value)
    return output


def _parent_group_signature(graph: dict, node: dict) -> tuple:
    section = section_identity(graph, node)
    if section:
        return ("SECTION", section)
    index = _node_index(graph)
    parents = []
    for edge in graph.get("edges") or []:
        if not isinstance(edge, dict) or str(edge.get("to")) != str(node.get("id")):
            continue
        parent = index.get(str(edge.get("from"))) or {}
        parents.append(
            (
                str(edge.get("type") or ""),
                str(parent.get("type") or ""),
                normalize_identity(parent.get("canonical_identity") or parent.get("label")),
            )
        )
    return tuple(sorted(parents))


def _jaccard(left: set, right: set) -> float:
    if not left and not right:
        return 1.0
    return len(left & right) / max(len(left | right), 1)


def score_node_pair(
    left_graph: dict,
    right_graph: dict,
    left: dict,
    right: dict,
    comparison_policy: SystemGraphComparisonPolicy = DEFAULT_COMPARISON_POLICY,
) -> dict:
    """Return transparent functional signals without using geometry."""
    score = 0.0
    signals = []
    left_identity, right_identity = identity_values(left), identity_values(right)
    canonical_overlap = sorted(left_identity & right_identity)
    if canonical_overlap:
        score += 0.62
        signals.append({"signal": "canonical_identity", "weight": 0.62, "values": canonical_overlap})

    left_role = functional_role(left_graph, left, comparison_policy)
    right_role = functional_role(right_graph, right, comparison_policy)
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
        "identity_conflict": bool(left_identity and right_identity and not canonical_overlap),
        "signals": signals,
        "geometry": {"used_for_identity": False, "weight": 0.0},
    }


def _functional_fallback_compatible(
    left_graph: dict,
    right_graph: dict,
    left: dict,
    right: dict,
    comparison_policy: SystemGraphComparisonPolicy,
) -> bool:
    if str(left.get("type")) != str(right.get("type")):
        return False
    if functional_role(left_graph, left, comparison_policy) != functional_role(
        right_graph, right, comparison_policy
    ):
        return False
    left_parent = _parent_group_signature(left_graph, left)
    right_parent = _parent_group_signature(right_graph, right)
    if not left_parent or left_parent != right_parent:
        return False
    if relation_signature(left_graph, str(left.get("id"))) != relation_signature(
        right_graph, str(right.get("id"))
    ):
        return False
    return _stable_attributes(left) == _stable_attributes(right)


def _maximum_weight_assignment(
    left_nodes: dict[str, dict],
    right_nodes: dict[str, dict],
    candidates: dict[tuple[str, str], dict],
) -> list[tuple[str, str]]:
    """Deterministic rectangular Hungarian assignment with unmatched dummies."""
    if not left_nodes or not right_nodes or not candidates:
        return []
    rows = sorted(left_nodes)
    real_columns = sorted(right_nodes)
    columns = real_columns + [f"\x00UNMATCHED:{row}" for row in rows]
    row_count, column_count = len(rows), len(columns)
    costs = []
    for left_id in rows:
        row = []
        for right_id in real_columns:
            candidate = candidates.get((left_id, right_id))
            row.append(-float(candidate.get("assignment_weight", 0.0)) if candidate else 0.0)
        row.extend([0.0] * row_count)
        costs.append(row)

    u = [0.0] * (row_count + 1)
    v = [0.0] * (column_count + 1)
    p = [0] * (column_count + 1)
    way = [0] * (column_count + 1)
    for row_index in range(1, row_count + 1):
        p[0] = row_index
        column0 = 0
        min_value = [float("inf")] * (column_count + 1)
        used = [False] * (column_count + 1)
        while True:
            used[column0] = True
            active_row = p[column0]
            delta = float("inf")
            next_column = 0
            for column in range(1, column_count + 1):
                if used[column]:
                    continue
                current = costs[active_row - 1][column - 1] - u[active_row] - v[column]
                if current < min_value[column]:
                    min_value[column] = current
                    way[column] = column0
                if min_value[column] < delta:
                    delta = min_value[column]
                    next_column = column
            for column in range(column_count + 1):
                if used[column]:
                    u[p[column]] += delta
                    v[column] -= delta
                else:
                    min_value[column] -= delta
            column0 = next_column
            if p[column0] == 0:
                break
        while True:
            previous = way[column0]
            p[column0] = p[previous]
            column0 = previous
            if column0 == 0:
                break

    assignment = []
    for column in range(1, column_count + 1):
        if not p[column] or column > len(real_columns):
            continue
        left_id = rows[p[column] - 1]
        right_id = real_columns[column - 1]
        if (left_id, right_id) in candidates:
            assignment.append((left_id, right_id))
    return sorted(assignment)


def _discrimination(candidate: dict) -> float:
    """Насколько пара отличима от соперников — по признакам тождества.

    Отступ и уверенность отвечают на РАЗНЫЕ вопросы. Уверенность — «достаточно
    ли хорошо прочитан чертёж», и она ограничена сверху качеством
    доказательств: ``confidence = min(score, sqrt(conf_left * conf_right))``.
    Отступ — «единственный ли это разумный кандидат», и это свойство признаков
    тождества, то есть счёта.

    Считать отступ по уверенности нельзя именно на хорошо прочитанных листах.
    Обе стороны выпущены одним экстрактором, поэтому потолок качества у всех
    кандидатов одного узла одинаков; если счёт любого из них выше потолка,
    ``min`` срезает их всех к одному числу. Отличие «1.00 против 0.90»
    исчезает, отступ обращается в нуль, и пара, у которой совпала ещё и
    секция, оказывается неотличимой от пары из соседней секции. На боевой паре
    ГРЩ так терялись 34 сопоставления из 42 при потолке 0.814.
    """
    return float(candidate["assessment"]["score"])


def _candidate_margin(
    candidate: dict,
    candidates: dict[tuple[str, str], dict],
) -> tuple[float, float]:
    left_id, right_id = candidate["left_id"], candidate["right_id"]
    value = _discrimination(candidate)
    left_alternative = max(
        [
            _discrimination(item)
            for item in candidates.values()
            if item["left_id"] == left_id and item["right_id"] != right_id
        ]
        or [0.0]
    )
    right_alternative = max(
        [
            _discrimination(item)
            for item in candidates.values()
            if item["right_id"] == right_id and item["left_id"] != left_id
        ]
        or [0.0]
    )
    return round(value - left_alternative, 3), round(value - right_alternative, 3)


def _decide_candidate(
    candidate: dict,
    candidates: dict[tuple[str, str], dict],
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict:
    left_margin, right_margin = _candidate_margin(candidate, candidates)
    confidence = float(candidate["decision_confidence"])
    high = (
        confidence >= comparison_policy.high_match_threshold
        and left_margin >= comparison_policy.high_match_margin
        and right_margin >= comparison_policy.high_match_margin
        and not candidate.get("identity_conflict", False)
    )
    decision = (
        "HIGH_MATCH"
        if high
        else "MEDIUM_MATCH"
        if confidence >= comparison_policy.medium_match_threshold
        else "LOW_MATCH"
    )
    return {
        **candidate,
        "decision": decision,
        "left_margin": left_margin,
        "right_margin": right_margin,
    }


def _record_match(candidate: dict) -> dict:
    assessment = candidate["assessment"]
    return {
        "left_id": candidate["left_id"],
        "right_id": candidate["right_id"],
        "score": assessment["score"],
        "confidence": round(float(candidate["decision_confidence"]), 3),
        "decision": candidate["decision"],
        "left_margin": candidate["left_margin"],
        "right_margin": candidate["right_margin"],
        "method": candidate["method"],
        "signals": candidate.get("signals", assessment["signals"]),
        "geometry": assessment["geometry"],
    }


def _terminal_target(
    graph: dict,
    outgoing_id: str,
    comparison_policy: SystemGraphComparisonPolicy,
) -> Optional[str]:
    for edge in graph.get("edges") or []:
        if (
            edge.get("type") == comparison_policy.terminal_edge_type
            and str(edge.get("from")) == outgoing_id
        ):
            return str(edge.get("to"))
    return None


def _align_unmatched_terminals_from_parents(
    left_graph: dict,
    right_graph: dict,
    matches: list[dict],
    unused_left: dict[str, dict],
    unused_right: dict[str, dict],
    comparison_policy: SystemGraphComparisonPolicy,
) -> tuple[list[dict], list[dict]]:
    """Use a high parent relation only when it does not override identity."""
    left_index, right_index = _node_index(left_graph), _node_index(right_graph)
    added, conflicts = [], []
    for parent in list(matches):
        left_parent = left_index.get(parent["left_id"]) or {}
        right_parent = right_index.get(parent["right_id"]) or {}
        if (
            left_parent.get("type") != comparison_policy.repeated_node_type
            or right_parent.get("type") != comparison_policy.repeated_node_type
        ):
            continue
        left_terminal = _terminal_target(left_graph, parent["left_id"], comparison_policy)
        right_terminal = _terminal_target(right_graph, parent["right_id"], comparison_policy)
        if not left_terminal or not right_terminal:
            continue
        if left_terminal not in unused_left or right_terminal not in unused_right:
            continue
        left_node, right_node = unused_left[left_terminal], unused_right[right_terminal]
        left_identities, right_identities = identity_values(left_node), identity_values(right_node)
        if left_identities and right_identities and not (left_identities & right_identities):
            conflicts.append(
                {
                    "kind": "parent_relation_conflicts_with_canonical_identity",
                    "left_id": left_terminal,
                    "right_id": right_terminal,
                    "left_parent": parent["left_id"],
                    "right_parent": parent["right_id"],
                }
            )
            continue
        assessment = score_node_pair(
            left_graph, right_graph, left_node, right_node, comparison_policy
        )
        confidence = round(
            min(
                parent["confidence"],
                _node_confidence(left_node),
                _node_confidence(right_node),
            ),
            3,
        )
        if confidence < comparison_policy.high_match_threshold:
            continue
        candidate = {
            "left_id": left_terminal,
            "right_id": right_terminal,
            "assessment": assessment,
            "decision_confidence": confidence,
            "decision": "HIGH_MATCH",
            "left_margin": confidence,
            "right_margin": confidence,
            "method": "matched_parent_relation",
            "signals": assessment["signals"]
            + [
                {
                    "signal": "matched_parent_relation",
                    "weight": 0.0,
                    "left_parent": parent["left_id"],
                    "right_parent": parent["right_id"],
                }
            ],
        }
        record = _record_match(candidate)
        matches.append(record)
        added.append(record)
        unused_left.pop(left_terminal)
        unused_right.pop(right_terminal)
    return added, conflicts


def _canonical_candidates(
    left_graph: dict,
    right_graph: dict,
    left_nodes: dict[str, dict],
    right_nodes: dict[str, dict],
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict[tuple[str, str], dict]:
    output = {}
    for left_id, left in left_nodes.items():
        if not identity_values(left):
            continue
        for right_id, right in right_nodes.items():
            if not (identity_values(left) & identity_values(right)):
                continue
            assessment = score_node_pair(
                left_graph, right_graph, left, right, comparison_policy
            )
            output[(left_id, right_id)] = {
                "left_id": left_id,
                "right_id": right_id,
                "assessment": assessment,
                "decision_confidence": assessment["confidence"],
                "assignment_weight": assessment["confidence"] + assessment["score"] / 1000.0,
                "identity_conflict": False,
                "method": "canonical_identity_global",
            }
    return output


def _functional_candidates(
    left_graph: dict,
    right_graph: dict,
    left_nodes: dict[str, dict],
    right_nodes: dict[str, dict],
    comparison_policy: SystemGraphComparisonPolicy,
) -> dict[tuple[str, str], dict]:
    output = {}
    left_roles = collections.Counter(
        functional_role(left_graph, node, comparison_policy) for node in left_nodes.values()
    )
    right_roles = collections.Counter(
        functional_role(right_graph, node, comparison_policy) for node in right_nodes.values()
    )
    for left_id, left in left_nodes.items():
        left_role = functional_role(left_graph, left, comparison_policy)
        for right_id, right in right_nodes.items():
            assessment = score_node_pair(
                left_graph, right_graph, left, right, comparison_policy
            )
            unique_role = (
                left_role == functional_role(right_graph, right, comparison_policy)
                and left_roles[left_role] == 1
                and right_roles[left_role] == 1
            )
            fallback = unique_role and _functional_fallback_compatible(
                left_graph, right_graph, left, right, comparison_policy
            )
            if fallback:
                decision_confidence = round(
                    min(_node_confidence(left), _node_confidence(right), 0.76), 3
                )
                signals = assessment["signals"] + [
                    {
                        "signal": "unique_functional_identity",
                        "weight": round(max(0.0, decision_confidence - assessment["score"]), 3),
                        "label_or_designation_changed": not bool(assessment["canonical_overlap"]),
                    }
                ]
                identity_conflict = False
                method = "unique_functional_identity"
            else:
                decision_confidence = assessment["confidence"]
                signals = assessment["signals"]
                identity_conflict = assessment["identity_conflict"]
                method = "composite_identity_global"
            if decision_confidence < comparison_policy.medium_match_threshold:
                continue
            output[(left_id, right_id)] = {
                "left_id": left_id,
                "right_id": right_id,
                "assessment": assessment,
                "decision_confidence": decision_confidence,
                "assignment_weight": decision_confidence + assessment["score"] / 1000.0,
                "identity_conflict": identity_conflict,
                "method": method,
                "signals": signals,
            }
    return output


def _accept_high_assignment(
    candidates: dict[tuple[str, str], dict],
    unused_left: dict[str, dict],
    unused_right: dict[str, dict],
    comparison_policy: SystemGraphComparisonPolicy,
) -> tuple[list[dict], list[dict]]:
    assignment = _maximum_weight_assignment(unused_left, unused_right, candidates)
    high, medium = [], []
    for key in assignment:
        decided = _decide_candidate(candidates[key], candidates, comparison_policy)
        record = _record_match(decided)
        if decided["decision"] == "HIGH_MATCH":
            high.append(record)
        elif decided["decision"] == "MEDIUM_MATCH":
            medium.append(record)
    for item in high:
        unused_left.pop(item["left_id"], None)
        unused_right.pop(item["right_id"], None)
    return high, medium


def _ambiguous_candidates(
    candidates: dict[tuple[str, str], dict],
    unused_left: dict[str, dict],
    unused_right: dict[str, dict],
    comparison_policy: SystemGraphComparisonPolicy,
) -> list[dict]:
    output = []
    scoped = {
        key: item
        for key, item in candidates.items()
        if key[0] in unused_left and key[1] in unused_right
    }
    for left_id in sorted(unused_left):
        alternatives = []
        for (candidate_left, right_id), candidate in scoped.items():
            if candidate_left != left_id:
                continue
            decided = _decide_candidate(candidate, scoped, comparison_policy)
            if decided["decision"] == "LOW_MATCH":
                continue
            alternatives.append(
                {
                    "right_id": right_id,
                    "score": candidate["assessment"]["score"],
                    "confidence": candidate["decision_confidence"],
                    "decision": "MEDIUM_MATCH",
                    "left_margin": decided["left_margin"],
                    "right_margin": decided["right_margin"],
                    "identity_conflict": candidate.get("identity_conflict", False),
                    "signals": candidate.get("signals", candidate["assessment"]["signals"]),
                }
            )
        if alternatives:
            output.append(
                {
                    "left_id": left_id,
                    "right_candidates": sorted(
                        alternatives,
                        key=lambda item: (-item["confidence"], -item["score"], item["right_id"]),
                    )[:5],
                }
            )
    return output


def empty_matching_result(left_graph: dict, right_graph: dict) -> dict:
    """Return a matcher-shaped result when invalid inputs must not be scored."""
    left_ids = sorted(_node_index(left_graph))
    right_ids = sorted(_node_index(right_graph))
    return {
        "matcher_version": MATCHER_VERSION,
        "matches": [],
        "medium_matches": [],
        "unmatched_left": left_ids,
        "unmatched_right": right_ids,
        "ambiguous": [],
        "ambiguous_left_ids": [],
        "ambiguous_right_ids": [],
        "relation_conflicts": [],
        "metrics": {
            "left_nodes": len(left_ids),
            "right_nodes": len(right_ids),
            "matched_pairs": 0,
            "left_match_rate": 0.0,
            "right_match_rate": 0.0,
            "ambiguous_left_nodes": 0,
            "ambiguous_right_nodes": 0,
        },
        "policy": {
            "algorithm": "not_run_invalid_input",
            "bbox_identity": False,
            "geometry_identity_weight": 0.0,
        },
    }


def match_graph_nodes(
    left_graph: dict,
    right_graph: dict,
    comparison_policy: SystemGraphComparisonPolicy = DEFAULT_COMPARISON_POLICY,
) -> dict:
    """Return only deterministic HIGH matches; retain MEDIUM as uncertainty."""
    left_index, right_index = _node_index(left_graph), _node_index(right_graph)
    unused_left = dict(left_index)
    unused_right = dict(right_index)
    matches, medium_matches = [], []

    canonical = _canonical_candidates(
        left_graph, right_graph, unused_left, unused_right, comparison_policy
    )
    high, medium = _accept_high_assignment(
        canonical, unused_left, unused_right, comparison_policy
    )
    matches.extend(high)
    medium_matches.extend(medium)

    _, relation_conflicts = _align_unmatched_terminals_from_parents(
        left_graph,
        right_graph,
        matches,
        unused_left,
        unused_right,
        comparison_policy,
    )

    functional = _functional_candidates(
        left_graph, right_graph, unused_left, unused_right, comparison_policy
    )
    high, medium = _accept_high_assignment(
        functional, unused_left, unused_right, comparison_policy
    )
    matches.extend(high)
    medium_matches.extend(medium)

    unresolved_candidates = {**canonical, **functional}
    ambiguous = _ambiguous_candidates(
        unresolved_candidates, unused_left, unused_right, comparison_policy
    )
    ambiguous_left = {item["left_id"] for item in ambiguous}
    ambiguous_right = {
        candidate["right_id"]
        for item in ambiguous
        for candidate in item["right_candidates"]
    }
    for conflict in relation_conflicts:
        ambiguous_left.add(conflict["left_id"])
        ambiguous_right.add(conflict["right_id"])

    matches = sorted(matches, key=lambda item: (item["left_id"], item["right_id"]))
    matched_left_ids = {item["left_id"] for item in matches}
    matched_right_ids = {item["right_id"] for item in matches}
    medium_matches = sorted(
        {
            (item["left_id"], item["right_id"]): item
            for item in medium_matches
            if item["left_id"] not in matched_left_ids
            and item["right_id"] not in matched_right_ids
        }.values(),
        key=lambda item: (item["left_id"], item["right_id"]),
    )
    return {
        "matcher_version": MATCHER_VERSION,
        "matches": matches,
        "medium_matches": medium_matches,
        "unmatched_left": sorted(unused_left),
        "unmatched_right": sorted(unused_right),
        "ambiguous": ambiguous,
        "ambiguous_left_ids": sorted(ambiguous_left),
        "ambiguous_right_ids": sorted(ambiguous_right),
        "relation_conflicts": relation_conflicts,
        "metrics": {
            "left_nodes": len(left_index),
            "right_nodes": len(right_index),
            "matched_pairs": len(matches),
            "left_match_rate": round(len(matches) / max(len(left_index), 1), 3),
            "right_match_rate": round(len(matches) / max(len(right_index), 1), 3),
            "ambiguous_left_nodes": len(ambiguous_left),
            "ambiguous_right_nodes": len(ambiguous_right),
        },
        "policy": {
            "policy_id": comparison_policy.policy_id,
            "algorithm": "deterministic_global_assignment",
            "margin_basis": "identity_score",
            "identity_priority": [
                "canonical_identity",
                "functional_role",
                "labels",
                "relations",
                "attributes",
                "geometry_diagnostic_only",
            ],
            "decisions": {
                "HIGH_MATCH": "used_for_structural_comparison",
                "MEDIUM_MATCH": "uncertainty_only",
                "LOW_MATCH": "not_used",
            },
            "bbox_identity": False,
            "geometry_identity_weight": 0.0,
            "high_match_threshold": comparison_policy.high_match_threshold,
            "medium_match_threshold": comparison_policy.medium_match_threshold,
            "high_match_margin": comparison_policy.high_match_margin,
        },
    }


__all__ = [
    "AMBIGUOUS_MATCH_THRESHOLD",
    "CANONICAL_MATCH_THRESHOLD",
    "MATCHER_VERSION",
    "STRONG_MATCH_THRESHOLD",
    "empty_matching_result",
    "functional_role",
    "identity_values",
    "label_values",
    "match_graph_nodes",
    "normalize_identity",
    "relation_signature",
    "score_node_pair",
    "section_identity",
]
