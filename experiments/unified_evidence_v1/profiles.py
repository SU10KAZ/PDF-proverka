"""Cross-representation evidence profiles — §4 and §5, read-only over the 213 tasks.

For every frozen candidate of every frozen task: the positive facts of its left
functions, the positive facts of its right functions, the facts the two sides
share, the facts they *explicitly* contradict, and the fields one side is silent
about.  The candidate generator is not touched; nothing is added, removed or
reordered; no candidate is scored.

A contradiction is emitted only where both sides state a single explicit value
of a field built to be compared — the board mark, or one quantity facet — and
the values differ.  A field present on one side only is ``unknown`` and never a
contradiction.
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from .synthesis import CERTIFIED, DECLARED, FunctionFact

CERTIFIED_FUNCTION_FACTS_BOTH_SIDES = "CERTIFIED_FUNCTION_FACTS_BOTH_SIDES"
CERTIFIED_LEFT_ONLY = "CERTIFIED_LEFT_ONLY"
CERTIFIED_RIGHT_ONLY = "CERTIFIED_RIGHT_ONLY"
NO_CERTIFIED_FACTS = "NO_CERTIFIED_FACTS"
COVERAGE_CLASS = (
    CERTIFIED_FUNCTION_FACTS_BOTH_SIDES, CERTIFIED_LEFT_ONLY, CERTIFIED_RIGHT_ONLY, NO_CERTIFIED_FACTS,
)

#: Fields on which two explicit single values may contradict.
COMPARABLE_FIELDS = ("board_mark", "electrical_quantities")


def _value_key(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _side_facts(
    function_ids: Sequence[str], side: str, pair_id: str,
    facts: Mapping[tuple[str, str, str], Sequence[FunctionFact]],
) -> list[FunctionFact]:
    out: list[FunctionFact] = []
    for function_id in sorted(set(function_ids)):
        out.extend(facts.get((pair_id, side, function_id), ()))
    return out


def _explicit_values(rows: Sequence[FunctionFact], field: str) -> dict[str, set[str]]:
    """Single explicit values per comparison key (facet for quantities)."""
    out: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        if row.field != field:
            continue
        if field == "electrical_quantities":
            out[str(row.value["facet"])].add(str(row.value["value"]))
        else:
            out[field].add(str(row.value))
    return out


def profile_candidate(
    candidate: Mapping[str, Any], pair_id: str,
    facts: Mapping[tuple[str, str, str], Sequence[FunctionFact]],
) -> dict[str, Any]:
    left_ids = sorted({str(m["left_function_id"]) for m in candidate.get("component_mapping") or [] if m.get("left_function_id")})
    right_ids = sorted({str(m["right_function_id"]) for m in candidate.get("component_mapping") or [] if m.get("right_function_id")})
    left = _side_facts(left_ids, "LEFT", pair_id, facts)
    right = _side_facts(right_ids, "RIGHT", pair_id, facts)
    left_keys: dict[tuple[str, str], set[str]] = defaultdict(set)
    right_keys: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in left:
        left_keys[row.key].add(row.basis)
    for row in right:
        right_keys[row.key].add(row.basis)
    shared = []
    for key in sorted(set(left_keys) & set(right_keys)):
        shared.append({
            "field": key[0], "value": json.loads(key[1]),
            "left_basis": sorted(left_keys[key]), "right_basis": sorted(right_keys[key]),
            "certified_both_sides": CERTIFIED in left_keys[key] and CERTIFIED in right_keys[key],
            "declared_both_sides": DECLARED in left_keys[key] and DECLARED in right_keys[key],
        })
    contradictions = []
    for field in COMPARABLE_FIELDS:
        left_values = _explicit_values(left, field)
        right_values = _explicit_values(right, field)
        for facet in sorted(set(left_values) & set(right_values)):
            if len(left_values[facet]) == 1 and len(right_values[facet]) == 1 and left_values[facet] != right_values[facet]:
                contradictions.append({
                    "field": field, "facet": facet,
                    "left": next(iter(left_values[facet])), "right": next(iter(right_values[facet])),
                })
    left_fields = {row.field for row in left}
    right_fields = {row.field for row in right}
    return {
        "candidate_id": str(candidate.get("candidate_id")),
        "relation_type": candidate.get("relation_type"),
        "group_derivability": candidate.get("group_derivability"),
        "left_function_ids": left_ids,
        "right_function_ids": right_ids,
        "positive_facts_left": dict(sorted(Counter(row.field for row in left).items())),
        "positive_facts_right": dict(sorted(Counter(row.field for row in right).items())),
        "certified_facts_left": sum(1 for row in left if row.basis == CERTIFIED),
        "certified_facts_right": sum(1 for row in right if row.basis == CERTIFIED),
        "shared_positive_facts": shared,
        "shared_count": len(shared),
        "shared_certified_both_sides": sum(1 for item in shared if item["certified_both_sides"]),
        "shared_natively_corroborated_both_sides": sum(
            1 for key in set(left_keys) & set(right_keys)
            if any(row.key == key and row.natively_corroborated for row in left)
            and any(row.key == key and row.natively_corroborated for row in right)),
        "explicit_contradictions": contradictions,
        "unknown_fields": {
            "left_only": sorted(left_fields - right_fields),
            "right_only": sorted(right_fields - left_fields),
        },
    }


def profile_tasks(
    tasks: Sequence[Mapping[str, Any]],
    facts: Mapping[tuple[str, str, str], Sequence[FunctionFact]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    classes: Counter = Counter()
    for task in tasks:
        pair_id = str(task["pair_id"])
        candidates = [profile_candidate(candidate, pair_id, facts) for candidate in task.get("candidates") or []]
        left_ids = sorted({fid for candidate in candidates for fid in candidate["left_function_ids"]})
        right_ids = sorted({fid for candidate in candidates for fid in candidate["right_function_ids"]})
        left_certified = any(row.basis == CERTIFIED for row in _side_facts(left_ids, "LEFT", pair_id, facts))
        right_certified = any(row.basis == CERTIFIED for row in _side_facts(right_ids, "RIGHT", pair_id, facts))
        if left_certified and right_certified:
            classification = CERTIFIED_FUNCTION_FACTS_BOTH_SIDES
        elif left_certified:
            classification = CERTIFIED_LEFT_ONLY
        elif right_certified:
            classification = CERTIFIED_RIGHT_ONLY
        else:
            classification = NO_CERTIFIED_FACTS
        classes[classification] += 1
        rows.append({
            "task_id": str(task["task_id"]),
            "pair_id": pair_id,
            "project": task.get("corpus"),
            "scope_id": task.get("scope_id"),
            "relation_types": sorted(str(v) for v in task.get("relation_types") or []),
            "group_derivability_classes": sorted(str(v) for v in task.get("group_derivability_classes") or []),
            "reference_candidate_ids": sorted({
                str(candidate_id) for item in task.get("references") or []
                if isinstance(item, Mapping)
                for candidate_id in (item.get("candidate_ids") or ([item["candidate_id"]] if item.get("candidate_id") else []))
            }),
            "reference_class": sorted(str(v) for v in task.get("reference_classes") or []),
            "sentinel": bool(task.get("sentinel")),
            "coverage_class": classification,
            "candidates": candidates,
        })
    return {
        "tasks": len(rows),
        "by_coverage_class": {key: classes[key] for key in COVERAGE_CLASS},
        "candidates_profiled": sum(len(row["candidates"]) for row in rows),
        "candidate_generator_changed": False,
        "candidates_changed": 0,
        "rows": rows,
    }


__all__ = ["COMPARABLE_FIELDS", "COVERAGE_CLASS", "profile_candidate", "profile_tasks"]
