"""The Phase 1 gate: PROVEN before and CERTIFIED after, and both sides of a task.

Read-only over the frozen 213-task population.  No candidate is added, removed
or reordered; the question is only which tasks now have a certified function on
both sides, and how that compares with the bridge's 26 tasks that had assembly
facts on both sides.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence

from .contract import AMBIGUOUS, CERTIFIED, CONTRADICTORY, MembershipCertificate, PARTIAL

CERTIFIED_FUNCTION_FACTS_BOTH_SIDES = "CERTIFIED_FUNCTION_FACTS_BOTH_SIDES"
CERTIFIED_LEFT_ONLY = "CERTIFIED_LEFT_ONLY"
CERTIFIED_RIGHT_ONLY = "CERTIFIED_RIGHT_ONLY"
NO_CERTIFIED_FACTS = "NO_CERTIFIED_FACTS"
COVERAGE_CLASS = (
    CERTIFIED_FUNCTION_FACTS_BOTH_SIDES, CERTIFIED_LEFT_ONLY, CERTIFIED_RIGHT_ONLY, NO_CERTIFIED_FACTS,
)


def task_functions(task: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    left: set[str] = set()
    right: set[str] = set()
    for candidate in task.get("candidates") or []:
        for mapping in candidate.get("component_mapping") or []:
            if mapping.get("left_function_id"):
                left.add(str(mapping["left_function_id"]))
            if mapping.get("right_function_id"):
                right.add(str(mapping["right_function_id"]))
    return left, right


def _joined(row: Any | None) -> bool:
    return bool(row and row.membership_status in {"PROVEN", "PARTIAL"} and row.assembly_id)


def phase1_gate(
    tasks: Sequence[Mapping[str, Any]],
    rows: Sequence[MembershipCertificate],
    scope_rows: Sequence[Mapping[str, Any]],
    bridge_memberships: Sequence[Any],
    decoys: Mapping[str, Any],
) -> dict[str, Any]:
    by_key = {(row.pair_id, row.side, row.function_id): row for row in rows}
    bridge_by_key = {
        (row.pair_id, row.side, row.function_id): row for row in bridge_memberships
    }
    scope_by_id = {str(row["scope_id"]): row for row in scope_rows}

    classes: Counter = Counter()
    per_relation: dict[str, Counter] = {}
    bridge_both = 0
    bridge_both_certified_both = 0
    strict_both = 0
    task_rows: list[dict[str, Any]] = []
    for task in tasks:
        pair_id = str(task["pair_id"])
        left_ids, right_ids = task_functions(task)
        left_rows = [by_key.get((pair_id, "LEFT", value)) for value in sorted(left_ids)]
        right_rows = [by_key.get((pair_id, "RIGHT", value)) for value in sorted(right_ids)]
        left_on = any(row and row.status == CERTIFIED for row in left_rows)
        right_on = any(row and row.status == CERTIFIED for row in right_rows)
        if left_on and right_on:
            classification = CERTIFIED_FUNCTION_FACTS_BOTH_SIDES
        elif left_on:
            classification = CERTIFIED_LEFT_ONLY
        elif right_on:
            classification = CERTIFIED_RIGHT_ONLY
        else:
            classification = NO_CERTIFIED_FACTS
        classes[classification] += 1
        for relation in task.get("relation_types") or []:
            per_relation.setdefault(str(relation), Counter())[classification] += 1

        bridge_left = any(_joined(bridge_by_key.get((pair_id, "LEFT", value))) for value in left_ids)
        bridge_right = any(_joined(bridge_by_key.get((pair_id, "RIGHT", value))) for value in right_ids)
        on_bridge_both = bridge_left and bridge_right
        bridge_both += int(on_bridge_both)
        bridge_both_certified_both += int(on_bridge_both and left_on and right_on)

        scope = scope_by_id.get(str(task.get("scope_id")))
        scope_certified = bool(scope and scope["status"] == CERTIFIED)
        # strict: the whole left scope certified and every right function of at
        # least one candidate certified
        strict = False
        if scope_certified:
            for candidate in task.get("candidates") or []:
                rights = {str(m["right_function_id"]) for m in candidate.get("component_mapping") or []
                          if m.get("right_function_id")}
                if rights and all(
                    (by_key.get((pair_id, "RIGHT", value)) or MembershipCertificate(
                        certificate_id="", pair_id="", project="", side="", function_id="",
                        scope_id=None, fragment_ids=(), physical_page=None, primary_mark=None,
                        status="UNKNOWN")).status == CERTIFIED for value in rights
                ):
                    strict = True
                    break
        strict_both += int(strict)
        task_rows.append({
            "task_id": str(task["task_id"]),
            "project": str(task.get("corpus")),
            "relation_types": sorted(str(value) for value in task.get("relation_types") or []),
            "coverage_class": classification,
            "on_bridge_both_sides": on_bridge_both,
            "left_certified": sum(1 for row in left_rows if row and row.status == CERTIFIED),
            "right_certified": sum(1 for row in right_rows if row and row.status == CERTIFIED),
            "left_functions": len(left_ids),
            "right_functions": len(right_ids),
            "left_scope_status": scope["status"] if scope else None,
            "strict_both_sides": strict,
        })

    statuses = Counter(row.status for row in rows)
    return {
        "tasks": len(tasks),
        "proven_before": sum(1 for row in bridge_memberships if row.membership_status == "PROVEN"),
        "certified_after": statuses[CERTIFIED],
        "certified_after_by_channel": dict(sorted(Counter(
            row.channel for row in rows if row.status == CERTIFIED).items())),
        "ambiguous": statuses[AMBIGUOUS],
        "contradictory": statuses[CONTRADICTORY],
        "partial": statuses[PARTIAL],
        "false_certificates_on_decoys": int(decoys["false_certificates_total"]),
        "bridge_tasks_with_assembly_facts_on_both_sides": bridge_both,
        "of_them_certified_on_both_sides": bridge_both_certified_both,
        "by_coverage_class": {key: classes[key] for key in COVERAGE_CLASS},
        "by_relation_type": {
            key: {cls: value[cls] for cls in COVERAGE_CLASS} for key, value in sorted(per_relation.items())
        },
        "strict_both_sides_tasks": strict_both,
        "meaningful_certified_coverage": classes[CERTIFIED_FUNCTION_FACTS_BOTH_SIDES] > 0,
        "candidate_generator_changed": False,
        "candidates_changed": 0,
        "rows": task_rows,
    }


__all__ = ["COVERAGE_CLASS", "phase1_gate", "task_functions"]
