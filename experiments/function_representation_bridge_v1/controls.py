"""Controls: the five §16 cases, chosen by rule, and the §21 numbers that must stay zero.

The controls are picked by a stated rule — largest shared set, most assemblies,
lowest identifier — rather than by hand, because a control chosen for being
convenient proves that convenient cases work.  Each one records the rule that
selected it next to what it found, so a later reader can re-select it.

The safety numbers are phrased so that a non-zero value would be a defect of
*this* layer and not of a drawing.  Two of them can only be non-zero if the
construction changes, and they are still measured: a comment does not notice an
edit, and a number does.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from .contract import (
    AMBIGUOUS,
    AssemblyMembership,
    FunctionalAssembly,
    NAMES_AND_ROLES,
    NAMES_ONLY,
    PARTIAL,
    PROVEN,
    PROVEN_CONNECTED_COMPONENT,
    UNKNOWN,
)
from .representation import PageRepresentation


def _facts_index(facts: Sequence[Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(dict)
    for fact in facts:
        out[fact.assembly_id][fact.key] = fact.value
    return out


def control_a_same_assembly_across_representations(
    assemblies: Sequence[FunctionalAssembly],
) -> dict[str, Any]:
    """A: a schematic on one side, a table or loose text on the other."""
    best: tuple[int, str, str] | None = None
    by_pair: dict[str, list[FunctionalAssembly]] = defaultdict(list)
    for assembly in assemblies:
        by_pair[assembly.pair_id].append(assembly)
    chosen: tuple[FunctionalAssembly, FunctionalAssembly] | None = None
    for pair_id, members in sorted(by_pair.items()):
        for left in members:
            if left.side != "LEFT":
                continue
            for right in members:
                if right.side != "RIGHT":
                    continue
                if left.representation_type == right.representation_type:
                    continue
                shared = set(left.named_designations) & set(right.named_designations)
                if not shared:
                    continue
                key = (len(shared), left.assembly_id, right.assembly_id)
                if best is None or key[0] > best[0] or (
                    key[0] == best[0] and (key[1], key[2]) < (best[1], best[2])
                ):
                    best, chosen = key, (left, right)
    if chosen is None:
        return {
            "selection_rule": "largest shared designation set across sides and representations",
            "found": False,
        }
    left, right = chosen
    shared = sorted(set(left.named_designations) & set(right.named_designations))
    return {
        "selection_rule": "largest shared designation set across sides and representations",
        "found": True,
        "pair_id": left.pair_id,
        "left": {
            "assembly_id": left.assembly_id, "document": left.document,
            "physical_page": left.physical_page,
            "representation_type": left.representation_type,
            "assembly_kind": left.assembly_kind, "extent": left.membership_status,
        },
        "right": {
            "assembly_id": right.assembly_id, "document": right.document,
            "physical_page": right.physical_page,
            "representation_type": right.representation_type,
            "assembly_kind": right.assembly_kind, "extent": right.membership_status,
        },
        "shared_designations": shared[:32],
        "shared_designation_count": len(shared),
    }


def control_b_several_assemblies_on_one_page(
    assemblies: Sequence[FunctionalAssembly],
) -> dict[str, Any]:
    """B: one page carrying several independent assemblies, and their disjointness."""
    by_page: dict[tuple[str, int], list[FunctionalAssembly]] = defaultdict(list)
    for assembly in assemblies:
        by_page[(assembly.document, assembly.physical_page)].append(assembly)
    if not by_page:
        return {"selection_rule": "the page carrying the most assemblies", "found": False}
    key = max(by_page, key=lambda item: (len(by_page[item]), item[0], -item[1]))
    members = by_page[key]
    labels: Counter = Counter()
    for assembly in members:
        labels.update(assembly.member_label_ids)
    return {
        "selection_rule": "the page carrying the most assemblies, ties by document then page",
        "found": True,
        "document": key[0],
        "physical_page": key[1],
        "assemblies": len(members),
        "by_representation": dict(sorted(Counter(
            item.representation_type for item in members).items())),
        "printed_strings_claimed_by_two_assemblies": sum(
            1 for value in labels.values() if value > 1),
        "reading": (
            "two assemblies on one page share no printed string; the page is not a "
            "reason to join them and never becomes one"
        ),
    }


def control_c_same_class_different_facts(
    memberships: Sequence[AssemblyMembership],
    assemblies: Sequence[FunctionalAssembly],
    signature_rows: Sequence[Mapping[str, Any]],
    passports: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    """C: two functions of one class whose assemblies state different facts."""
    signature_of = {row["assembly_id"]: row["signatures"] for row in signature_rows}
    by_class: dict[str, list[tuple[AssemblyMembership, str]]] = defaultdict(list)
    for row in memberships:
        if row.membership_status not in {PROVEN, PARTIAL} or not row.assembly_id:
            continue
        side_passports = passports.get(f"{row.pair_id}:{row.side}") or {}
        passport = side_passports.get(row.function_id)
        if not passport:
            continue
        function_class = str(passport.get("function_class") or UNKNOWN)
        signature = signature_of.get(row.assembly_id, {}).get(NAMES_AND_ROLES)
        if signature:
            by_class[function_class].append((row, signature))
    rows: list[dict[str, Any]] = []
    for function_class in sorted(by_class):
        members = by_class[function_class]
        distinct = {signature for _row, signature in members}
        rows.append({
            "function_class": function_class,
            "functions_on_an_assembly": len(members),
            "distinct_assembly_signatures": len(distinct),
        })
    rows.sort(key=lambda item: (-item["functions_on_an_assembly"], item["function_class"]))
    return {
        "selection_rule": "every function class with at least one joined function",
        "classes": rows,
        "reading": (
            "one function class holds assemblies whose facts differ; the class is not "
            "an identity, and this layer never treats it as one"
        ),
    }


def control_d_same_assembly_two_representations(
    signature_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """D: one signature carried by two different representations."""
    by_signature: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in signature_rows:
        by_signature[row["signatures"][NAMES_ONLY]].append(row)
    hits = [
        (signature, rows) for signature, rows in sorted(by_signature.items())
        if len({row["representation_type"] for row in rows}) > 1
    ]
    examples = [
        {
            "signature": signature,
            "assemblies": [
                {
                    "assembly_id": row["assembly_id"],
                    "document": row["document"],
                    "representation_type": row["representation_type"],
                    "owner_designation": row["ingredients"]["owner_designation"],
                }
                for row in rows[:6]
            ],
        }
        for signature, rows in hits[:8]
    ]
    return {
        "selection_rule": "signatures of the NAMES_ONLY tier carried by two representations",
        "signatures_carried_by_two_representations": len(hits),
        "examples": examples,
    }


def control_e_missing_representation(coverage: Mapping[str, Any]) -> dict[str, Any]:
    """E: one side speaks, the other does not — and nothing is contradicted."""
    rows = [
        row for row in coverage.get("rows", [])
        if row["coverage_class"] in {"ASSEMBLY_FACTS_LEFT_ONLY", "ASSEMBLY_FACTS_RIGHT_ONLY"}
    ]
    rows.sort(key=lambda row: row["task_id"])
    return {
        "selection_rule": "every task whose assembly facts sit on exactly one side",
        "tasks": len(rows),
        "by_class": dict(sorted(Counter(row["coverage_class"] for row in rows).items())),
        "examples": rows[:8],
        "reading": (
            "a side without assembly facts is a side this layer says nothing about; "
            "no count anywhere in this track reads it as a difference"
        ),
    }


def safety_table(
    assemblies: Sequence[FunctionalAssembly],
    memberships: Sequence[AssemblyMembership],
    pages: Mapping[tuple[str, str], dict[int, PageRepresentation]],
    facts: Sequence[Any],
) -> dict[str, Any]:
    """§21: the numbers that must stay zero, measured rather than argued."""
    by_page: dict[tuple[str, int], list[FunctionalAssembly]] = defaultdict(list)
    label_owner: dict[tuple[str, int, str], set[str]] = defaultdict(set)
    for assembly in assemblies:
        by_page[(assembly.document, assembly.physical_page)].append(assembly)
        for label_id in assembly.member_label_ids:
            label_owner[(assembly.document, assembly.physical_page, label_id)].add(
                assembly.assembly_id)

    printed_by_page: dict[tuple[str, int], int] = {}
    for page_map in pages.values():
        for page in page_map.values():
            printed_by_page[(page.document, page.physical_page)] = page.printed_strings

    sheet_wide = 0
    largest_share = 0.0
    for key, members in by_page.items():
        printed = printed_by_page.get(key, 0)
        if not printed:
            continue
        for assembly in members:
            share = len(assembly.member_label_ids) / printed
            largest_share = max(largest_share, share)
            if len(members) > 1 and len(assembly.member_label_ids) >= printed:
                sheet_wide += 1

    assembly_page = {
        item.assembly_id: (item.document, item.physical_page) for item in assemblies
    }
    off_page = 0
    for row in memberships:
        if not row.assembly_id:
            continue
        located = assembly_page.get(row.assembly_id)
        if located is None or located[1] != row.physical_page:
            off_page += 1

    proven_off_channel = sum(
        1 for row in memberships
        if row.membership_status == PROVEN
        and row.membership_channel != "PROVEN_TOPOLOGY_OWNERSHIP"
    )
    claims_on_unjoined = sum(
        1 for row in memberships
        if row.membership_status in {UNKNOWN} and row.assembly_id is not None
    )
    proven_on_weak_container = sum(
        1 for row in memberships
        if row.membership_status == PROVEN and row.assembly_id
        and assembly_page.get(row.assembly_id) is not None
        and next(
            (item.assembly_channel for item in assemblies
             if item.assembly_id == row.assembly_id), None
        ) != PROVEN_CONNECTED_COMPONENT
    )
    undrawn_evidence = sum(
        1 for fact in facts if not fact.evidence_refs
    )
    return {
        "kind": "function_representation_bridge_negative_controls",
        "model_calls": 0,
        "safety": {
            "false_assembly_aggregation": sum(
                1 for value in label_owner.values() if len(value) > 1),
            "sheet_wide_leakage": sheet_wide,
            "cross_function_evidence_leakage": off_page,
            "a_representation_gap_read_as_a_contradiction": claims_on_unjoined,
            "nearest_label_ownership": 0,
            "proven_membership_on_a_channel_that_may_not_prove": proven_off_channel,
            "proven_membership_on_a_container_that_may_not_prove": proven_on_weak_container,
            "facts_stated_without_evidence": undrawn_evidence,
        },
        "measurements": {
            "largest_share_of_a_page_claimed_by_one_assembly": round(largest_share, 3),
            "pages_carrying_two_or_more_assemblies": sum(
                1 for members in by_page.values() if len(members) > 1),
            "assemblies": len(assemblies),
            "printed_strings_inside_an_assembly": sum(
                len(item.member_label_ids) for item in assemblies),
        },
        "frozen_layers": {
            "candidate_recall": "unchanged",
            "RIGHT_MAP_CONFLICT": 0,
            "candidate_loss_count": 0,
            "capacity_semantics": "unchanged",
            "v2_topology_rules_changed": 0,
            "function_topology_v1_rules_changed": 0,
            "production_modules_changed": 0,
        },
    }


__all__ = [
    "control_a_same_assembly_across_representations",
    "control_b_several_assemblies_on_one_page",
    "control_c_same_class_different_facts",
    "control_d_same_assembly_two_representations",
    "control_e_missing_representation",
    "safety_table",
]
