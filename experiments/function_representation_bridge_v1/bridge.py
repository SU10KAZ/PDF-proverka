"""The bridge itself: which side of a task can speak, and can the two meet.

Four measurements live here, and they are deliberately kept apart because they
answer different questions and a single number would blur them.

**§13 — what each side speaks.**  A side's representation is the representation
of the assembly it reached.  When it reached none, the page's own inventory is
reported instead, clearly marked as a description of the sheet rather than as
evidence about the function: a page is never a container here, and it does not
become one by being counted.

**§14 — the new coverage metric.**  Not "does topology exist on both sides",
which the previous track measured at zero out of 213, but "can each side state
positive assembly facts".  This is the number the whole design was built to
move.

**§8 — normalization, tested rather than assumed.**  Thirty drawn feeders and
thirty table rows are *not* declared the same fact.  Two things are measured
separately: how often the two sides' printed **designation sets** meet, and how
often their **counts** merely coincide.  Designations are printed; counts agree
by accident, and equating them would manufacture agreement out of arithmetic.

**§20 — recall against the research references.**  The corpus has no
authoritative functional truth, so the 49 ``RESEARCH_REFERENCE`` rows are used
for exactly what they are: hypotheses whose named candidate can be checked for
positive assembly evidence.  Absence of evidence is counted as absence of
evidence and never as a negative.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from .contract import (
    ASSEMBLY_FACTS_BOTH_SIDES,
    ASSEMBLY_FACTS_LEFT_ONLY,
    ASSEMBLY_FACTS_RIGHT_ONLY,
    AssemblyMembership,
    FunctionalAssembly,
    MIXED,
    NO_ASSEMBLY_FACTS,
    PARTIAL,
    PROVEN,
    SCHEMATIC,
    TABLE,
    TEXT,
)
from .representation import PageRepresentation

#: A side that joined nothing still has a page, and the page still has a shape.
#: It is reported for §13's table and never used as evidence about a function.
PAGE_DESCRIPTION_ONLY = "PAGE_DESCRIPTION_ONLY"


def _joined(row: AssemblyMembership | None) -> bool:
    return bool(row and row.membership_status in {PROVEN, PARTIAL} and row.assembly_id)


def _side_representation(
    rows: Sequence[AssemblyMembership | None],
    assemblies_by_id: Mapping[str, FunctionalAssembly],
    pages: Mapping[int, PageRepresentation],
) -> tuple[str, str]:
    """``(representation, how it was decided)`` for one side of one task."""
    reached = {
        assemblies_by_id[row.assembly_id].representation_type
        for row in rows if _joined(row) and row.assembly_id in assemblies_by_id
    }
    if len(reached) == 1:
        return next(iter(reached)), "ASSEMBLY"
    if len(reached) > 1:
        return MIXED, "ASSEMBLY"
    kinds: set[str] = set()
    for row in rows:
        if row is None or row.physical_page is None:
            continue
        page = pages.get(row.physical_page)
        if page is None:
            continue
        kinds.update(kind for kind in page.representation_types if kind != MIXED)
    if not kinds:
        return TEXT, PAGE_DESCRIPTION_ONLY
    if len(kinds) > 1:
        return MIXED, PAGE_DESCRIPTION_ONLY
    return next(iter(kinds)), PAGE_DESCRIPTION_ONLY


def _task_functions(task: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    left: set[str] = set()
    right: set[str] = set()
    for candidate in task.get("candidates") or []:
        for mapping in candidate.get("component_mapping") or []:
            if mapping.get("left_function_id"):
                left.add(str(mapping["left_function_id"]))
            if mapping.get("right_function_id"):
                right.add(str(mapping["right_function_id"]))
    return left, right


def coverage_audit(
    tasks: Sequence[Mapping[str, Any]],
    memberships: Sequence[AssemblyMembership],
    assemblies: Sequence[FunctionalAssembly],
    pages: Mapping[tuple[str, str], dict[int, PageRepresentation]],
) -> dict[str, Any]:
    """§13 and §14 in one pass over the task population."""
    by_key: dict[tuple[str, str, str], AssemblyMembership] = {
        (row.pair_id, row.side, row.function_id): row for row in memberships
    }
    assemblies_by_id = {item.assembly_id: item for item in assemblies}

    coverage: Counter = Counter()
    pairs: Counter = Counter()
    per_relation: dict[str, Counter] = defaultdict(Counter)
    decided_by: Counter = Counter()
    rows: list[dict[str, Any]] = []
    for task in tasks:
        pair_id = str(task["pair_id"])
        left_ids, right_ids = _task_functions(task)
        left_rows = [by_key.get((pair_id, "LEFT", value)) for value in sorted(left_ids)]
        right_rows = [by_key.get((pair_id, "RIGHT", value)) for value in sorted(right_ids)]
        left_on = any(_joined(row) for row in left_rows)
        right_on = any(_joined(row) for row in right_rows)
        if left_on and right_on:
            classification = ASSEMBLY_FACTS_BOTH_SIDES
        elif left_on:
            classification = ASSEMBLY_FACTS_LEFT_ONLY
        elif right_on:
            classification = ASSEMBLY_FACTS_RIGHT_ONLY
        else:
            classification = NO_ASSEMBLY_FACTS
        coverage[classification] += 1
        left_kind, left_how = _side_representation(
            left_rows, assemblies_by_id, pages[(pair_id, "LEFT")])
        right_kind, right_how = _side_representation(
            right_rows, assemblies_by_id, pages[(pair_id, "RIGHT")])
        pair_class = MIXED if MIXED in (left_kind, right_kind) else f"{left_kind}_TO_{right_kind}"
        pairs[pair_class] += 1
        decided_by[f"LEFT:{left_how}"] += 1
        decided_by[f"RIGHT:{right_how}"] += 1
        for relation in task.get("relation_types") or ["UNKNOWN"]:
            per_relation[str(relation)][classification] += 1
        rows.append({
            "task_id": str(task["task_id"]),
            "project": str(task.get("corpus")),
            "scope_id": str(task.get("scope_id")),
            "relation_types": sorted(str(value) for value in task.get("relation_types") or []),
            "left_function_count": len(left_ids),
            "right_function_count": len(right_ids),
            "left_functions_on_an_assembly": sum(1 for row in left_rows if _joined(row)),
            "right_functions_on_an_assembly": sum(1 for row in right_rows if _joined(row)),
            "left_representation": left_kind,
            "left_representation_decided_by": left_how,
            "right_representation": right_kind,
            "right_representation_decided_by": right_how,
            "representation_pair": pair_class,
            "coverage_class": classification,
        })
    return {
        "tasks": len(rows),
        "by_coverage_class": {key: coverage[key] for key in sorted(coverage)},
        "by_representation_pair": {key: pairs[key] for key in sorted(pairs)},
        "representation_decided_by": {key: decided_by[key] for key in sorted(decided_by)},
        "by_relation_type": {
            relation: {key: value[key] for key in sorted(value)}
            for relation, value in sorted(per_relation.items())
        },
        "rows": rows,
        "rule": (
            "assembly facts are optional positive evidence: a side without them is "
            "not a side that contradicts anything"
        ),
    }


def normalization_audit(
    assemblies: Sequence[FunctionalAssembly],
    facts: Sequence[Any],
) -> dict[str, Any]:
    """§8: do the two representations meet on designations, or only on counts?"""
    by_assembly: dict[str, dict[str, Any]] = defaultdict(dict)
    for fact in facts:
        by_assembly[fact.assembly_id][fact.key] = fact.value
    schematic: list[tuple[FunctionalAssembly, set[str], int]] = []
    tabular: list[tuple[FunctionalAssembly, set[str], int]] = []
    for assembly in assemblies:
        values = by_assembly.get(assembly.assembly_id, {})
        if assembly.representation_type == SCHEMATIC:
            branch = {str(value) for value in values.get("outgoing_branch_designations", [])}
            schematic.append((assembly, branch, int(values.get("feeder_count", 0) or 0)))
        elif assembly.representation_type == TABLE:
            leaders: set[str] = set()
            from .assembly import designations

            leaders.update(designations(values.get("table_row_leaders", []) or []))
            tabular.append((assembly, leaders, int(values.get("table_row_count", 0) or 0)))
    designation_meetings = 0
    count_coincidences = 0
    examples: list[dict[str, Any]] = []
    for left, left_names, left_count in schematic:
        for right, right_names, right_count in tabular:
            if left.pair_id != right.pair_id:
                continue
            shared = left_names & right_names
            if shared:
                designation_meetings += 1
                if len(examples) < 24:
                    examples.append({
                        "schematic_assembly_id": left.assembly_id,
                        "table_assembly_id": right.assembly_id,
                        "same_document": left.document == right.document,
                        "shared_designations": sorted(shared)[:12],
                        "feeder_count": left_count,
                        "table_row_count": right_count,
                    })
            elif left_count and left_count == right_count:
                count_coincidences += 1
    return {
        "schematic_assemblies": len(schematic),
        "table_assemblies": len(tabular),
        "pairs_meeting_on_printed_designations": designation_meetings,
        "pairs_whose_counts_coincide_without_a_shared_designation": count_coincidences,
        "examples": examples,
        "rule": (
            "a shared printed designation is evidence; two counts that happen to be "
            "equal are arithmetic, and this layer never turns one into the other"
        ),
    }


def reference_recall(
    tasks: Sequence[Mapping[str, Any]],
    memberships: Sequence[AssemblyMembership],
) -> dict[str, Any]:
    """§20: on the research references, how often does the named candidate speak?"""
    by_key: dict[tuple[str, str, str], AssemblyMembership] = {
        (row.pair_id, row.side, row.function_id): row for row in memberships
    }
    counts: Counter = Counter()
    rows: list[dict[str, Any]] = []
    for task in tasks:
        references = task.get("references") or []
        if not references:
            continue
        pair_id = str(task["pair_id"])
        named = {
            str(value)
            for reference in references
            for value in reference.get("candidate_ids") or []
        }
        for candidate in task.get("candidates") or []:
            if str(candidate.get("candidate_id")) not in named:
                continue
            left: set[str] = set()
            right: set[str] = set()
            for mapping in candidate.get("component_mapping") or []:
                if mapping.get("left_function_id"):
                    left.add(str(mapping["left_function_id"]))
                if mapping.get("right_function_id"):
                    right.add(str(mapping["right_function_id"]))
            left_on = any(_joined(by_key.get((pair_id, "LEFT", value))) for value in left)
            right_on = any(_joined(by_key.get((pair_id, "RIGHT", value))) for value in right)
            key = (
                ASSEMBLY_FACTS_BOTH_SIDES if left_on and right_on
                else ASSEMBLY_FACTS_LEFT_ONLY if left_on
                else ASSEMBLY_FACTS_RIGHT_ONLY if right_on
                else NO_ASSEMBLY_FACTS
            )
            counts[key] += 1
            rows.append({
                "task_id": str(task["task_id"]),
                "project": str(task.get("corpus")),
                "candidate_id": str(candidate.get("candidate_id")),
                "reference_class": sorted(
                    str(reference.get("reference_class")) for reference in references),
                "coverage_class": key,
            })
    return {
        "referenced_candidates": sum(counts.values()),
        "by_coverage_class": {key: counts[key] for key in sorted(counts)},
        "rows": rows,
        "authoritative_truth": False,
        "rule": (
            "the references are research hypotheses, never truth; a candidate with no "
            "assembly evidence is a candidate this layer says nothing about"
        ),
    }


def control_sheet_walk(
    control: tuple[str, str, int],
    pages: Mapping[tuple[str, str], dict[int, PageRepresentation]],
    assemblies: Mapping[tuple[str, str], dict[int, list[FunctionalAssembly]]],
    facts: Sequence[Any],
) -> dict[str, Any]:
    """§17: the ГРЩ sheet, and whatever the other side says about the same things.

    The counterpart page is not written down here.  It is chosen by a stated
    rule — the page of the other side that prints the most of the control
    sheet's own designations, ties broken by the lower page number — so the
    control moves with the corpus instead of pointing at a page that happened to
    be convenient.
    """
    pair_id, side, page_number = control
    other = "LEFT" if side == "RIGHT" else "RIGHT"
    if page_number not in pages.get((pair_id, side), {}):
        # only reachable on a truncated run; a full run always carries the sheet
        return {
            "control": {"pair_id": pair_id, "side": side, "physical_page": page_number},
            "control_sheet_read": False,
        }
    page = pages[(pair_id, side)][page_number]
    here = assemblies[(pair_id, side)].get(page_number, [])
    by_assembly: dict[str, dict[str, Any]] = defaultdict(dict)
    for fact in facts:
        by_assembly[fact.assembly_id][fact.key] = fact.value

    named: set[str] = set()
    for assembly in here:
        named.update(assembly.named_designations)
        if assembly.owner_designation:
            named.update(_designations_of(assembly.owner_designation))

    best_page: int | None = None
    best_shared: set[str] = set()
    for number in sorted(pages[(pair_id, other)]):
        counterpart = pages[(pair_id, other)][number]
        shared = named & set(counterpart.sheet_marks)
        if len(shared) > len(best_shared):
            best_page, best_shared = number, shared

    counterpart_rows: list[dict[str, Any]] = []
    if best_page is not None:
        for assembly in assemblies[(pair_id, other)].get(best_page, []):
            counterpart_rows.append({
                "assembly_id": assembly.assembly_id,
                "assembly_channel": assembly.assembly_channel,
                "representation_type": assembly.representation_type,
                "assembly_kind": assembly.assembly_kind,
                "extent": assembly.membership_status,
                "owner_designation": assembly.owner_designation,
                "named_designations": list(assembly.named_designations)[:24],
                "facts": {
                    key: by_assembly[assembly.assembly_id][key]
                    for key in sorted(by_assembly.get(assembly.assembly_id, {}))
                    if key in {"quantity_facets", "feeder_count", "bus_count",
                               "table_row_count", "printed_string_count"}
                },
            })

    # Read in the order an engineer would: the drawn graph, then the ruled
    # blocks, then the loose strings — and the loose ones trimmed, because a
    # sheet carries dozens of one-string stroke groups and none of them is the
    # point of this control.
    order = {SCHEMATIC: 0, TABLE: 1, TEXT: 2}
    ranked = sorted(
        here,
        key=lambda item: (
            order.get(item.representation_type, 3),
            -len(item.member_node_ids),
            -len(item.member_label_ids),
            item.assembly_id,
        ),
    )
    listed = [item for item in ranked if item.representation_type != TEXT]
    trimmed = [item for item in ranked if item.representation_type == TEXT]
    control_rows = []
    for assembly in listed + trimmed[:8]:
        control_rows.append({
            "assembly_id": assembly.assembly_id,
            "assembly_channel": assembly.assembly_channel,
            "representation_type": assembly.representation_type,
            "assembly_kind": assembly.assembly_kind,
            "extent": assembly.membership_status,
            "owner_designation": assembly.owner_designation,
            "named_designations": list(assembly.named_designations)[:24],
            "facts": {
                key: by_assembly[assembly.assembly_id][key]
                for key in sorted(by_assembly.get(assembly.assembly_id, {}))
                if key in {"quantity_facets", "feeder_count", "bus_count", "equipment_count",
                           "table_row_count", "table_column_captions", "printed_string_count"}
            },
        })
    counterpart_page = pages[(pair_id, other)].get(best_page) if best_page else None
    return {
        "control": {"pair_id": pair_id, "side": side, "physical_page": page_number},
        "control_sheet": {
            "document": page.document,
            "printed_strings": page.printed_strings,
            "proven_conductors": page.conductor_count,
            "assemblies": len(here),
            "assemblies_by_representation": {
                key: sum(1 for item in here if item.representation_type == key)
                for key in sorted({item.representation_type for item in here})
            },
            "loose_stroke_groups_not_listed": max(len(trimmed) - 8, 0),
            "rows": control_rows,
        },
        "counterpart_selection_rule": (
            "the page of the other side printing the most of the control sheet's own "
            "designations, ties broken by the lower page number"
        ),
        "counterpart": {
            "side": other,
            "physical_page": best_page,
            "shared_designations": sorted(best_shared),
            "printed_strings": counterpart_page.printed_strings if counterpart_page else 0,
            "proven_conductors": counterpart_page.conductor_count if counterpart_page else 0,
            "assemblies": len(counterpart_rows),
            "rows": counterpart_rows,
        },
        "reading": (
            "the same nine engineering things are named on both sides; on one they sit "
            "in ruled cells that attach their numbers to them, on the other they are "
            "printed loose and nothing drawn attaches anything — so this layer names "
            "the meeting and attributes none of the loose numbers"
        ),
    }


def _designations_of(value: str) -> list[str]:
    from .assembly import designations

    return designations([value])


__all__ = [
    "PAGE_DESCRIPTION_ONLY",
    "control_sheet_walk",
    "coverage_audit",
    "normalization_audit",
    "reference_recall",
]
