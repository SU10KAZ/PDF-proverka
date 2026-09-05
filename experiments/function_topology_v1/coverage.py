"""Who is on the graph, who is not, and — the whole point — *why not*.

§11 of the track is the instruction that shapes this module: V2 reported that
**not one** lineage task has both of its sides on the graph, and that left
functions on a node numbered **zero out of 273**.  A number like that is either a
property of the corpus or a defect of the layer, and the two are told apart only
by taking it apart.

So every function is walked down a ladder of questions, each answerable from the
artifact, and the first one that stops it is recorded as the mechanism:

    does the page carry a vector layer at all?
    does it carry a drawn electrical graph?
    does it carry an aggregate with a proven extent?
    does the passport even have a mark to look for?
    is that mark among the strings the sheet prints?
    is it printed along a conductor, or only in the title and the stamp?

§12 governs how the answer may be read.  If the left version of a project states
a function in a table and the right version draws it, topology is not a channel
both can be measured on — it is positive evidence on one side.  Its absence on
the other is not a disagreement, and no count in this module is allowed to be
read as one.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus

from .aggregation import PageAggregation
from .contract import (
    AMBIGUOUS_BINDING,
    BOTH_SIDES_ON_TOPOLOGY,
    LEFT_ONLY_ON_TOPOLOGY,
    NEITHER_SIDE_ON_TOPOLOGY,
    NO_BINDING,
    PARTIAL_BINDING,
    PROVEN,
    PROVEN_BINDING,
    RIGHT_ONLY_ON_TOPOLOGY,
    ScopeBinding,
    UNKNOWN,
)


def page_inventory(
    results: Mapping[tuple[str, str], list[Any]],
    aggregations: Mapping[tuple[str, str], dict[int, PageAggregation]],
) -> dict[str, Any]:
    """Per document, what its pages actually hold."""
    rows: list[dict[str, Any]] = []
    for pair_id in sorted(
        frozen_corpus.PROJECTS,
        key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key]),
    ):
        for side in frozen_corpus.SIDES:
            pages = results[(pair_id, side)]
            aggregated = aggregations[(pair_id, side)]
            with_strokes = with_graph = with_proven = 0
            subgraphs = proven = partial = 0
            for result in pages:
                page = aggregated.get(result.page)
                if len(result.data.strokes.edges):
                    with_strokes += 1
                if page is not None and page.subgraphs:
                    with_graph += 1
                    subgraphs += len(page.subgraphs)
                    proven_here = [
                        item for item in page.subgraphs if item.boundary_status == PROVEN
                    ]
                    proven += len(proven_here)
                    partial += sum(
                        1 for item in page.subgraphs if item.boundary_status != PROVEN)
                    if proven_here:
                        with_proven += 1
            rows.append({
                "project": frozen_corpus.PROJECTS[pair_id],
                "pair_id": pair_id,
                "side": side,
                "pages": len(pages),
                "pages_with_a_vector_layer": with_strokes,
                "pages_with_a_drawn_graph": with_graph,
                "pages_with_a_proven_aggregate": with_proven,
                "subgraphs": subgraphs,
                "subgraphs_with_a_proven_extent": proven,
                "subgraphs_without_a_bus": partial,
            })
    return {"documents": rows}


def side_coverage(
    bindings: Sequence[ScopeBinding],
    results: Mapping[tuple[str, str], list[Any]],
    aggregations: Mapping[tuple[str, str], dict[int, PageAggregation]],
) -> dict[str, Any]:
    """§11: per corpus and per side, who binds and what stopped the rest."""
    by_document: dict[tuple[str, str], list[ScopeBinding]] = defaultdict(list)
    for row in bindings:
        by_document[(row.pair_id, row.side)].append(row)
    rows: list[dict[str, Any]] = []
    for pair_id in sorted(
        frozen_corpus.PROJECTS,
        key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key]),
    ):
        for side in frozen_corpus.SIDES:
            members = by_document[(pair_id, side)]
            aggregated = aggregations[(pair_id, side)]
            pages_of_functions = {row.physical_page for row in members if row.physical_page}
            topology_pages = {
                page for page in pages_of_functions
                if aggregated.get(page) is not None and aggregated[page].subgraphs
            }
            statuses = Counter(row.binding_status for row in members)
            causes = Counter(
                row.cause for row in members if row.binding_status != PROVEN_BINDING)
            rows.append({
                "project": frozen_corpus.PROJECTS[pair_id],
                "pair_id": pair_id,
                "side": side,
                "functions": len(members),
                "distinct_source_pages": len(pages_of_functions),
                "source_pages_with_a_drawn_graph": len(topology_pages),
                "functions_on_a_page_with_a_drawn_graph": sum(
                    1 for row in members if row.physical_page in topology_pages),
                "binding_status": {key: statuses[key] for key in sorted(statuses)},
                "PROVEN_BOUND": statuses[PROVEN_BINDING],
                "PARTIAL_BOUND": statuses[PARTIAL_BINDING],
                "AMBIGUOUS_BOUND": statuses[AMBIGUOUS_BINDING],
                "UNBOUND": statuses[NO_BINDING] + statuses[UNKNOWN],
                "cause": {key: causes[key] for key in sorted(causes)},
            })
    return {
        "documents": rows,
        "totals": {
            "functions": sum(row["functions"] for row in rows),
            "PROVEN_BOUND": sum(row["PROVEN_BOUND"] for row in rows),
            "PARTIAL_BOUND": sum(row["PARTIAL_BOUND"] for row in rows),
            "AMBIGUOUS_BOUND": sum(row["AMBIGUOUS_BOUND"] for row in rows),
            "UNBOUND": sum(row["UNBOUND"] for row in rows),
        },
        "reading": (
            "an unbound function is a function this layer says nothing about; "
            "§12 forbids reading it as a disagreement with the other version"
        ),
    }


def cross_representation(
    tasks: Sequence[Mapping[str, Any]],
    bindings: Sequence[ScopeBinding],
) -> dict[str, Any]:
    """§19: for every lineage task, which sides could speak topology at all.

    A task names a left scope and, through its candidates, a set of right
    functions.  The class is decided by whether each side reached a subgraph —
    never by whether they agree, which this layer does not ask.
    """
    bound: dict[tuple[str, str, str], ScopeBinding] = {
        (row.pair_id, row.side, row.function_id): row for row in bindings
    }

    def on_topology(pair_id: str, side: str, function_id: str) -> bool:
        row = bound.get((pair_id, side, function_id))
        return bool(row and row.binding_status in {PROVEN_BINDING, PARTIAL_BINDING})

    counts: Counter = Counter()
    per_relation: dict[str, Counter] = defaultdict(Counter)
    rows: list[dict[str, Any]] = []
    for task in tasks:
        pair_id = str(task["pair_id"])
        left_functions: set[str] = set()
        right_functions: set[str] = set()
        for candidate in task.get("candidates") or []:
            for mapping in candidate.get("component_mapping") or []:
                if mapping.get("left_function_id"):
                    left_functions.add(str(mapping["left_function_id"]))
                if mapping.get("right_function_id"):
                    right_functions.add(str(mapping["right_function_id"]))
        left_on = any(on_topology(pair_id, "LEFT", value) for value in left_functions)
        right_on = any(on_topology(pair_id, "RIGHT", value) for value in right_functions)
        if left_on and right_on:
            classification = BOTH_SIDES_ON_TOPOLOGY
        elif left_on:
            classification = LEFT_ONLY_ON_TOPOLOGY
        elif right_on:
            classification = RIGHT_ONLY_ON_TOPOLOGY
        else:
            classification = NEITHER_SIDE_ON_TOPOLOGY
        counts[classification] += 1
        for relation in task.get("relation_types") or ["UNKNOWN"]:
            per_relation[str(relation)][classification] += 1
        rows.append({
            "task_id": str(task["task_id"]),
            "project": str(task.get("corpus")),
            "scope_id": str(task.get("scope_id")),
            "relation_types": sorted(str(value) for value in task.get("relation_types") or []),
            "left_function_count": len(left_functions),
            "right_function_count": len(right_functions),
            "left_functions_on_topology": sum(
                1 for value in left_functions if on_topology(pair_id, "LEFT", value)),
            "right_functions_on_topology": sum(
                1 for value in right_functions if on_topology(pair_id, "RIGHT", value)),
            "representation_class": classification,
        })
    return {
        "tasks": len(rows),
        "by_representation_class": {key: counts[key] for key in sorted(counts)},
        "by_relation_type": {
            relation: {key: value[key] for key in sorted(value)}
            for relation, value in sorted(per_relation.items())
        },
        "rows": rows,
        "rule": (
            "topology is optional positive evidence: a side without it is not a "
            "side that contradicts anything"
        ),
    }


__all__ = ["cross_representation", "page_inventory", "side_coverage"]
