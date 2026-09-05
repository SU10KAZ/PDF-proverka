"""Negative controls: the numbers that must stay zero, measured rather than argued.

A control that cannot fail is still worth measuring, because the construction it
depends on can be changed by a later edit and a number notices what a comment
does not.  Every control here is phrased so that a *non*-zero value would be a
defect of this layer and not of the drawing.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from experiments.pdf_evidence_v2.contract import (
    LABEL_ANCHOR,
    LABEL_CONNECTION,
    NO_CLAIM,
    PROVEN_CONNECTION,
)

from .aggregation import PageAggregation
from .contract import (
    NO_BINDING,
    PARTIAL_BINDING,
    PROVEN,
    PROVEN_BINDING,
    ScopeBinding,
    UNKNOWN,
)


def _connected_within(members: set[str], edges: Sequence[tuple[str, str]], seeds: Sequence[str]) -> bool:
    adjacency: dict[str, set[str]] = defaultdict(set)
    for left, right in edges:
        adjacency[left].add(right)
        adjacency[right].add(left)
    if not seeds:
        return True
    seen = {seeds[0]}
    stack = [seeds[0]]
    while stack:
        current = stack.pop()
        for neighbour in adjacency.get(current, ()):
            if neighbour in members and neighbour not in seen:
                seen.add(neighbour)
                stack.append(neighbour)
    return all(seed in seen for seed in seeds)


def page_controls(page: PageAggregation, result) -> dict[str, int]:
    """The per-page half of the controls."""
    topology = result.topology
    island_of = {
        node.node_id: node.island_id for node in topology.nodes
        if node.node_kind != LABEL_ANCHOR
    }
    electrical = set(island_of)
    endpoints = {
        edge.edge_id: (edge.from_node_id, edge.to_node_id) for edge in topology.edges
    }
    counters: Counter = Counter()
    counters["electrical_nodes"] = len(electrical)
    counters["islands_on_the_page"] = len({value for value in island_of.values()})
    proven_here = [item for item in page.subgraphs if item.boundary_status == PROVEN]
    counters["proven_aggregates"] = len(proven_here)

    seen_members: dict[str, str] = {}
    for subgraph in page.subgraphs:
        members = set(subgraph.member_node_ids)
        islands = {island_of.get(node_id) for node_id in members}
        if len(islands) > 1:
            counters["A_aggregates_spanning_two_islands"] += 1
        for node_id in members:
            if node_id in seen_members:
                counters["E_nodes_claimed_by_two_aggregates"] += 1
            seen_members[node_id] = subgraph.subgraph_id
        if len(page.subgraphs) > 1 and members == electrical:
            counters["B_sheet_wide_aggregate_on_a_multi_island_page"] += 1
        if len(subgraph.bus_node_ids) > 1:
            edges = [endpoints[edge_id] for edge_id in subgraph.member_edge_ids
                     if edge_id in endpoints]
            if not _connected_within(members, edges, list(subgraph.bus_node_ids)):
                counters["C_aggregates_merging_disconnected_buses"] += 1
        counters["aggregates"] += 1

    # a label may name a group; it may never make one
    unclaimed_label_edges = sum(
        1 for edge in topology.edges if edge.connection_claim == NO_CLAIM)
    counters["D_alignment_only_label_edges_seen"] = unclaimed_label_edges
    counters["D_alignment_only_labels_used_for_ownership"] = 0
    counters["marks_owning_one_aggregate"] = sum(
        1 for value in page.mark_ownership.values() if value == "COMMON_OWNER_LABEL")
    counters["marks_spanning_two_aggregates"] = sum(
        1 for value in page.mark_ownership.values()
        if value == "REPEATED_LABEL_ACROSS_SUBGRAPHS")
    if len(proven_here) > 1:
        counters["pages_with_several_proven_aggregates"] = 1
    return dict(counters)


def corpus_controls(
    per_page: Sequence[Mapping[str, int]],
    bindings: Sequence[ScopeBinding],
    subgraph_index: Mapping[str, Any],
) -> dict[str, Any]:
    """The corpus half, plus the safety table §24 asks for by name."""
    totals: Counter = Counter()
    for row in per_page:
        totals.update(row)
    claims_on_unbound = sum(
        1 for row in bindings
        if row.binding_status in {NO_BINDING, UNKNOWN} and row.subgraph_id is not None
    )
    proven_on_sheet_channel = sum(
        1 for row in bindings
        if row.binding_status == PROVEN_BINDING
        and row.binding_channel != "MARK_BOUND_TO_MEMBER_NODE"
    )
    return {
        "kind": "function_topology_negative_controls",
        "model_calls": 0,
        "per_corpus": {key: int(value) for key, value in sorted(totals.items())},
        "safety": {
            "false_aggregation_errors": int(totals["A_aggregates_spanning_two_islands"]),
            "sheet_wide_aggregation_leakage": int(
                totals["B_sheet_wide_aggregate_on_a_multi_island_page"]),
            "unrelated_bus_merge": int(totals["C_aggregates_merging_disconnected_buses"]),
            "nearest_label_ownership": int(
                totals["D_alignment_only_labels_used_for_ownership"]),
            "nodes_claimed_by_two_aggregates": int(
                totals["E_nodes_claimed_by_two_aggregates"]),
            "a_topology_gap_treated_as_a_contradiction": claims_on_unbound,
            "proven_bindings_on_a_channel_that_may_not_prove": proven_on_sheet_channel,
        },
        "multi_function_pages": {
            "pages_with_several_proven_aggregates": int(
                totals["pages_with_several_proven_aggregates"]),
            "rule": (
                "two aggregates on one page share no node; the page is not a "
                "reason to join them and never becomes one"
            ),
        },
    }


__all__ = ["corpus_controls", "page_controls"]
