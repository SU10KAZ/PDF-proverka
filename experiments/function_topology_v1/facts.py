"""Function facts an aggregate can state — and only the ones it can state.

§13 of the track asks the question this module exists for: can a
FunctionTopologySubgraph be turned into facts that are comparable even when the
*other* side of a pair has no topology at all — a table, a Markdown paragraph, a
stamp?  If it can, a graph on the right can meet a table on the left through
facts both can carry, and topology stops being a channel that only fires when
both documents happen to be vector exports.

Two rules govern what may be emitted, and both come straight from V1's
asymmetry.

*Positive only.*  Every fact says what the drawing shows.  There is no fact that
says a device is not there, a consumer is not fed, a bus does not exist.  A count
of zero is a count, not an assertion of absence — and the guard on the vocabulary
still refuses the words that would turn it into one.

*Direction only from an arrowhead.*  V2 measured 13 arrowheads on 278 pages and
49 directed edges, so "incoming" and "outgoing" are almost never provable.  The
honest pair of numbers is therefore split in two: what an arrowhead proved, and
how many of the aggregate's own wires simply end free — the drawn ports, with no
claim about which way the energy runs.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from experiments.pdf_evidence_v1.textnorm import normalize
from experiments.pdf_evidence_v2.contract import (
    BUS,
    EQUIPMENT,
    FEEDER,
    LABEL_ANCHOR,
    PROVEN,
    TABLE_PORT,
    TERMINAL,
)

from .aggregation import PageAggregation, _adjacency, _electrical_edges
from .contract import FunctionTopologySubgraph


def facts_of_subgraph(
    subgraph: FunctionTopologySubgraph,
    result,
    adjacency: Mapping[str, set[str]],
    kinds: Mapping[str, str],
    symbol_of_node: Mapping[str, str | None],
    directed: Mapping[str, tuple[str, str]],
) -> dict[str, Any]:
    """Everything one aggregate can positively state about itself."""
    members = set(subgraph.member_node_ids)
    degree = {node_id: len(set(adjacency.get(node_id, ())) & members) for node_id in members}
    free_feeders = sorted(
        node_id for node_id in members
        if kinds.get(node_id) == FEEDER and degree.get(node_id, 0) <= 1
    )
    directed_in = sum(
        1 for _edge_id, (source, target) in directed.items()
        if target in members and source not in members
    )
    directed_out = sum(
        1 for _edge_id, (source, target) in directed.items()
        if source in members and target not in members
    )
    devices = Counter(
        symbol_of_node.get(node_id) or "-" for node_id in subgraph.equipment_node_ids
    )
    return {
        "subgraph_id": subgraph.subgraph_id,
        "document": subgraph.document,
        "physical_page": subgraph.physical_page,
        "boundary_status": subgraph.boundary_status,
        "owner_marks": sorted(subgraph.function_marks),
        "bus_count": len(subgraph.bus_node_ids),
        "bus_exists": bool(subgraph.bus_node_ids),
        "feeder_count": len(subgraph.feeder_node_ids),
        "equipment_count": len(subgraph.equipment_node_ids),
        "terminal_count": len(subgraph.terminal_node_ids),
        "free_ended_feeder_count": len(free_feeders),
        "device_shape_multiset": {key: devices[key] for key in sorted(devices)},
        "arrow_proven_inbound_edge_count": directed_in,
        "arrow_proven_outbound_edge_count": directed_out,
        "direction_evidence_available": bool(directed_in or directed_out),
        "branch_label_count": len(subgraph.consumer_labels),
        "branch_labels": sorted(subgraph.consumer_labels)[:64],
        "folded_branch_labels": sorted({normalize(text) for text in subgraph.consumer_labels})[:64],
        "cross_referenced_marks": sorted(subgraph.source_labels),
        "topology_signature": subgraph.topology_signature,
        "claim": "every count states what the sheet draws; none states what it does not",
    }


def page_facts(page: PageAggregation, result) -> list[dict[str, Any]]:
    topology = result.topology
    kinds = {node.node_id: node.node_kind for node in topology.nodes}
    symbol_of_node = {node.node_id: node.symbol_signature for node in topology.nodes}
    adjacency = _adjacency(_electrical_edges(topology))
    directed = {
        edge.edge_id: (edge.from_node_id, edge.to_node_id)
        for edge in topology.edges if edge.direction_status == PROVEN
    }
    return [
        facts_of_subgraph(subgraph, result, adjacency, kinds, symbol_of_node, directed)
        for subgraph in page.subgraphs
    ]


#: Which of a passport's own fields a subgraph fact can be compared against
#: without any extraction at all.  Deliberately short: these are the fields whose
#: value a drawing can *show*.
COMPARABLE_TO_PASSPORT = (
    "equipment_roles",
    "stable_entities",
    "systems",
    "consumers",
)


def comparable_fact_shape(facts: Mapping[str, Any]) -> dict[str, Any]:
    """The subset of a subgraph's facts that a text-only side could also carry.

    This is the shape a cross-representation comparison would use: counts and
    folded strings, no coordinates, no node identifiers, no page.  Missing
    topology on the other side makes this shape unavailable — never contradicted.
    """
    return {
        "owner_marks": list(facts["owner_marks"]),
        "bus_exists": facts["bus_exists"],
        "feeder_count": facts["feeder_count"],
        "equipment_count": facts["equipment_count"],
        "free_ended_feeder_count": facts["free_ended_feeder_count"],
        "folded_branch_labels": list(facts["folded_branch_labels"]),
    }


__all__ = [
    "COMPARABLE_TO_PASSPORT", "comparable_fact_shape", "facts_of_subgraph", "page_facts",
]
