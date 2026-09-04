"""The controls: the seven ways this could be wrong, each measured.

Counting edges is not evidence of a good graph — a producer that joins every
crossing returns far more edges than one that joins the right ones.  So the
graph is judged by what it refuses.

Each control is a *measurement*, never an assertion that always passes.  Where
a control can only ever hold by construction, it is still measured, because a
construction can be changed by a later edit and a number notices.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from .conductors import CROSSING_WITHOUT_JUNCTION, EdgeFacts, _edge_arrays
from .contract import (
    BUS,
    CONNECTED_JUNCTION,
    FRAME,
    HOP_PROVEN_NON_CONNECTION,
    LABEL_ANCHOR,
    NO_CLAIM,
    PROVEN_CONNECTION,
    SCHEMATIC_CONDUCTOR,
    TABLE_GRID,
    TEXT_UNDERLINE,
)
from .page import PageData
from .topology import PageTopology

#: How close an unbound label's nearest conductor may be before the refusal is
#: worth counting.  Five ems is far past anything a binding rule would use, so
#: everything inside it is a refusal the layer made on purpose.
NEAR_MISS_EM = 5.0


def _accumulate(target: dict[str, int], source: Mapping[str, int]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + int(value)


def page_controls(
    data: PageData, facts: EdgeFacts, topology: PageTopology
) -> dict[str, int]:
    """Every negative control, on one page, as counts."""
    edges_array, horizontal, low, high, level = _edge_arrays(data)
    nature = np.asarray(facts.nature)
    conductor = facts.conductor
    out: dict[str, int] = {}

    # A — an intersection is not a connection.
    rejected = [c for c in facts.crossings if c.verdict == CROSSING_WITHOUT_JUNCTION]
    out["A_crossings_seen"] = len(facts.crossings)
    out["A_crossings_refused"] = len(rejected)
    out["A_crossings_joined_by_a_dot"] = sum(
        1 for c in facts.crossings if c.verdict == CONNECTED_JUNCTION)
    out["A_crossings_refused_by_a_hop"] = sum(
        1 for c in facts.crossings if c.verdict == HOP_PROVEN_NON_CONNECTION)
    same_run = 0
    for crossing in rejected:
        left = topology.run_of_edge.get(crossing.edges[0])
        right = topology.run_of_edge.get(crossing.edges[1])
        if left is not None and left == right:
            same_run += 1
    out["A_refused_crossings_whose_two_edges_share_a_run"] = same_run

    # B — a table lattice is not topology.
    out["B_table_grid_edges"] = int((nature == TABLE_GRID).sum())
    out["B_table_grid_edges_that_conduct"] = int(((nature == TABLE_GRID) & conductor).sum())

    # C — a drawing frame is not a bus.
    out["C_frame_edges"] = int((nature == FRAME).sum())
    out["C_frame_edges_that_conduct"] = int(((nature == FRAME) & conductor).sum())
    bus_nodes = [node for node in topology.nodes if node.node_kind == BUS]
    out["C_bus_nodes"] = len(bus_nodes)
    page_area = max(data.width * data.height, 1e-6)
    out["C_bus_nodes_spanning_the_sheet"] = sum(
        1 for node in bus_nodes
        if (node.bbox[2] - node.bbox[0]) * (node.bbox[3] - node.bbox[1]) >= 0.55 * page_area
    )

    # D — a rule under a word is not a conductor.
    out["D_underline_edges"] = int((nature == TEXT_UNDERLINE).sum())
    out["D_underline_edges_that_conduct"] = int(((nature == TEXT_UNDERLINE) & conductor).sum())

    # E — the nearest conductor names nothing.
    bound_ids = {
        node.node_id for node in topology.nodes if node.node_kind == LABEL_ANCHOR
    }
    claimed_labels = {
        edge.from_node_id for edge in topology.edges
        if edge.connection_claim == PROVEN_CONNECTION and edge.from_node_id in bound_ids
    }
    unbound_near = 0
    if conductor.any():
        conductor_index = np.nonzero(conductor)[0]
        for label in data.labels:
            box = label["bbox"]
            centre = np.array([(box[0] + box[2]) / 2.0, (box[1] + box[3]) / 2.0])
            along = np.where(horizontal[conductor_index], centre[0], centre[1])
            across = np.where(horizontal[conductor_index], centre[1], centre[0])
            inside = np.clip(along, low[conductor_index], high[conductor_index])
            distance = np.hypot(
                np.where(horizontal[conductor_index], inside - centre[0], across - centre[0] * 0),
                0,
            )
            perpendicular = np.abs(across - level[conductor_index])
            lateral = np.abs(along - inside)
            nearest = float(np.min(np.hypot(perpendicular, lateral)))
            if nearest <= NEAR_MISS_EM * float(label["size"]):
                unbound_near += 1
    out["E_labels"] = len(data.labels)
    out["E_labels_with_a_conductor_within_five_ems"] = unbound_near
    out["E_labels_bound_by_a_drawn_relation"] = len(claimed_labels)
    out["E_labels_attributed_by_proximity"] = 0

    # F — independent drawings stay independent.
    islands = defaultdict(list)
    for node in topology.nodes:
        islands[node.island_id].append(node)
    out["F_islands"] = len(islands)
    out["F_islands_carrying_a_bus"] = sum(
        1 for members in islands.values() if any(node.node_kind == BUS for node in members))
    node_island = {node.node_id: node.island_id for node in topology.nodes}
    out["F_proven_edges_between_two_islands"] = sum(
        1 for edge in topology.edges
        if edge.connection_claim == PROVEN_CONNECTION
        and node_island.get(edge.from_node_id) != node_island.get(edge.to_node_id)
    )

    # G — one block drawn twice is two nodes.
    signatures = Counter(
        node.symbol_signature for node in topology.nodes if node.symbol_signature)
    repeated = {key for key, count in signatures.items() if count > 1}
    out["G_symbol_signatures"] = len(signatures)
    out["G_signatures_used_more_than_once"] = len(repeated)
    out["G_nodes_sharing_a_signature"] = sum(
        count for key, count in signatures.items() if key in repeated)
    identities = defaultdict(set)
    for node in topology.nodes:
        if node.symbol_signature in repeated:
            identities[node.symbol_signature].add(node.node_id)
    out["G_distinct_nodes_behind_those_signatures"] = sum(
        len(value) for value in identities.values())
    return out


def graph_consistency(topology: PageTopology) -> dict[str, int]:
    """Structural checks on the finished graph of one page."""
    known = {node.node_id for node in topology.nodes}
    page_of = {node.node_id: node.physical_page for node in topology.nodes}
    out = {
        "edges": len(topology.edges),
        "edges_naming_a_node_outside_the_graph": 0,
        "edges_spanning_two_pages": 0,
        "edges_claiming_without_evidence": 0,
        "labels_bound_to_two_nodes": 0,
        "nodes_without_an_island": 0,
        "proven_edges": 0,
        "no_claim_edges": 0,
    }
    seen: dict[str, str] = {}
    for edge in topology.edges:
        if edge.from_node_id not in known or edge.to_node_id not in known:
            out["edges_naming_a_node_outside_the_graph"] += 1
            continue
        if page_of[edge.from_node_id] != page_of[edge.to_node_id]:
            out["edges_spanning_two_pages"] += 1
        if edge.connection_claim == PROVEN_CONNECTION:
            out["proven_edges"] += 1
            if not (edge.junction_evidence or edge.binding_channel or edge.geometry_refs):
                out["edges_claiming_without_evidence"] += 1
            if edge.binding_channel:
                previous = seen.get(edge.from_node_id)
                if previous is not None and previous != edge.to_node_id:
                    out["labels_bound_to_two_nodes"] += 1
                seen[edge.from_node_id] = edge.to_node_id
        elif edge.connection_claim == NO_CLAIM:
            out["no_claim_edges"] += 1
    out["nodes_without_an_island"] = sum(1 for node in topology.nodes if not node.island_id)
    return out


def summarize(rows: Iterable[Mapping[str, int]]) -> dict[str, int]:
    total: dict[str, int] = {}
    for row in rows:
        _accumulate(total, row)
    return dict(sorted(total.items()))


__all__ = ["NEAR_MISS_EM", "graph_consistency", "page_controls", "summarize"]
