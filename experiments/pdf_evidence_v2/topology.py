"""From proven strokes to a graph of things.

An edge of welded ink is not a thing an engineer talks about.  A *run* is: one
wire, drawn as a polyline, possibly interrupted by hops.  So the graph is built
one level above the strokes.

Two rules decide where a run ends, and both are the drawing's, not a
parameter's:

* two conductor pieces meeting end to end, **and nobody else meeting there**,
  are one wire that turns a corner or jumps a hop;
* three or more pieces meeting at one point is a branch, and a branch is a
  node, not a bend.

Everything else follows.  A run carrying three or more junction dots is
distributing to several places and is a **bus**; three is the smallest count
that means "shared" rather than "linked".  A dot joining exactly two runs is a
splice, a **connector**, not a device.  A symbol cluster with two or more
conductor terminals is **equipment** in series; with one, it is a **terminal**.
A lattice a conductor ends on is a **table port** — the little property table a
draughtsman hangs on the end of a feeder.

Nothing in this module knows what any of it is *called*.  Naming arrives later
and separately, and a node whose name never arrives keeps working.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from . import symbols as symbols_module
from .conductors import EdgeFacts, _edge_arrays, _region_kind_by_component
from .contract import (
    BRANCH,
    BUS,
    CONNECTOR,
    CONTINUOUS_POLYLINE,
    COINCIDENT_ENDPOINTS,
    ELECTRICAL_CONNECTION,
    EQUIPMENT,
    EQUIPMENT_PORT,
    FEEDER,
    JUNCTION,
    JUNCTION_DOT,
    LABEL_ANCHOR,
    NO_CLAIM,
    PROVEN_CONNECTION,
    TABLE_PORT,
    TABLE_REFERENCE,
    TEE_TERMINATION,
    TERMINAL,
    TopologyEdge,
    TopologyNode,
    UNDIRECTED,
    UNNAMED,
)
from .page import PageData

#: A run carrying at least this many junction dots distributes rather than
#: links.  Two is a wire between two places; three is a bus.
BUS_MIN_DOTS = 3
#: Points are quantized to this grid when grouping meetings, in points.
MEET_GRID = 1.0


@dataclass
class PageTopology:
    """The graph of one physical page."""

    document: str
    page: int
    nodes: list[TopologyNode] = field(default_factory=list)
    edges: list[TopologyEdge] = field(default_factory=list)
    run_of_edge: dict[int, int] = field(default_factory=dict)
    node_of_run: dict[int, str] = field(default_factory=dict)
    node_of_cluster: dict[int, str] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)

    def node_by_id(self) -> dict[str, TopologyNode]:
        return {node.node_id: node for node in self.nodes}


class _Union:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        a, b = self.find(left), self.find(right)
        if a != b:
            self.parent[max(a, b)] = min(a, b)


def _key(point: Sequence[float]) -> tuple[int, int]:
    return (int(round(float(point[0]) / MEET_GRID)), int(round(float(point[1]) / MEET_GRID)))


def build_runs(data: PageData, facts: EdgeFacts) -> tuple[dict[int, int], dict[tuple[int, int], set[int]]]:
    """Group conductor edges into wires.

    A meeting joins two pieces into one wire only when exactly those two pieces
    meet there.  Where three meet, the meeting is a branch and stays a node —
    merging it would erase the branch and hand back a graph in which a bus and
    its feeder are the same object.
    """
    conductor = facts.conductor
    indices = np.nonzero(conductor)[0]
    union = _Union(len(data.strokes.edges))
    meetings: dict[tuple[int, int], set[int]] = defaultdict(set)
    for junction in facts.junctions:
        members = [index for index in junction.edges if conductor[index]]
        if not members:
            continue
        if junction.evidence in {COINCIDENT_ENDPOINTS, CONTINUOUS_POLYLINE}:
            meetings[_key(junction.point)].update(members)
    for point, members in meetings.items():
        if len(members) == 2:
            first, second = sorted(members)
            union.union(first, second)
    run_of_edge = {int(index): union.find(int(index)) for index in indices}
    return run_of_edge, meetings


def build_page(
    data: PageData, facts: EdgeFacts, *, arrowheads: Mapping[int, str] | None = None
) -> PageTopology:
    """Build the topology graph of one physical page."""
    edges_array, horizontal, low, high, level = _edge_arrays(data)
    conductor = facts.conductor
    run_of_edge, meetings = build_runs(data, facts)
    members_of_run: dict[int, list[int]] = defaultdict(list)
    for edge_index, run in run_of_edge.items():
        members_of_run[run].append(edge_index)

    dots_on_run: dict[int, int] = defaultdict(int)
    for junction in facts.junctions:
        if junction.evidence != JUNCTION_DOT:
            continue
        for run in {run_of_edge[index] for index in junction.edges if index in run_of_edge}:
            dots_on_run[run] += 1

    ordered_runs = sorted(
        members_of_run,
        key=lambda run: (
            round(float(min(min(edges_array[i][1], edges_array[i][3]) for i in members_of_run[run])), 2),
            round(float(min(min(edges_array[i][0], edges_array[i][2]) for i in members_of_run[run])), 2),
            run,
        ),
    )

    nodes: list[TopologyNode] = []
    node_of_run: dict[int, str] = {}
    counters: dict[str, int] = defaultdict(int)
    for position, run in enumerate(ordered_runs):
        rows = edges_array[members_of_run[run]]
        bbox = (
            float(min(rows[:, 0].min(), rows[:, 2].min())),
            float(min(rows[:, 1].min(), rows[:, 3].min())),
            float(max(rows[:, 0].max(), rows[:, 2].max())),
            float(max(rows[:, 1].max(), rows[:, 3].max())),
        )
        kind = BUS if dots_on_run.get(run, 0) >= BUS_MIN_DOTS else FEEDER
        node_id = f"n:{data.page:04d}:r{position:05d}"
        node_of_run[run] = node_id
        nodes.append(TopologyNode(
            node_id=node_id,
            document=data.document,
            physical_page=data.page,
            node_kind=kind,
            bbox=bbox,
            anchor=((bbox[0] + bbox[2]) / 2.0, (bbox[1] + bbox[3]) / 2.0),
            evidence_refs=tuple(
                data.strokes.geometry_ref(index) for index in sorted(members_of_run[run])[:16]
            ),
            notes=(f"pieces={len(members_of_run[run])}", f"dots={dots_on_run.get(run, 0)}"),
        ))
        counters[f"nodes_{kind}"] += 1

    graph_edges: list[TopologyEdge] = []

    def mint(prefix: str) -> str:
        return f"e:{data.page:04d}:{prefix}{len(graph_edges):05d}"

    # junctions between different runs
    grouped: dict[tuple[int, int], dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
    for junction in facts.junctions:
        runs = {run_of_edge[index] for index in junction.edges if index in run_of_edge}
        if len(runs) < 2:
            continue
        grouped[_key(junction.point)][junction.evidence].update(runs)
    junction_nodes = 0
    for point_key in sorted(grouped):
        for evidence in sorted(grouped[point_key]):
            runs = sorted(grouped[point_key][evidence])
            if len(runs) < 2:
                continue
            point = (point_key[0] * MEET_GRID, point_key[1] * MEET_GRID)
            kind = JUNCTION if len(runs) >= 3 else CONNECTOR
            if evidence == JUNCTION_DOT and len(runs) == 2:
                kind = CONNECTOR
            node_id = f"n:{data.page:04d}:j{junction_nodes:05d}"
            junction_nodes += 1
            nodes.append(TopologyNode(
                node_id=node_id,
                document=data.document,
                physical_page=data.page,
                node_kind=kind,
                bbox=(point[0] - 1.0, point[1] - 1.0, point[0] + 1.0, point[1] + 1.0),
                anchor=point,
                evidence_refs=(f"junction:{evidence}",),
                notes=(f"runs={len(runs)}",),
            ))
            counters[f"nodes_{kind}"] += 1
            for run in runs:
                bus_side = nodes[[node.node_id for node in nodes].index(node_of_run[run])].node_kind == BUS
                graph_edges.append(TopologyEdge(
                    edge_id=mint("c"),
                    document=data.document,
                    physical_page=data.page,
                    from_node_id=node_of_run[run],
                    to_node_id=node_id,
                    edge_kind=BRANCH if (bus_side and evidence == JUNCTION_DOT) else ELECTRICAL_CONNECTION,
                    connection_claim=PROVEN_CONNECTION,
                    direction_status=UNDIRECTED,
                    junction_evidence=evidence,
                    geometry_refs=(f"point:{round(point[0], 2)},{round(point[1], 2)}",),
                ))

    # symbol clusters that touch conductors
    node_of_cluster: dict[int, str] = {}
    for cluster in facts.clusters:
        terminals = [index for index in facts.cluster_terminals.get(cluster.index, ())
                     if conductor[index]]
        if not terminals:
            continue
        runs = sorted({run_of_edge[index] for index in terminals})
        kind = EQUIPMENT if len(runs) >= 2 else TERMINAL
        node_id = f"n:{data.page:04d}:s{cluster.index:05d}"
        node_of_cluster[cluster.index] = node_id
        nodes.append(TopologyNode(
            node_id=node_id,
            document=data.document,
            physical_page=data.page,
            node_kind=kind,
            bbox=cluster.bbox,
            anchor=cluster.centre,
            symbol_signature=cluster.signature,
            evidence_refs=(f"cluster:{cluster.index}", f"strokes:{cluster.strokes}"),
            notes=(f"terminals={len(terminals)}",),
        ))
        counters[f"nodes_{kind}"] += 1
        for run in runs:
            graph_edges.append(TopologyEdge(
                edge_id=mint("p"),
                document=data.document,
                physical_page=data.page,
                from_node_id=node_of_run[run],
                to_node_id=node_id,
                edge_kind=ELECTRICAL_CONNECTION,
                connection_claim=PROVEN_CONNECTION,
                direction_status=UNDIRECTED,
                junction_evidence=EQUIPMENT_PORT,
                geometry_refs=(f"cluster:{cluster.index}",),
            ))

    # the device that made a break in a line
    for position, gap in enumerate(facts.series_gaps):
        if not (conductor[gap.axis_low] and conductor[gap.axis_high]):
            continue
        runs = sorted({run_of_edge[gap.axis_low], run_of_edge[gap.axis_high]})
        if len(runs) < 2:
            continue
        if gap.clusters and all(index in node_of_cluster for index in gap.clusters):
            # the ink already stands as a node of its own with both ports
            existing = {node_of_cluster[index] for index in gap.clusters}
            if any(
                {edge.from_node_id, edge.to_node_id} == {node_of_run[runs[0]], target}
                for target in existing for edge in graph_edges
            ):
                continue
        rows = edges_array[[gap.axis_low, gap.axis_high]]
        bbox = (
            float(min(rows[:, 0].min(), rows[:, 2].min())),
            float(min(rows[:, 1].min(), rows[:, 3].min())),
            float(max(rows[:, 0].max(), rows[:, 2].max())),
            float(max(rows[:, 1].max(), rows[:, 3].max())),
        )
        node_id = f"n:{data.page:04d}:g{position:05d}"
        nodes.append(TopologyNode(
            node_id=node_id,
            document=data.document,
            physical_page=data.page,
            node_kind=EQUIPMENT,
            bbox=bbox,
            anchor=((bbox[0] + bbox[2]) / 2.0, (bbox[1] + bbox[3]) / 2.0),
            symbol_signature=(
                facts.clusters[gap.clusters[0]].signature
                if gap.clusters and gap.clusters[0] < len(facts.clusters) else None
            ),
            evidence_refs=(
                f"series_gap:{round(gap.gap, 2)}", f"ink_cover:{round(gap.covered, 3)}",
            ),
            notes=(f"clusters={len(gap.clusters)}",),
        ))
        counters["nodes_EQUIPMENT"] += 1
        counters["equipment_from_series_gap"] += 1
        for run in runs:
            graph_edges.append(TopologyEdge(
                edge_id=mint("q"),
                document=data.document,
                physical_page=data.page,
                from_node_id=node_of_run[run],
                to_node_id=node_id,
                edge_kind=ELECTRICAL_CONNECTION,
                connection_claim=PROVEN_CONNECTION,
                direction_status=UNDIRECTED,
                junction_evidence=EQUIPMENT_PORT,
                geometry_refs=(f"series_gap:{gap.axis_low}-{gap.axis_high}",),
            ))

    # tables a conductor ends on
    kinds = _region_kind_by_component(data)
    region_bbox: dict[int, tuple[float, float, float, float]] = {}
    for region in data.regions:
        parts = region.region_id.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            region_bbox[int(parts[1])] = tuple(float(value) for value in region.bbox)
    table_hits: dict[int, set[int]] = defaultdict(set)
    for junction in facts.junctions:
        conducting = [index for index in junction.edges if conductor[index]]
        if not conducting:
            continue
        for index in junction.edges:
            component = int(facts.region[index])
            if kinds.get(component) in {"TABLE", "STAMP"} and not conductor[index]:
                for other in conducting:
                    table_hits[component].add(run_of_edge[other])
    for component in sorted(table_hits):
        bbox = region_bbox.get(component)
        if bbox is None:
            continue
        node_id = f"n:{data.page:04d}:b{component:05d}"
        nodes.append(TopologyNode(
            node_id=node_id,
            document=data.document,
            physical_page=data.page,
            node_kind=TABLE_PORT,
            bbox=bbox,
            anchor=((bbox[0] + bbox[2]) / 2.0, (bbox[1] + bbox[3]) / 2.0),
            region_id=f"reg_{data.page:04d}_{component:05d}",
            evidence_refs=(f"region:{component}",),
        ))
        counters["nodes_TABLE_PORT"] += 1
        for run in sorted(table_hits[component]):
            graph_edges.append(TopologyEdge(
                edge_id=mint("b"),
                document=data.document,
                physical_page=data.page,
                from_node_id=node_of_run[run],
                to_node_id=node_id,
                edge_kind=TABLE_REFERENCE,
                connection_claim=PROVEN_CONNECTION,
                direction_status=UNDIRECTED,
                geometry_refs=(f"region:{component}",),
            ))

    topology = PageTopology(
        document=data.document,
        page=data.page,
        nodes=nodes,
        edges=graph_edges,
        run_of_edge=run_of_edge,
        node_of_run=node_of_run,
        node_of_cluster=node_of_cluster,
        counters=dict(counters),
    )
    assign_islands(topology)
    return topology


def assign_islands(topology: PageTopology) -> int:
    """Independent drawings on one sheet stay independent.

    Control F of this track: a page carrying three unrelated schemes must not
    hand back one graph.  Nothing joins them here, and the island identifier
    makes that visible rather than merely true.

    An island is a *drawing*, so label anchors do not form one.  A bound label
    takes the island of the thing it names; a label recorded by alignment alone
    takes it too, and takes no claim with it — the island says where the string
    sits, the edge's ``NO_CLAIM`` says what that is worth.
    """
    electrical = [node for node in topology.nodes if node.node_kind != LABEL_ANCHOR]
    index_of = {node.node_id: position for position, node in enumerate(electrical)}
    union = _Union(len(electrical))
    for edge in topology.edges:
        if edge.connection_claim != PROVEN_CONNECTION:
            continue
        if edge.from_node_id in index_of and edge.to_node_id in index_of:
            union.union(index_of[edge.from_node_id], index_of[edge.to_node_id])
    roots: dict[int, str] = {}
    for position, node in enumerate(electrical):
        root = union.find(position)
        if root not in roots:
            roots[root] = f"i:{topology.page:04d}:{len(roots):04d}"
        node.island_id = roots[root]
    island_of = {node.node_id: node.island_id for node in electrical}
    for edge in topology.edges:
        target = island_of.get(edge.to_node_id)
        if target is None:
            continue
        for node in topology.nodes:
            if node.node_id == edge.from_node_id and node.node_kind == LABEL_ANCHOR:
                node.island_id = target
    topology.counters["islands"] = len(roots)
    return len(roots)


__all__ = ["BUS_MIN_DOTS", "MEET_GRID", "PageTopology", "assign_islands", "build_page", "build_runs"]
