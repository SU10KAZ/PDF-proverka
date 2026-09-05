"""One level up: from wires to the thing an engineer names.

The unit is the **maximal proven-connected component** of one physical page —
V2's island, taken exactly as V2 left it.  The choice is not stylistic.  It is
the only grouping on offer whose boundary is proven rather than chosen: control F
of V2 measured *zero* proven edges between two islands, so nothing drawn crosses
the boundary of a component, and no threshold decides where it ends.

Three things are then read off the component, and each one is read off the
*drawing* rather than off a page:

* whether it contains a proven **bus** — a point that distributes rather than
  links.  A component with a bus is board-shaped; one without is a piece.
* which printed **marks** name it.  A mark names the component only when every
  node it is bound to lies inside that one component.  A mark whose bound nodes
  straddle two components names neither, and above all does not join them.
* which of its feeders end **free** — the drawn ports of the aggregate, the
  nearest honest analogue of "outgoing lines" on a sheet whose arrowheads are
  almost never drawn.

The bus-anchored grouping the track asks about in §5 is computed here too, and
deliberately *not* used as the unit.  The measurement explains itself: a board's
distribution point is not one bus but a stack of parallel bars, so grouping by a
single bus turns one board into several overlapping groups covering the same
feeders.  That number is reported instead of an opinion.
"""
from __future__ import annotations

from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v2 import instance_identity as production_marks
from experiments.pdf_evidence_v2.contract import (
    BUS,
    CONNECTOR,
    EQUIPMENT,
    FEEDER,
    JUNCTION,
    LABEL_ANCHOR,
    LABEL_CONNECTION,
    PROVEN_CONNECTION,
    TABLE_PORT,
    TERMINAL,
)
from experiments.pdf_evidence_v2.pipeline import PageResult
from experiments.pdf_evidence_v2.topology import PageTopology

from .contract import (
    AMBIGUOUS,
    BUS_ANCHORED_GROUP,
    COMMON_OWNER_LABEL,
    CONNECTED_COMPONENT,
    FunctionTopologySubgraph,
    PARTIAL,
    PROVEN,
    REPEATED_LABEL_ACROSS_SUBGRAPHS,
    UNKNOWN,
    stable_id,
)

#: A component smaller than this, named by nothing, is not a thing yet.
TRIVIAL_MEMBERS = 2


@dataclass
class PageAggregation:
    """Every aggregate of one physical page, with the tables to read them."""

    document: str
    physical_page: int
    subgraphs: list[FunctionTopologySubgraph] = field(default_factory=list)
    subgraph_of_node: dict[str, str] = field(default_factory=dict)
    nodes_of_mark: dict[str, list[str]] = field(default_factory=dict)
    mark_ownership: dict[str, str] = field(default_factory=dict)
    sheet_marks: set[str] = field(default_factory=set)
    bus_groups: list[dict[str, Any]] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)


def _electrical_edges(topology: PageTopology) -> list[Any]:
    kinds = {node.node_id: node.node_kind for node in topology.nodes}
    out = []
    for edge in topology.edges:
        if edge.connection_claim != PROVEN_CONNECTION:
            continue
        if edge.edge_kind == LABEL_CONNECTION:
            continue
        if LABEL_ANCHOR in {kinds.get(edge.from_node_id), kinds.get(edge.to_node_id)}:
            continue
        out.append(edge)
    return out


def _adjacency(edges: Sequence[Any]) -> dict[str, set[str]]:
    adjacency: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        adjacency[edge.from_node_id].add(edge.to_node_id)
        adjacency[edge.to_node_id].add(edge.from_node_id)
    return adjacency


def _marks_of(text: str) -> list[str]:
    """The production extractor, both sides of every comparison, always.

    V2 measured what happens otherwise: the CAD font hands back ``ГPЩ1`` with a
    Latin ``P``, and raw strings never match.
    """
    return [str(row["mark"]) for row in production_marks.extract_marks(str(text))]


def bound_marks_by_node(topology: PageTopology) -> dict[str, list[str]]:
    """Marks each node carries through a *proven* label binding, and nothing else."""
    nodes = topology.node_by_id()
    out: dict[str, list[str]] = defaultdict(list)
    for edge in topology.edges:
        if edge.edge_kind != LABEL_CONNECTION or edge.connection_claim != PROVEN_CONNECTION:
            continue
        anchor = nodes.get(edge.from_node_id)
        if anchor is None:
            continue
        for text in anchor.labels:
            out[edge.to_node_id].extend(_marks_of(text))
    return {key: sorted(set(value)) for key, value in out.items()}


def bound_texts_by_node(topology: PageTopology) -> dict[str, list[str]]:
    nodes = topology.node_by_id()
    out: dict[str, list[str]] = defaultdict(list)
    for edge in topology.edges:
        if edge.edge_kind != LABEL_CONNECTION or edge.connection_claim != PROVEN_CONNECTION:
            continue
        anchor = nodes.get(edge.from_node_id)
        if anchor is None:
            continue
        out[edge.to_node_id].extend(str(text) for text in anchor.labels)
    return {key: sorted(set(value)) for key, value in out.items()}


def label_ids_by_node(topology: PageTopology) -> dict[str, list[str]]:
    nodes = topology.node_by_id()
    out: dict[str, list[str]] = defaultdict(list)
    for edge in topology.edges:
        if edge.edge_kind != LABEL_CONNECTION or edge.connection_claim != PROVEN_CONNECTION:
            continue
        anchor = nodes.get(edge.from_node_id)
        if anchor is None:
            continue
        for reference in anchor.evidence_refs:
            if reference.startswith("label:"):
                out[edge.to_node_id].append(reference.split(":", 1)[1])
    return {key: sorted(set(value)) for key, value in out.items()}


def sheet_marks(result: PageResult) -> set[str]:
    """Every mark printed anywhere on the sheet — a sheet-scoped presence.

    True about the sheet and silent about which wire owns it.  It is allowed to
    *support* a binding and never to prove one.
    """
    found: set[str] = set()
    for label in result.data.labels:
        found.update(_marks_of(str(label["text"])))
    return found


def bus_anchored_groups(topology: PageTopology) -> list[dict[str, Any]]:
    """§5, measured rather than believed.

    For every bus, the feeders that reach it without passing through another
    bus.  The groups are reported as sets and never as a partition, because on a
    three-phase board they overlap by construction: the bars are drawn as
    separate runs and a feeder taps several of them.
    """
    edges = _electrical_edges(topology)
    adjacency = _adjacency(edges)
    kinds = {node.node_id: node.node_kind for node in topology.nodes}
    buses = sorted(node.node_id for node in topology.nodes if node.node_kind == BUS)
    groups: list[dict[str, Any]] = []
    for bus in buses:
        reached: set[str] = {bus}
        queue: deque[str] = deque([bus])
        while queue:
            current = queue.popleft()
            for neighbour in sorted(adjacency.get(current, ())):
                if neighbour in reached:
                    continue
                if kinds.get(neighbour) == BUS:
                    reached.add(neighbour)
                    continue  # another bus terminates this walk
                reached.add(neighbour)
                queue.append(neighbour)
        feeders = sorted(node for node in reached if kinds.get(node) == FEEDER)
        groups.append({
            "bus_node_id": bus,
            "member_count": len(reached),
            "feeder_node_ids": feeders,
            "feeder_count": len(feeders),
        })
    return groups


def aggregate_page(result: PageResult) -> PageAggregation:
    """Every function-level aggregate of one physical page."""
    topology = result.topology
    document, page = topology.document, topology.page
    edges = _electrical_edges(topology)
    adjacency = _adjacency(edges)
    electrical = [node for node in topology.nodes if node.node_kind != LABEL_ANCHOR]
    kinds = {node.node_id: node.node_kind for node in electrical}
    marks_by_node = bound_marks_by_node(topology)
    texts_by_node = bound_texts_by_node(topology)
    labels_by_node = label_ids_by_node(topology)

    # components, grouped by the island V2 already proved
    members_of_island: dict[str, list[str]] = defaultdict(list)
    for node in electrical:
        members_of_island[node.island_id or f"i:{page:04d}:orphan"].append(node.node_id)
    edges_of_island: dict[str, list[str]] = defaultdict(list)
    island_of_node = {
        node_id: island for island, ids in members_of_island.items() for node_id in ids
    }
    endpoints: dict[str, tuple[str, str]] = {}
    for edge in edges:
        endpoints[edge.edge_id] = (edge.from_node_id, edge.to_node_id)
        island = island_of_node.get(edge.from_node_id)
        if island is not None and island == island_of_node.get(edge.to_node_id):
            edges_of_island[island].append(edge.edge_id)

    # which component every mark's bound nodes live in
    nodes_of_mark: dict[str, list[str]] = defaultdict(list)
    for node_id, marks in marks_by_node.items():
        if node_id not in island_of_node:
            continue
        for mark in marks:
            nodes_of_mark[mark].append(node_id)
    mark_ownership: dict[str, str] = {}
    for mark, node_ids in nodes_of_mark.items():
        islands = {island_of_node[node_id] for node_id in node_ids}
        mark_ownership[mark] = (
            COMMON_OWNER_LABEL if len(islands) == 1 else REPEATED_LABEL_ACROSS_SUBGRAPHS
        )

    counters: Counter = Counter()
    subgraphs: list[FunctionTopologySubgraph] = []
    subgraph_of_node: dict[str, str] = {}
    for island in sorted(members_of_island):
        member_ids = sorted(members_of_island[island])
        edge_ids = sorted(edges_of_island.get(island, []))
        buses = [node_id for node_id in member_ids if kinds.get(node_id) == BUS]
        feeders = [node_id for node_id in member_ids if kinds.get(node_id) == FEEDER]
        equipment = [node_id for node_id in member_ids if kinds.get(node_id) == EQUIPMENT]
        terminals = [
            node_id for node_id in member_ids if kinds.get(node_id) in {TERMINAL, TABLE_PORT}
        ]
        own_marks = sorted({
            mark for node_id in member_ids for mark in marks_by_node.get(node_id, ())
            if mark_ownership.get(mark) == COMMON_OWNER_LABEL
        })
        shared_marks = sorted({
            mark for node_id in member_ids for mark in marks_by_node.get(node_id, ())
            if mark_ownership.get(mark) == REPEATED_LABEL_ACROSS_SUBGRAPHS
        })
        named = any(node_id in marks_by_node for node_id in member_ids)

        if buses:
            boundary = PROVEN
        elif len(member_ids) < TRIVIAL_MEMBERS and not named:
            boundary = UNKNOWN
        else:
            boundary = PARTIAL

        # a single drawn link holding two bus-bearing halves together: the sheet
        # may have tied two boards rather than drawn one, and this layer will not
        # choose between the readings.  The test is structural on purpose — the
        # naming test that preceded it could be bridged by a section code printed
        # on both halves (``ГРЩ1-РП1-1`` and ``ГРЩ2-РП1-1`` share ``РП1``), which
        # is exactly the "repeated convention" §6 warns about.
        bridges = _bus_bearing_bridges(member_ids, edge_ids, endpoints, buses)
        if boundary == PROVEN and bridges:
            boundary = AMBIGUOUS
        families = _contesting_families(own_marks, marks_by_node, texts_by_node, member_ids)

        subgraph_id = stable_id(
            "fts",
            {
                "document": document,
                "physical_page": page,
                "aggregation_channel": CONNECTED_COMPONENT,
                "member_node_ids": member_ids,
            },
        )
        for node_id in member_ids:
            subgraph_of_node[node_id] = subgraph_id
        consumer_texts = sorted({
            text for node_id in member_ids for text in texts_by_node.get(node_id, ())
        })
        subgraphs.append(FunctionTopologySubgraph(
            subgraph_id=subgraph_id,
            document=document,
            physical_page=page,
            aggregation_channel=CONNECTED_COMPONENT,
            boundary_status=boundary,
            member_node_ids=tuple(member_ids),
            member_edge_ids=tuple(edge_ids),
            bus_node_ids=tuple(buses),
            feeder_node_ids=tuple(feeders),
            equipment_node_ids=tuple(equipment),
            terminal_node_ids=tuple(terminals),
            source_region_ids=tuple(sorted({
                node.region_id for node in electrical
                if node.node_id in set(member_ids) and node.region_id
            })),
            label_evidence_ids=tuple(sorted({
                label_id for node_id in member_ids
                for label_id in labels_by_node.get(node_id, ())
            })),
            function_marks=tuple(own_marks),
            consumer_labels=tuple(consumer_texts),
            source_labels=tuple(shared_marks),
            evidence_refs=(f"island:{island}", f"proven_edges:{len(edge_ids)}"),
            notes=tuple(
                [
                    f"naming_families={len(families)}",
                    f"bus_bearing_halves_joined_by_one_link={bridges}",
                ]
                + ([f"contesting_families={','.join(sorted(families))}"]
                   if len(families) > 1 else [])
            ),
        ))
        counters[f"boundary_{boundary}"] += 1
        counters["subgraphs"] += 1
        counters["members"] += len(member_ids)

    counters["marks_owning_one_subgraph"] = sum(
        1 for value in mark_ownership.values() if value == COMMON_OWNER_LABEL)
    counters["marks_spanning_subgraphs"] = sum(
        1 for value in mark_ownership.values() if value == REPEATED_LABEL_ACROSS_SUBGRAPHS)

    return PageAggregation(
        document=document,
        physical_page=page,
        subgraphs=subgraphs,
        subgraph_of_node=subgraph_of_node,
        nodes_of_mark={key: sorted(value) for key, value in sorted(nodes_of_mark.items())},
        mark_ownership=dict(sorted(mark_ownership.items())),
        sheet_marks=sheet_marks(result),
        bus_groups=bus_anchored_groups(topology),
        counters=dict(counters),
    )


def _bus_bearing_bridges(
    member_ids: Sequence[str],
    edge_ids: Sequence[str],
    endpoints: Mapping[str, tuple[str, str]],
    buses: Sequence[str],
) -> int:
    """Drawn links whose removal would leave two halves that each hold a bus.

    A bridge is a fact of the drawing — one stroke joining two otherwise separate
    halves — and it is the one shape that distinguishes a board with several
    parallel bars (cross-linked many ways, no bridge at all) from two boards tied
    together by a single link.  Neither reading is chosen here; the aggregate is
    marked ``AMBIGUOUS`` and keeps every member it has.
    """
    if len(buses) < 2:
        return 0
    adjacency: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for edge_id in edge_ids:
        pair = endpoints.get(edge_id)
        if pair is None:
            continue
        adjacency[pair[0]].append((pair[1], edge_id))
        adjacency[pair[1]].append((pair[0], edge_id))
    order: dict[str, int] = {}
    low: dict[str, int] = {}
    counter = 0
    found = 0
    bus_set = set(buses)
    subtree_buses: dict[str, int] = defaultdict(int)
    total_buses = len(bus_set)
    for root in member_ids:
        if root in order:
            continue
        stack: list[tuple[str, str | None, int]] = [(root, None, 0)]
        while stack:
            node, parent_edge, state = stack.pop()
            if state == 0:
                if node in order:
                    continue
                counter += 1
                order[node] = low[node] = counter
                subtree_buses[node] = 1 if node in bus_set else 0
                stack.append((node, parent_edge, 1))
                for neighbour, edge_id in adjacency.get(node, ()):
                    if edge_id == parent_edge:
                        continue
                    if neighbour in order:
                        low[node] = min(low[node], order[neighbour])
                    else:
                        stack.append((neighbour, edge_id, 0))
            else:
                for neighbour, edge_id in adjacency.get(node, ()):
                    if edge_id == parent_edge or neighbour not in order:
                        continue
                    if order[neighbour] > order[node]:
                        low[node] = min(low[node], low[neighbour])
                        subtree_buses[node] += subtree_buses[neighbour]
                        if low[neighbour] > order[node]:
                            inside = subtree_buses[neighbour]
                            if 0 < inside < total_buses:
                                found += 1
    return found


def _contesting_families(
    marks: Sequence[str],
    marks_by_node: Mapping[str, Sequence[str]],
    texts_by_node: Mapping[str, Sequence[str]],
    member_ids: Sequence[str],
) -> set[str]:
    """The naming families that could each be claiming this whole component.

    Two rules, and both were forced by the control sheet rather than chosen.

    *Naming one wire is not claiming the board.*  ``ХМ1`` and ``ХМ2`` are printed
    on the control sheet along exactly one feeder each: two chillers fed **by**
    the board, not two rivals for its identity.  ``ЩНО`` likewise.  A designation
    claims more than a wire only when it is bound to two or more members — which
    is what claiming a drawn whole looks like.

    *Co-printing unites.*  ``ГРЩ1-РП1-15 ППГнг(А)-HF 5х185мм²`` prints a board, a
    section and a cable code in one designation, so they are one family: the
    draughtsman wrote them as one designation.  Two boards welded by a tie print
    their names apart, and that is the case this test exists to flag.
    """
    nodes_of_mark: dict[str, set[str]] = defaultdict(set)
    for node_id in member_ids:
        for mark in marks_by_node.get(node_id, ()):
            if mark in set(marks):
                nodes_of_mark[mark].add(node_id)
    contesting = {mark for mark, nodes in nodes_of_mark.items() if len(nodes) >= 2}
    if len(contesting) < 2:
        return {_series(mark) for mark in contesting}
    series_of_mark = {mark: _series(mark) for mark in contesting}
    parent = {series: series for series in set(series_of_mark.values())}

    def find(item: str) -> str:
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    for node_id in member_ids:
        for text in texts_by_node.get(node_id, ()):
            present = sorted({
                series_of_mark[mark] for mark in _marks_of(text) if mark in series_of_mark
            })
            for other in present[1:]:
                a, b = find(present[0]), find(other)
                if a != b:
                    parent[a] = b
    return {find(series) for series in set(series_of_mark.values())}


def _series(mark: str) -> str:
    digits = "".join(character for character in mark if not character.isdigit())
    return digits or mark


__all__ = [
    "PageAggregation", "TRIVIAL_MEMBERS", "aggregate_page", "bound_marks_by_node",
    "bound_texts_by_node", "bus_anchored_groups", "label_ids_by_node", "sheet_marks",
]
