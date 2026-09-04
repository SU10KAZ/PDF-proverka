"""Identity from structure, and the two shapes lineage cares about.

``GROUNDING_LIGHTNING #31`` is a page identity: it distinguishes two functions
by where they were printed.  An engineering identity distinguishes them by what
they are wired to — the class of the thing, the device in series with it, how
far it sits from the bus that feeds it, and how its branch fans out.

Two functions that are genuinely the same installation on two revisions of a
sheet keep that shape when the sheet is re-laid-out; two that merely look alike
do not.  This module computes the shape and, more importantly, measures its
*distinguishing power*: how many distinct signatures a page's feeders actually
have.  A signature that every feeder shares distinguishes nothing, and saying
so is the point of measuring it.

Merge and split are read as shapes too.  A convergence is several runs meeting
at one node and leaving as one; a divergence is the reverse.  Neither is
evidence of a lineage merge on its own — the track's own rule is that a shared
target does not prove a merge — so they are counted as *candidate structures*
and joined to a lineage task only when both of its sides are actually mapped to
a node on this corpus, which is a number this module reports rather than
assumes.
"""
from __future__ import annotations

import hashlib
from collections import Counter, defaultdict, deque
from typing import Any, Iterable, Mapping, Sequence

from .contract import (
    BUS,
    EQUIPMENT,
    FEEDER,
    LABEL_ANCHOR,
    LABEL_CONNECTION,
    NO_CLAIM,
    PROVEN_CONNECTION,
    TopologyEdge,
    TopologyNode,
)
from .topology import PageTopology

#: How far a signature looks.  Two hops reaches the device in series and the
#: node beyond it; further turns every feeder on a board into its whole board.
SIGNATURE_RADIUS = 2
#: Hops searched when measuring the distance to a bus.
BUS_SEARCH_LIMIT = 12


def electrical_adjacency(topology: PageTopology) -> dict[str, set[str]]:
    """Neighbours through proven, non-label edges only."""
    adjacency: dict[str, set[str]] = defaultdict(set)
    kinds = {node.node_id: node.node_kind for node in topology.nodes}
    for edge in topology.edges:
        if edge.connection_claim != PROVEN_CONNECTION:
            continue
        if edge.edge_kind == LABEL_CONNECTION:
            continue
        if kinds.get(edge.from_node_id) == LABEL_ANCHOR or kinds.get(edge.to_node_id) == LABEL_ANCHOR:
            continue
        adjacency[edge.from_node_id].add(edge.to_node_id)
        adjacency[edge.to_node_id].add(edge.from_node_id)
    return adjacency


def hops_to_bus(topology: PageTopology, adjacency: Mapping[str, set[str]]) -> dict[str, int]:
    """Distance from every node to the nearest bus, or absent when unreachable."""
    buses = [node.node_id for node in topology.nodes if node.node_kind == BUS]
    distance: dict[str, int] = {node_id: 0 for node_id in buses}
    queue: deque[tuple[str, int]] = deque((node_id, 0) for node_id in buses)
    while queue:
        current, depth = queue.popleft()
        if depth >= BUS_SEARCH_LIMIT:
            continue
        for neighbour in sorted(adjacency.get(current, ())):
            if neighbour in distance:
                continue
            distance[neighbour] = depth + 1
            queue.append((neighbour, depth + 1))
    return distance


def signatures(topology: PageTopology) -> dict[str, str]:
    """A structural fingerprint per node."""
    adjacency = electrical_adjacency(topology)
    nodes = topology.node_by_id()
    distance = hops_to_bus(topology, adjacency)
    out: dict[str, str] = {}
    for node in topology.nodes:
        if node.node_kind == LABEL_ANCHOR:
            continue
        seen = {node.node_id: 0}
        frontier: deque[tuple[str, int]] = deque([(node.node_id, 0)])
        neighbourhood: list[str] = []
        while frontier:
            current, depth = frontier.popleft()
            if depth >= SIGNATURE_RADIUS:
                continue
            for neighbour in sorted(adjacency.get(current, ())):
                if neighbour in seen:
                    continue
                seen[neighbour] = depth + 1
                other = nodes.get(neighbour)
                if other is not None:
                    neighbourhood.append(
                        f"{depth + 1}:{other.node_kind}:{other.symbol_signature or '-'}"
                    )
                frontier.append((neighbour, depth + 1))
        payload = "|".join([
            node.node_kind,
            node.symbol_signature or "-",
            f"deg={len(adjacency.get(node.node_id, ()))}",
            f"bus={distance.get(node.node_id, -1)}",
            ";".join(sorted(neighbourhood)),
        ])
        out[node.node_id] = "top_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return out


def distinguishing_power(topology: PageTopology, fingerprints: Mapping[str, str]) -> dict[str, Any]:
    """How much a structural identity actually separates same-class nodes."""
    by_kind: dict[str, list[str]] = defaultdict(list)
    for node in topology.nodes:
        if node.node_id in fingerprints:
            by_kind[node.node_kind].append(fingerprints[node.node_id])
    out: dict[str, Any] = {}
    for kind, values in sorted(by_kind.items()):
        counts = Counter(values)
        out[kind] = {
            "nodes": len(values),
            "distinct_signatures": len(counts),
            "largest_group": max(counts.values()) if counts else 0,
            "singletons": sum(1 for value in counts.values() if value == 1),
        }
    return out


def convergences(topology: PageTopology) -> dict[str, Any]:
    """Fan-in and fan-out structures, counted and never interpreted.

    A node where several runs meet and one leaves has the shape a merge would
    have.  It is not a merge: on a single sheet it is a busbar tap, a terminal
    block or a three-way tee.  The track's own rule — a shared target does not
    prove a merge — applies here unchanged, so these are candidate structures
    and are reported as such.
    """
    adjacency = electrical_adjacency(topology)
    nodes = topology.node_by_id()
    fan: Counter[int] = Counter()
    equipment_fan: Counter[int] = Counter()
    examples: list[dict[str, Any]] = []
    for node_id, neighbours in sorted(adjacency.items()):
        node = nodes.get(node_id)
        if node is None or node.node_kind == LABEL_ANCHOR:
            continue
        runs = [
            neighbour for neighbour in neighbours
            if nodes.get(neighbour) is not None and nodes[neighbour].node_kind in {FEEDER, BUS}
        ]
        fan[len(runs)] += 1
        if node.node_kind == EQUIPMENT:
            equipment_fan[len(runs)] += 1
            if len(runs) >= 3 and len(examples) < 8:
                examples.append({
                    "node_id": node_id,
                    "runs": len(runs),
                    "bbox": [round(float(value), 2) for value in node.bbox],
                })
    return {
        "fan_by_degree": {str(key): value for key, value in sorted(fan.items())},
        "equipment_fan_by_degree": {str(key): value for key, value in sorted(equipment_fan.items())},
        "convergence_candidates": sum(value for key, value in fan.items() if key >= 3),
        "series_pairs": int(fan.get(2, 0)),
        "examples": examples,
        "rule": "a convergence is a shape, not a merge; a shared target proves nothing",
    }


def bound_marks(topology: PageTopology) -> dict[str, list[str]]:
    """Every printed string a node carries by a proven binding."""
    nodes = topology.node_by_id()
    out: dict[str, list[str]] = defaultdict(list)
    for edge in topology.edges:
        if edge.edge_kind != LABEL_CONNECTION or edge.connection_claim != PROVEN_CONNECTION:
            continue
        anchor = nodes.get(edge.from_node_id)
        if anchor is None:
            continue
        out[edge.to_node_id].extend(str(text) for text in anchor.labels)
    return {key: sorted(set(value)) for key, value in sorted(out.items())}


def aligned_marks(topology: PageTopology) -> dict[str, list[str]]:
    """Strings recorded beside a node by alignment, claiming nothing."""
    nodes = topology.node_by_id()
    out: dict[str, list[str]] = defaultdict(list)
    for edge in topology.edges:
        if edge.connection_claim != NO_CLAIM:
            continue
        anchor = nodes.get(edge.from_node_id)
        if anchor is None:
            continue
        out[edge.to_node_id].extend(str(text) for text in anchor.labels)
    return {key: sorted(set(value)) for key, value in sorted(out.items())}


__all__ = [
    "BUS_SEARCH_LIMIT", "SIGNATURE_RADIUS", "aligned_marks", "bound_marks",
    "convergences", "distinguishing_power", "electrical_adjacency",
    "hops_to_bus", "signatures",
]
