"""Direction, and the one word that must not be allowed to supply it.

A connection proven is not a direction proven.  Only a drawn arrow states which
way power flows on a schematic, so ``DIRECTION_EVIDENCE`` in the contract has
exactly one member and this module is the only thing that may produce it.

The rule exists because of a trap this corpus sets in plain sight.  The
strongest-looking textual signal is the word **Ввод** ("incoming"), and on the
control page every feeder leaving the main board carries it: ``ВРУ1 ввод 1 –
Корпус 1,2``.  The word is true — the line *is* an incoming feeder — for the
switchboard at its far end, which is not on this sheet.  A rule that read it
locally would turn every outgoing line of ГРЩ1 into an incoming one and invert
the sheet.

The demonstration is deterministic and needs no semantics: the same conductor
carries a second bound label, ``ГРЩ1-РП1-1 …``, which names it as line 1 of
board ГРЩ1.  One wire, two names, opposite ends.  The module counts how many
edges a keyword rule would have directed and how many of those carry such a
co-bound counter-name, and then directs none of them.

Arrowheads are read in both forms this corpus draws: the filled triangle, whose
painted scanlines only ever widen, and the open triangle, three strokes meeting
in a closed loop with at least one of them slanted.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from .conductors import EdgeFacts, TOUCH_TOL, _edge_arrays
from .contract import (
    ARROWHEAD,
    BOUND,
    ELECTRICAL_CONNECTION,
    FEED,
    PROVEN,
    PROVEN_CONNECTION,
    TopologyEdge,
)
from .page import PageData
from .topology import PageTopology

#: An arrowhead is small.  Larger triangular ink is a symbol, not a marker.
ARROW_MAX_SIZE = 24.0
#: …and it must sit on the conductor it directs.
ARROW_TOL = 2.0

#: Words that name a direction *relative to something*, which is exactly why
#: they may not supply one here.  Held as a closed list so the measurement of
#: "what a keyword rule would have done" is reproducible.
INCOMING_TERMS = ("ввод", "вводы", "питание от", "от щ", "входящ")
OUTGOING_TERMS = ("отходящ", "к щ", "потребител", "нагрузк")
_WORD = re.compile(r"[a-zа-яё]+", re.IGNORECASE)
#: A board designation: the first token carries a hyphen and mixes letters with
#: digits, as ``ГРЩ1-РП1-3`` does and ``223.2кВт`` does not.  It is a shape
#: test, not a dictionary, so it needs no list of board names to maintain.
_DESIGNATION = re.compile(r"^(?=[^\s]*[-–])(?=[^\s]*\d)(?=[^\s]*[A-Za-zА-Яа-яЁё])[^\s]+")


def _is_designation(text: str) -> bool:
    return bool(_DESIGNATION.match(str(text).strip()))


@dataclass
class Arrowhead:
    """One drawn arrow, with the conductor it sits on and where it points."""

    bbox: tuple[float, float, float, float]
    apex: tuple[float, float]
    edge: int
    filled: bool


def find_arrowheads(data: PageData, facts: EdgeFacts) -> list[Arrowhead]:
    """Every arrow drawn on a proven conductor of this page."""
    edges, horizontal, low, high, level = _edge_arrays(data)
    conductor = facts.conductor
    found: list[Arrowhead] = []
    if not len(edges):
        return found

    def carrier(bbox: tuple[float, float, float, float]) -> int | None:
        cx = (bbox[0] + bbox[2]) / 2.0
        cy = (bbox[1] + bbox[3]) / 2.0
        along = np.where(horizontal, cx, cy)
        across = np.where(horizontal, cy, cx)
        hit = (
            conductor
            & (np.abs(across - level) <= max(bbox[2] - bbox[0], bbox[3] - bbox[1]) / 2.0 + ARROW_TOL)
            & (along >= low - ARROW_TOL) & (along <= high + ARROW_TOL)
        )
        candidates = np.nonzero(hit)[0]
        return int(candidates[0]) if len(candidates) == 1 else None

    for blob in data.strokes.blobs:
        width, height = blob.width, blob.height
        if max(width, height) > ARROW_MAX_SIZE or min(width, height) <= 0.5:
            continue
        if not blob.widens_monotonically():
            continue
        edge = carrier(blob.bbox)
        if edge is None:
            continue
        profile = np.asarray(blob.profile, dtype=np.float64)
        tip_at_low = profile[0] < profile[-1]
        apex = (
            ((blob.bbox[0] + blob.bbox[2]) / 2.0, blob.bbox[1] if tip_at_low else blob.bbox[3])
        )
        found.append(Arrowhead(bbox=blob.bbox, apex=apex, edge=edge, filled=True))

    for cluster in facts.clusters:
        if cluster.strokes != 3 or cluster.oversize:
            continue
        if max(cluster.bbox[2] - cluster.bbox[0], cluster.bbox[3] - cluster.bbox[1]) > ARROW_MAX_SIZE:
            continue
        if not cluster.slanted_members:
            continue
        rows = data.strokes.slanted[list(cluster.slanted_members)]
        points = np.vstack([rows[:, :2], rows[:, 2:]])
        if len(points) < 4:
            continue
        # A closed triangle: every endpoint is shared with another stroke.
        shared = sum(
            1 for point in points
            if int((np.hypot(points[:, 0] - point[0], points[:, 1] - point[1]) <= TOUCH_TOL).sum()) >= 2
        )
        if shared < len(points):
            continue
        edge = carrier(cluster.bbox)
        if edge is None:
            continue
        centre = points.mean(axis=0)
        apex_point = points[int(np.argmax(np.hypot(points[:, 0] - centre[0], points[:, 1] - centre[1])))]
        found.append(Arrowhead(
            bbox=cluster.bbox, apex=(float(apex_point[0]), float(apex_point[1])),
            edge=edge, filled=False,
        ))
    found.sort(key=lambda arrow: (round(arrow.bbox[1], 2), round(arrow.bbox[0], 2)))
    return found


def apply_directions(
    data: PageData, facts: EdgeFacts, topology: PageTopology, arrowheads: Sequence[Arrowhead]
) -> dict[str, int]:
    """Turn a connection into a feed where, and only where, an arrow says so."""
    counters = {"arrowheads": len(arrowheads), "edges_directed": 0}
    if not arrowheads:
        return counters
    by_run: dict[str, list[Arrowhead]] = defaultdict(list)
    for arrow in arrowheads:
        run = topology.run_of_edge.get(arrow.edge)
        if run is None:
            continue
        node_id = topology.node_of_run.get(run)
        if node_id:
            by_run[node_id].append(arrow)
    nodes = topology.node_by_id()
    for edge in topology.edges:
        if edge.edge_kind != ELECTRICAL_CONNECTION or edge.connection_claim != PROVEN_CONNECTION:
            continue
        arrows = by_run.get(edge.from_node_id) or by_run.get(edge.to_node_id)
        if not arrows or len(arrows) != 1:
            continue
        arrow = arrows[0]
        target = nodes.get(edge.to_node_id)
        source = nodes.get(edge.from_node_id)
        if target is None or source is None:
            continue
        towards_target = (
            abs(arrow.apex[0] - target.anchor[0]) + abs(arrow.apex[1] - target.anchor[1])
            < abs(arrow.apex[0] - source.anchor[0]) + abs(arrow.apex[1] - source.anchor[1])
        )
        if not towards_target:
            edge.from_node_id, edge.to_node_id = edge.to_node_id, edge.from_node_id
        edge.edge_kind = FEED
        edge.direction_status = PROVEN
        edge.direction_evidence = ARROWHEAD
        edge.geometry_refs = tuple(edge.geometry_refs) + (
            f"arrow:{round(arrow.apex[0], 2)},{round(arrow.apex[1], 2)}",
        )
        counters["edges_directed"] += 1
    return counters


def keyword_trap(topology: PageTopology) -> dict[str, Any]:
    """What a keyword rule would have done, and why it is not allowed to.

    The trap is read off the graph rather than off the text: group every label
    anchor by the conductor it hangs on — the ones bound by a drawn relation
    and the ones recorded by alignment alone — and look for a conductor
    carrying both a direction word and its own line number.  ``ВРУ1 ввод 1 –
    Корпус 3`` and ``ГРЩ1-РП1-3 …`` on one wire is the whole demonstration: one
    conductor, two names, opposite ends.  No meaning is read; both strings are
    attached to the same node by rules that ran before this function.
    """
    nodes = topology.node_by_id()
    by_target: dict[str, list[str]] = defaultdict(list)
    for edge in topology.edges:
        source = nodes.get(edge.from_node_id)
        if source is None or source.node_kind != "LABEL_ANCHOR":
            continue
        for text in source.labels:
            by_target[edge.to_node_id].append(str(text))
    directed = 0
    contradicted = 0
    examples: list[dict[str, Any]] = []
    for node_id, texts in sorted(by_target.items()):
        keyword = [
            text for text in texts
            if any(term in text.lower() for term in INCOMING_TERMS + OUTGOING_TERMS)
        ]
        if not keyword:
            continue
        directed += 1
        counter_name = [
            text for text in texts
            if _is_designation(text)
            and not any(term in text.lower() for term in INCOMING_TERMS + OUTGOING_TERMS)
        ]
        if counter_name:
            contradicted += 1
            if len(examples) < 8:
                examples.append({
                    "node_id": node_id,
                    "direction_word": sorted(keyword)[0],
                    "counter_name": sorted(counter_name)[0],
                })
    return {
        "nodes_a_keyword_rule_would_direct": directed,
        "of_those_carrying_a_counter_name_on_the_same_conductor": contradicted,
        "edges_this_layer_directs_from_a_keyword": 0,
        "examples": examples,
        "rule": "a direction word names the far end; the contract admits only an arrowhead",
    }


__all__ = [
    "ARROW_MAX_SIZE", "ARROW_TOL", "INCOMING_TERMS", "OUTGOING_TERMS",
    "Arrowhead", "apply_directions", "find_arrowheads", "keyword_trap",
]
