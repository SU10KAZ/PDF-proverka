"""Naming a node: which printed string belongs to which drawn thing.

V1 already owns the rule.  A label belongs to a structure when a stroke is
drawn *along* it — approaching within a fraction of the label's own font size
and running beside it for most of its length — or when exactly one closed box
contains it, or when it sits in exactly one cell of a drawn lattice.  Proximity
attributes nothing, being the only candidate attributes nothing, and being
inside something the size of the sheet attributes nothing.

What changes here is the *target*.  V1 attributed a label to a **region**, and a
region on a schematic is the whole sheet: bus, feeders, frames and symbols are
one connected component of touching strokes, and V1's own sheet-scale rule then
correctly refused to let it own anything.  That is why the control page of this
corpus — five hundred and seventy-nine printed strings on one single-line
diagram — produced almost no fragment-local evidence.

V2 attributes the same label, by the same rule, to a **run**: one wire.  The
feeder mark ``ГРЩ1-РП1-1 3хППГнг(А)-HF 5x150мм²`` is written along one vertical
conductor and along no other, so it names that conductor and not the sheet.

The three outcomes are kept apart on purpose.  ``BOUND`` names a node,
``AMBIGUOUS`` says the drawing offered more than one and the layer will not
choose, and ``UNBOUND`` says no drawn relation was found.  The second and third
are different facts and collapsing them would hide which of the two rules is
failing on a given sheet.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import numpy as np

from experiments.pdf_evidence_v1 import structure as v1_structure

from .conductors import EdgeFacts, _edge_arrays, _runs_along
from .contract import (
    AMBIGUOUS,
    BOUND,
    INSIDE_SINGLE_SYMBOL_BOX,
    INSIDE_SINGLE_TABLE_CELL,
    LABEL_ANCHOR,
    LABEL_CONNECTION,
    NAMED,
    NO_CLAIM,
    PROVEN_CONNECTION,
    RUNS_ALONG_SINGLE_CONDUCTOR,
    TopologyEdge,
    TopologyNode,
    UNBOUND,
    UNDIRECTED,
    UNKNOWN as UNKNOWN_KIND,
    UNNAMED as UNNAMED_OWNERSHIP,
)
from .page import PageData
from .topology import PageTopology

#: Slack when testing that a label sits inside a symbol's box, in points.
INSIDE_SLACK = 1.0


@dataclass
class BindingRecord:
    """One attempt to attach one printed string to the drawing."""

    label_id: str
    text: str
    bbox: tuple[float, float, float, float]
    status: str
    channel: str | None
    node_id: str | None
    candidates: int
    provenance: str
    #: Perpendicular distance from the label's box to the conductor that took
    #: it, in ems of the label's own size.  Kept so the inherited rule can be
    #: audited instead of trusted: a binding at 0.00 em is a string written on
    #: its wire, one at 0.25 em is a string the wire happened to pass under.
    offset_em: float | None = None
    target_kind: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "label_id": self.label_id,
            "text": self.text,
            "bbox": [round(float(value), 2) for value in self.bbox],
            "status": self.status,
            "channel": self.channel,
            "node_id": self.node_id,
            "candidates": self.candidates,
            "provenance": self.provenance,
            "offset_em": None if self.offset_em is None else round(float(self.offset_em), 3),
            "target_kind": self.target_kind,
        }


def bind_labels(
    data: PageData, facts: EdgeFacts, topology: PageTopology
) -> tuple[list[BindingRecord], dict[str, int]]:
    """Attach every printed string of the page to at most one node."""
    edges_array, horizontal, low, high, level = _edge_arrays(data)
    conductor = facts.conductor
    run_of_edge = topology.run_of_edge
    node_of_run = topology.node_of_run
    by_id = topology.node_by_id()

    symbol_nodes = [
        node for node in topology.nodes
        if node.symbol_signature is not None
    ]
    table_nodes = {node.region_id: node for node in topology.nodes if node.region_id}

    counters: dict[str, int] = defaultdict(int)
    records: list[BindingRecord] = []
    for label in data.labels:
        box = tuple(float(value) for value in label["bbox"])
        counters["labels"] += 1
        hit = _runs_along(box, bool(label["vertical"]), float(label["size"]),
                          low, high, level, horizontal)
        runs = sorted({run_of_edge[int(index)] for index in hit if conductor[int(index)]})
        if len(runs) == 1:
            chosen = [int(index) for index in hit
                      if conductor[int(index)] and run_of_edge[int(index)] == runs[0]]
            if bool(label["vertical"]):
                offset = min(min(abs(level[index] - box[0]), abs(level[index] - box[2]))
                             if not horizontal[index] else 0.0 for index in chosen)
            else:
                offset = min(min(abs(level[index] - box[1]), abs(level[index] - box[3]))
                             if horizontal[index] else 0.0 for index in chosen)
            inside = any(
                (box[0] <= level[index] <= box[2]) if not horizontal[index]
                else (box[1] <= level[index] <= box[3]) for index in chosen
            )
            target = by_id.get(node_of_run[runs[0]])
            records.append(BindingRecord(
                label_id=str(label["label_id"]), text=str(label["text"]), bbox=box,
                status=BOUND, channel=RUNS_ALONG_SINGLE_CONDUCTOR,
                node_id=node_of_run[runs[0]], candidates=1,
                provenance=str(label["provenance"]),
                offset_em=0.0 if inside else float(offset) / max(float(label["size"]), 1e-6),
                target_kind=target.node_kind if target else None,
            ))
            counters["bound_runs_along"] += 1
            counters["bound_on_the_line" if inside else "bound_beside_the_line"] += 1
            continue
        if len(runs) > 1:
            records.append(BindingRecord(
                label_id=str(label["label_id"]), text=str(label["text"]), bbox=box,
                status=AMBIGUOUS, channel=None, node_id=None, candidates=len(runs),
                provenance=str(label["provenance"]),
            ))
            counters["ambiguous_runs_along"] += 1
            continue

        inside = [
            node for node in symbol_nodes
            if node.bbox[0] - INSIDE_SLACK <= box[0] and box[2] <= node.bbox[2] + INSIDE_SLACK
            and node.bbox[1] - INSIDE_SLACK <= box[1] and box[3] <= node.bbox[3] + INSIDE_SLACK
        ]
        if len(inside) == 1:
            records.append(BindingRecord(
                label_id=str(label["label_id"]), text=str(label["text"]), bbox=box,
                status=BOUND, channel=INSIDE_SINGLE_SYMBOL_BOX,
                node_id=inside[0].node_id, candidates=1,
                provenance=str(label["provenance"]),
            ))
            counters["bound_inside_symbol"] += 1
            continue
        if len(inside) > 1:
            innermost = min(inside, key=lambda node: (node.bbox[2] - node.bbox[0]) * (node.bbox[3] - node.bbox[1]))
            nested = all(
                node.bbox[0] <= innermost.bbox[0] + INSIDE_SLACK
                and node.bbox[2] >= innermost.bbox[2] - INSIDE_SLACK
                and node.bbox[1] <= innermost.bbox[1] + INSIDE_SLACK
                and node.bbox[3] >= innermost.bbox[3] - INSIDE_SLACK
                for node in inside if node is not innermost
            )
            if nested:
                records.append(BindingRecord(
                    label_id=str(label["label_id"]), text=str(label["text"]), bbox=box,
                    status=BOUND, channel=INSIDE_SINGLE_SYMBOL_BOX,
                    node_id=innermost.node_id, candidates=len(inside),
                    provenance=str(label["provenance"]),
                ))
                counters["bound_inside_symbol"] += 1
                continue
            records.append(BindingRecord(
                label_id=str(label["label_id"]), text=str(label["text"]), bbox=box,
                status=AMBIGUOUS, channel=None, node_id=None, candidates=len(inside),
                provenance=str(label["provenance"]),
            ))
            counters["ambiguous_inside_symbol"] += 1
            continue

        cell_owner = None
        for region_id, node in table_nodes.items():
            region = next((r for r in data.regions if r.region_id == region_id), None)
            if region is None:
                continue
            if not (
                region.bbox[0] - INSIDE_SLACK <= box[0] and box[2] <= region.bbox[2] + INSIDE_SLACK
                and region.bbox[1] - INSIDE_SLACK <= box[1] and box[3] <= region.bbox[3] + INSIDE_SLACK
            ):
                continue
            if v1_structure._cell_of(region, box) is not None:
                cell_owner = node if cell_owner is None else False
        if cell_owner not in (None, False):
            records.append(BindingRecord(
                label_id=str(label["label_id"]), text=str(label["text"]), bbox=box,
                status=BOUND, channel=INSIDE_SINGLE_TABLE_CELL,
                node_id=cell_owner.node_id, candidates=1,
                provenance=str(label["provenance"]),
            ))
            counters["bound_table_cell"] += 1
            continue
        if cell_owner is False:
            counters["ambiguous_table_cell"] += 1
            records.append(BindingRecord(
                label_id=str(label["label_id"]), text=str(label["text"]), bbox=box,
                status=AMBIGUOUS, channel=None, node_id=None, candidates=2,
                provenance=str(label["provenance"]),
            ))
            continue
        records.append(BindingRecord(
            label_id=str(label["label_id"]), text=str(label["text"]), bbox=box,
            status=UNBOUND, channel=None, node_id=None, candidates=0,
            provenance=str(label["provenance"]),
        ))
        counters["unbound"] += 1
    return records, dict(counters)


def apply_bindings(
    data: PageData, topology: PageTopology, records: Sequence[BindingRecord]
) -> int:
    """Add a label anchor node and a binding edge for every bound string."""
    by_id = topology.node_by_id()
    added = 0
    labels_of_node: dict[str, list[str]] = defaultdict(list)
    for position, record in enumerate(records):
        if record.status != BOUND or record.node_id is None:
            continue
        node_id = f"n:{data.page:04d}:l{position:05d}"
        topology.nodes.append(TopologyNode(
            node_id=node_id,
            document=data.document,
            physical_page=data.page,
            node_kind=LABEL_ANCHOR,
            bbox=record.bbox,
            anchor=((record.bbox[0] + record.bbox[2]) / 2.0, (record.bbox[1] + record.bbox[3]) / 2.0),
            ownership_status=NAMED,
            labels=(record.text,),
            evidence_refs=(f"label:{record.label_id}", f"provenance:{record.provenance}"),
        ))
        topology.edges.append(TopologyEdge(
            edge_id=f"e:{data.page:04d}:n{position:05d}",
            document=data.document,
            physical_page=data.page,
            from_node_id=node_id,
            to_node_id=record.node_id,
            edge_kind=LABEL_CONNECTION,
            connection_claim=PROVEN_CONNECTION,
            direction_status=UNDIRECTED,
            binding_channel=record.channel,
            geometry_refs=(f"label:{record.label_id}",),
        ))
        labels_of_node[record.node_id].append(record.text)
        added += 1
    for node_id, texts in labels_of_node.items():
        node = by_id.get(node_id)
        if node is None:
            continue
        node.labels = tuple(sorted(texts))
        node.ownership_status = NAMED
    topology.counters["nodes_LABEL_ANCHOR"] = added
    topology.counters["labels_bound"] = added
    return added




# ---------------------------------------------------------------------------
# what alignment offers, and what it is not allowed to claim
# ---------------------------------------------------------------------------

#: Two labels are co-extensive when they overlap this much along their long
#: axis, measured against the shorter of the two.
COLUMN_OVERLAP = 0.8
#: …and sit within this many ems of each other across it.
COLUMN_GAP_EM = 1.0
#: Hops allowed away from a bound label.  Three columns of a feeder legend need
#: two; more than three is a paragraph, not a column.
COLUMN_MAX_DEPTH = 3


def column_adjacency(
    data: PageData, topology: PageTopology, records: Sequence[BindingRecord]
) -> tuple[list[TopologyEdge], dict[str, int]]:
    """Labels that a *typographic* rule would attach, recorded and not claimed.

    On the control page the feeder legend is three columns of vertical text.
    The middle column is drawn beside the conductor and binds; the outer one —
    ``ВРУ1 ввод 1 – Корпус 1,2``, the string that carries the building and the
    corpus — is sixteen points away with nothing drawn between.  Nothing on the
    sheet connects it to that feeder except the fact that it lines up.

    Alignment is not a drawn relation and this layer may not turn it into one.
    It is recorded anyway, as ``UNKNOWN`` edges carrying ``NO_CLAIM``, for two
    reasons: it measures exactly how much a consumer with an *independent*
    check could still gain, and it is where the direction-word trap lives.
    """
    bound = {record.label_id: record for record in records if record.status == BOUND and record.node_id}
    if not bound:
        return [], {"column_candidates": 0, "column_edges": 0}
    labels = {str(row["label_id"]): row for row in data.labels}
    order = sorted(labels)
    neighbours: dict[str, list[str]] = defaultdict(list)
    rows = [labels[label_id] for label_id in order]
    for position, left in enumerate(rows):
        lb = left["bbox"]
        for right in rows[position + 1:]:
            rb = right["bbox"]
            if bool(left["vertical"]) != bool(right["vertical"]):
                continue
            size = max(float(left["size"]), float(right["size"]), 1e-6)
            if left["vertical"]:
                overlap = min(lb[3], rb[3]) - max(lb[1], rb[1])
                extent = min(lb[3] - lb[1], rb[3] - rb[1])
                gap = max(rb[0] - lb[2], lb[0] - rb[2])
            else:
                overlap = min(lb[2], rb[2]) - max(lb[0], rb[0])
                extent = min(lb[2] - lb[0], rb[2] - rb[0])
                gap = max(rb[1] - lb[3], lb[1] - rb[3])
            if extent <= 0 or overlap < COLUMN_OVERLAP * extent:
                continue
            if gap > COLUMN_GAP_EM * size:
                continue
            neighbours[str(left["label_id"])].append(str(right["label_id"]))
            neighbours[str(right["label_id"])].append(str(left["label_id"]))

    reached: dict[str, tuple[str, int]] = {}
    frontier = [(label_id, record.node_id, 0) for label_id, record in sorted(bound.items())]
    while frontier:
        label_id, node_id, depth = frontier.pop(0)
        if depth >= COLUMN_MAX_DEPTH:
            continue
        for neighbour in sorted(neighbours.get(label_id, ())):
            if neighbour in bound or neighbour in reached:
                continue
            reached[neighbour] = (str(node_id), depth + 1)
            frontier.append((neighbour, str(node_id), depth + 1))

    edges: list[TopologyEdge] = []
    for position, (label_id, (node_id, depth)) in enumerate(sorted(reached.items())):
        row = labels[label_id]
        anchor_id = f"n:{data.page:04d}:x{position:05d}"
        topology.nodes.append(TopologyNode(
            node_id=anchor_id,
            document=data.document,
            physical_page=data.page,
            node_kind=LABEL_ANCHOR,
            bbox=tuple(float(value) for value in row["bbox"]),
            anchor=(
                (float(row["bbox"][0]) + float(row["bbox"][2])) / 2.0,
                (float(row["bbox"][1]) + float(row["bbox"][3])) / 2.0,
            ),
            ownership_status=UNNAMED_OWNERSHIP,
            labels=(str(row["text"]),),
            evidence_refs=(f"label:{label_id}",),
            notes=("aligned_only",),
        ))
        edges.append(TopologyEdge(
            edge_id=f"e:{data.page:04d}:x{position:05d}",
            document=data.document,
            physical_page=data.page,
            from_node_id=anchor_id,
            to_node_id=node_id,
            edge_kind=UNKNOWN_KIND,
            connection_claim=NO_CLAIM,
            direction_status=UNKNOWN_KIND,
            geometry_refs=(),
            notes=(f"co_extensive_label_column depth={depth}", "alignment is not a drawn relation"),
        ))
    topology.edges.extend(edges)
    return edges, {
        "column_candidates": len(reached),
        "column_edges": len(edges),
    }


__all__ = [
    "COLUMN_GAP_EM", "COLUMN_MAX_DEPTH", "COLUMN_OVERLAP", "INSIDE_SLACK",
    "BindingRecord", "apply_bindings", "bind_labels", "column_adjacency",
]
