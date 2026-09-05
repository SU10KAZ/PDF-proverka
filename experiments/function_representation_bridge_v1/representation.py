"""What one page carries, and which drawn containers it offers.

The inventory first, because the whole track turns on it: §13 asks which
representation each side of a lineage task speaks, and that cannot be answered
by a guess about what a sheet "is".  A page is read for four independent
signals — printed strings, drawn strokes, ruled grids and a proven electrical
graph — and its representation is whatever it actually carries.

Then the containers.  Three kinds pass, and the reason each passes is a drawn
fact rather than a size or a distance:

``PROVEN_CONNECTED_COMPONENT``
    V2's island, taken from ``function_topology_v1`` unchanged.

``DRAWN_TABLE_LATTICE``
    A ruled grid whose **first row prints a caption in a contiguous run of
    columns starting at the first one**.  This is the rule that separates a
    table from a riser diagram drawn on a grid, and it was forced by the corpus:
    on the ГРЩ sheet nine little lattices each print one caption — ``ВРУ1``,
    ``ВРУ3``, ``ШУ-ХЦ`` — above ``Рр,кВт`` and ``Iр,А``, while the left
    document's riser sheets carry one grid of long rulings covering two fifths
    of the page whose first row prints nothing at all.  A table declares its own
    columns; a drawing on a grid does not.

``DRAWN_STROKE_GROUP``
    A connected component of strokes that carries printed text through V1's own
    drawn relations — a leader running *along* the label, or a closed box around
    it.  It is bounded by its own connectivity and says nothing about whether
    its strokes conduct.

One absorption rule keeps the three from claiming the same ink.  1 221 of the
1 482 strings V2 bound to a conductor are *also* owned by a stroke group under
V1's leader rule — the same drawn leader, read by two layers.  Where that
happens the schematic assembly wins, the stroke group hands the string over, and
a stroke group left holding nothing is not emitted.  Without this rule the same
printed string would belong to two assemblies, which is exactly what negative
control E forbids.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from experiments.function_topology_v1 import aggregation as topology_aggregation
from experiments.function_topology_v1.contract import FunctionTopologySubgraph

from .contract import (
    DRAWN_STROKE_GROUP,
    DRAWN_TABLE_LATTICE,
    MIXED,
    PROVEN_CONNECTED_COMPONENT,
    SCHEMATIC,
    TABLE,
    TEXT,
)

#: V1's drawn ownerships.  Nothing else attributes a printed string to anything.
DRAWN_OWNERSHIPS = ("TABLE_CELL", "DIRECT_CONTAINMENT", "CONNECTED_CALLOUT")
#: Region kinds that can become a stroke-group container.  ``SHEET_FRAME`` is
#: absent on purpose: the sheet border is not a thing on the sheet.
STROKE_GROUP_KINDS = ("EDGE_GROUP", "BOX")
#: Region kinds that can become a table container.
LATTICE_KINDS = ("TABLE", "STAMP")


@dataclass
class Container:
    """One drawn container of a page, before it becomes an assembly."""

    container_id: str
    channel: str
    region_id: str | None
    region_kind: str | None
    subgraph: FunctionTopologySubgraph | None = None
    label_ids: tuple[str, ...] = ()
    cells: dict[tuple[int, int], tuple[str, ...]] = field(default_factory=dict)
    column_captions: tuple[str, ...] = ()
    rows: int = 0
    columns: int = 0
    area_share: float = 0.0
    notes: tuple[str, ...] = ()

    @property
    def representation_type(self) -> str:
        if self.channel == PROVEN_CONNECTED_COMPONENT:
            return SCHEMATIC
        if self.channel == DRAWN_TABLE_LATTICE:
            return TABLE
        return TEXT


@dataclass
class PageRepresentation:
    """Everything this layer reads off one physical page."""

    document: str
    pair_id: str
    side: str
    physical_page: int
    printed_strings: int
    stroke_count: int
    conductor_count: int
    lattice_count: int
    aggregate_count: int
    proven_aggregate_count: int
    representation_types: tuple[str, ...]
    containers: list[Container] = field(default_factory=list)
    labels_by_id: dict[str, dict[str, Any]] = field(default_factory=dict)
    aggregation: topology_aggregation.PageAggregation | None = None
    sheet_marks: tuple[str, ...] = ()
    counters: dict[str, int] = field(default_factory=dict)
    _folded: list[tuple[str, str]] | None = None

    def folded_labels(self) -> list[tuple[str, str]]:
        """``(normalized text, label id)`` for every printed string, folded once.

        Placing a passport's documented values asks the same page the same
        question dozens of times, and the sensitivity curve asks it seven times
        over; folding the page once turns minutes into seconds and changes no
        answer.
        """
        from experiments.pdf_evidence_v1.textnorm import normalize

        if self._folded is None:
            self._folded = [
                (normalize(row["text"]), label_id)
                for label_id, row in self.labels_by_id.items()
            ]
        return self._folded

    def inventory(self) -> dict[str, Any]:
        return {
            "document": self.document,
            "physical_page": self.physical_page,
            "printed_strings": self.printed_strings,
            "drawn_strokes": self.stroke_count,
            "proven_conductors": self.conductor_count,
            "ruled_lattices": self.lattice_count,
            "drawn_aggregates": self.aggregate_count,
            "aggregates_with_a_proven_extent": self.proven_aggregate_count,
            "representation_types": list(self.representation_types),
            "containers": len(self.containers),
            "containers_by_channel": {
                channel: sum(1 for item in self.containers if item.channel == channel)
                for channel in sorted({item.channel for item in self.containers})
            },
        }


def _header_row_columns(cells: Mapping[tuple[int, int], Sequence[str]]) -> list[int] | None:
    """The columns of a printed header row, or ``None`` when there is none.

    A table declares its columns in its first row: the row indexed zero, filled
    from the first column onwards without a gap.  Both halves matter.  Dropping
    "row zero" would let any dense band of a drawing pass; dropping "contiguous
    from column zero" would let a riser sheet whose top band prints values in
    columns 0, 3, 6, 9 and 12 pass as a five-column table.
    """
    columns = sorted({column for row, column in cells if row == 0})
    if not columns:
        return None
    if columns != list(range(len(columns))):
        return None
    return columns


def _cells_of_page(labels: Sequence[Mapping[str, Any]]) -> dict[str, dict[tuple[int, int], list[str]]]:
    out: dict[str, dict[tuple[int, int], list[str]]] = defaultdict(dict)
    for label in labels:
        if label["ownership"] != "TABLE_CELL" or not label["region_id"]:
            continue
        key = (int(label["cell"][0]), int(label["cell"][1]))
        out[str(label["region_id"])].setdefault(key, []).append(str(label["text"]))
    return out


def _labels_with_ownership(result: Any) -> list[dict[str, Any]]:
    """Every printed string of the page with V1's structural attribution.

    ``PageResult`` already carries V1's answer for exactly these strings, so the
    two layers cannot drift apart about which lattice owns what.
    """
    ownership = result.v1_ownership
    rows: list[dict[str, Any]] = []
    for label in result.data.labels:
        attribution = ownership.get(str(label["label_id"])) or {}
        rows.append({
            "label_id": str(label["label_id"]),
            "text": str(label["text"]),
            "bbox": [float(value) for value in label["bbox"]],
            "provenance": str(label["provenance"]),
            "ownership": str(attribution.get("ownership") or "NO_OWNERSHIP"),
            "region_id": attribution.get("region_id"),
            "cell": attribution.get("cell"),
        })
    return rows


def read_page(pair_id: str, side: str, result: Any) -> PageRepresentation:
    """One page: its inventory, its containers and the strings inside them."""
    data = result.data
    labels = _labels_with_ownership(result)
    labels_by_id = {row["label_id"]: row for row in labels}
    aggregation = topology_aggregation.aggregate_page(result)
    cells_by_region = _cells_of_page(labels)
    regions = {region.region_id: region for region in data.regions}
    page_area = max(float(data.width) * float(data.height), 1e-6)

    # strings the drawing itself proved to run along a conductor
    claimed: dict[str, str] = {}
    for subgraph in aggregation.subgraphs:
        for label_id in subgraph.label_evidence_ids:
            claimed[str(label_id)] = subgraph.subgraph_id

    containers: list[Container] = []
    for subgraph in aggregation.subgraphs:
        containers.append(Container(
            container_id=subgraph.subgraph_id,
            channel=PROVEN_CONNECTED_COMPONENT,
            region_id=None,
            region_kind=None,
            subgraph=subgraph,
            label_ids=tuple(sorted(
                label_id for label_id in subgraph.label_evidence_ids
                if label_id in labels_by_id
            )),
            notes=(f"island_members={len(subgraph.member_node_ids)}",),
        ))

    owned: dict[str, list[str]] = defaultdict(list)
    for row in labels:
        if row["ownership"] in DRAWN_OWNERSHIPS and row["region_id"]:
            owned[str(row["region_id"])].append(row["label_id"])

    for region_id in sorted(owned):
        region = regions.get(region_id)
        if region is None:
            continue
        free = tuple(sorted(
            label_id for label_id in owned[region_id] if label_id not in claimed
        ))
        area_share = (
            (region.bbox[2] - region.bbox[0]) * (region.bbox[3] - region.bbox[1]) / page_area
        )
        if region.kind in LATTICE_KINDS:
            cells = cells_by_region.get(region_id, {})
            header = _header_row_columns(cells)
            if header is None:
                continue
            captions = tuple(
                " ".join(cells[(0, column)]) for column in header if (0, column) in cells
            )
            containers.append(Container(
                container_id=region_id,
                channel=DRAWN_TABLE_LATTICE,
                region_id=region_id,
                region_kind=region.kind,
                label_ids=tuple(sorted(owned[region_id])),
                cells={key: tuple(value) for key, value in sorted(cells.items())},
                column_captions=captions,
                rows=len(region.rows),
                columns=len(region.columns),
                area_share=round(area_share, 4),
                notes=(f"header_columns={len(header)}",),
            ))
            continue
        if region.kind in STROKE_GROUP_KINDS and free:
            containers.append(Container(
                container_id=region_id,
                channel=DRAWN_STROKE_GROUP,
                region_id=region_id,
                region_kind=region.kind,
                label_ids=free,
                area_share=round(area_share, 4),
                notes=(
                    f"strings_handed_to_the_schematic={len(owned[region_id]) - len(free)}",
                ),
            ))

    lattices = sum(1 for region in data.regions if region.kind in LATTICE_KINDS)
    kinds: list[str] = []
    if aggregation.subgraphs:
        kinds.append(SCHEMATIC)
    if any(item.channel == DRAWN_TABLE_LATTICE for item in containers):
        kinds.append(TABLE)
    if labels:
        kinds.append(TEXT)
    representation_types = tuple(kinds) if len(kinds) < 2 else (MIXED, *kinds)

    return PageRepresentation(
        document=str(data.document),
        pair_id=pair_id,
        side=side,
        physical_page=int(result.page),
        printed_strings=len(labels),
        stroke_count=int(len(data.strokes.edges)),
        conductor_count=int(result.facts.conductor.sum()),
        lattice_count=lattices,
        aggregate_count=len(aggregation.subgraphs),
        proven_aggregate_count=sum(
            1 for item in aggregation.subgraphs if item.boundary_status == "PROVEN"),
        representation_types=representation_types,
        containers=containers,
        labels_by_id=labels_by_id,
        aggregation=aggregation,
        sheet_marks=tuple(sorted(aggregation.sheet_marks)),
        counters={
            "strings_with_a_drawn_owner": sum(
                1 for row in labels if row["ownership"] in DRAWN_OWNERSHIPS),
            "strings_bound_to_a_conductor": len(claimed),
            "strings_bound_to_a_conductor_and_owned_by_a_stroke_group": sum(
                1 for row in labels
                if row["label_id"] in claimed and row["ownership"] in DRAWN_OWNERSHIPS
            ),
            "lattices_refused_for_lacking_a_header_row": sum(
                1 for region_id, cells in cells_by_region.items()
                if regions.get(region_id) is not None
                and regions[region_id].kind in LATTICE_KINDS
                and _header_row_columns(cells) is None
            ),
        },
    )


def document_inventory(pages: Sequence[PageRepresentation]) -> dict[str, Any]:
    """Per document, what its pages actually hold — §13's raw material."""
    if not pages:
        return {}
    counters: dict[str, int] = defaultdict(int)
    for page in pages:
        counters["pages"] += 1
        counters["pages_with_printed_text"] += int(bool(page.printed_strings))
        counters["pages_with_a_vector_layer"] += int(bool(page.stroke_count))
        counters["pages_with_a_ruled_lattice"] += int(bool(page.lattice_count))
        counters["pages_with_a_drawn_graph"] += int(bool(page.aggregate_count))
        counters["pages_with_a_table_container"] += int(any(
            item.channel == DRAWN_TABLE_LATTICE for item in page.containers))
        counters["pages_with_a_stroke_group_container"] += int(any(
            item.channel == DRAWN_STROKE_GROUP for item in page.containers))
        counters["pages_with_no_container_at_all"] += int(not page.containers)
        counters["containers"] += len(page.containers)
        counters["printed_strings"] += page.printed_strings
        for key, value in page.counters.items():
            counters[key] += value
    return {
        "document": pages[0].document,
        "pair_id": pages[0].pair_id,
        "side": pages[0].side,
        **{key: int(value) for key, value in sorted(counters.items())},
    }


__all__ = [
    "DRAWN_OWNERSHIPS",
    "LATTICE_KINDS",
    "STROKE_GROUP_KINDS",
    "Container",
    "PageRepresentation",
    "document_inventory",
    "read_page",
]
