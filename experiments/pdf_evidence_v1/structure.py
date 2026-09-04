"""Structural regions and the only three ways a fact may belong to one.

Membership is a drawn relation or it does not exist:

* ``TABLE_CELL`` — the text sits in exactly one cell of a drawn lattice;
* ``DIRECT_CONTAINMENT`` — exactly one closed box contains it;
* ``CONNECTED_CALLOUT`` — a stroke is drawn *along* the label: it approaches
  within a fraction of the label's own font size **and** runs beside it for
  most of the label's length.  The second condition is the whole rule; without
  it "a leader" degenerates into "the nearest stroke".

Nothing else attributes anything.  In particular: being the closest region,
being the only region, and being inside a region the size of the sheet all
attribute nothing.  The negative controls in the audit measure exactly these
three temptations.

Rules carried forward unchanged from the v2.9 / v3.0 tracks, because they were
paid for: a region covering most of the page restates ``sheet == fragment``
under a new name; a lone region is not evidence; proximity is never proof.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import numpy as np

from . import geometry as geometry_module
from .contract import (
    AMBIGUOUS_OWNERSHIP,
    CONNECTED_CALLOUT,
    DIRECT_CONTAINMENT,
    NO_OWNERSHIP,
    STAMP_ZONE,
    TABLE_CELL,
)
from .extraction import PageSource

REGION_KINDS = ("SHEET_FRAME", "STAMP", "TABLE", "BOX", "EDGE_GROUP", "TEXT_SECTION")

#: A region covering at least this share of the page never confers ownership.
SHEET_SCALE_AREA = 0.55
#: A lattice needs at least this many rulings in each direction to be a table.
MIN_TABLE_ROWS = 3
MIN_TABLE_COLUMNS = 2
#: A ruling must span this share of the lattice bbox to count as a row/column.
RULING_SPAN = 0.55
#: Slack, in points, when testing whether text sits inside a boundary.
CONTAINMENT_SLACK = 1.0
#: Leader gap, as a share of the label's font size.  Reported with a
#: sensitivity curve, never presented as a tuned single truth.
LEADER_GAP_EM = 0.3
#: Share of the label's length the stroke must run along.
LEADER_OVERLAP = 0.8


@dataclass
class Region:
    region_id: str
    kind: str
    bbox: list[float]
    edge_count: int
    horizontal: int
    vertical: int
    rows: list[float] = field(default_factory=list)
    columns: list[float] = field(default_factory=list)
    edges: np.ndarray = field(default_factory=lambda: np.zeros((0, 4)))

    @property
    def area(self) -> float:
        return max(0.0, self.bbox[2] - self.bbox[0]) * max(0.0, self.bbox[3] - self.bbox[1])

    def cells(self) -> list[dict[str, Any]]:
        """Cell rectangles of a lattice, in reading order.

        The rectangle is what makes a table cell usable as evidence: a row and
        column index says which cell, the rectangle says where to look.
        """
        out: list[dict[str, Any]] = []
        for row in range(len(self.rows) - 1):
            for column in range(len(self.columns) - 1):
                out.append({
                    "region_id": self.region_id,
                    "row": row,
                    "column": column,
                    "bbox": [
                        round(float(self.columns[column]), 2),
                        round(float(self.rows[row]), 2),
                        round(float(self.columns[column + 1]), 2),
                        round(float(self.rows[row + 1]), 2),
                    ],
                })
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "region_id": self.region_id,
            "region_kind": self.kind,
            "bbox": [round(float(value), 2) for value in self.bbox],
            "edges": self.edge_count,
            "horizontal_edges": self.horizontal,
            "vertical_edges": self.vertical,
            "rows": len(self.rows),
            "columns": len(self.columns),
        }


def _bbox(edges: np.ndarray) -> list[float]:
    return [
        float(min(edges[:, 0].min(), edges[:, 2].min())),
        float(min(edges[:, 1].min(), edges[:, 3].min())),
        float(max(edges[:, 0].max(), edges[:, 2].max())),
        float(max(edges[:, 1].max(), edges[:, 3].max())),
    ]


def _contains(outer: Sequence[float], inner: Sequence[float], slack: float = CONTAINMENT_SLACK) -> bool:
    return (
        inner[0] >= outer[0] - slack
        and inner[1] >= outer[1] - slack
        and inner[2] <= outer[2] + slack
        and inner[3] <= outer[3] + slack
    )


def _rulings(edges: np.ndarray, axis: int, bbox: Sequence[float]) -> list[float]:
    if len(edges) == 0:
        return []
    if axis == 0:
        span = edges[:, 2] - edges[:, 0]
        extent = max(bbox[2] - bbox[0], 1e-6)
        level = edges[:, 1]
    else:
        span = edges[:, 3] - edges[:, 1]
        extent = max(bbox[3] - bbox[1], 1e-6)
        level = edges[:, 0]
    keep = span >= RULING_SPAN * extent
    return sorted({round(float(value), 1) for value in level[keep]})


def build_regions(source: PageSource) -> list[Region]:
    """Regions of one page, ordered deterministically by position and size."""
    horizontal = source.geometry.horizontal
    vertical = source.geometry.vertical
    if len(horizontal) == 0 and len(vertical) == 0:
        return _text_only_regions(source)
    labels, count = geometry_module.incidence_components(horizontal, vertical)
    if len(horizontal) and len(vertical):
        stacked = np.vstack([horizontal, vertical])
    else:
        stacked = horizontal if len(horizontal) else vertical
    page_area = max(source.width * source.height, 1e-6)
    regions: list[Region] = []
    for component in range(count):
        member = stacked[labels == component]
        if len(member) == 0:
            continue
        bbox = _bbox(member)
        n_horizontal = int((member[:, 1] == member[:, 3]).sum())
        n_vertical = len(member) - n_horizontal
        area_share = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) / page_area
        rows = _rulings(member[member[:, 1] == member[:, 3]], 0, bbox)
        columns = _rulings(member[member[:, 0] == member[:, 2]], 1, bbox)
        if area_share >= SHEET_SCALE_AREA and len(member) <= 8:
            kind = "SHEET_FRAME"
        elif area_share >= SHEET_SCALE_AREA:
            kind = "EDGE_GROUP"
        elif source.in_stamp_zone(bbox) and len(rows) >= MIN_TABLE_ROWS:
            kind = "STAMP"
        elif len(rows) >= MIN_TABLE_ROWS and len(columns) >= MIN_TABLE_COLUMNS:
            kind = "TABLE"
        elif len(member) == 4 and n_horizontal == 2 and n_vertical == 2:
            kind = "BOX"
        else:
            kind = "EDGE_GROUP"
        regions.append(Region(
            region_id=f"reg_{source.page:04d}_{component:05d}",
            kind=kind,
            bbox=bbox,
            edge_count=int(len(member)),
            horizontal=n_horizontal,
            vertical=n_vertical,
            rows=rows if kind in {"TABLE", "STAMP"} else [],
            columns=columns if kind in {"TABLE", "STAMP"} else [],
            edges=member,
        ))
    regions.sort(key=lambda region: (region.bbox[1], region.bbox[0], -region.area, region.region_id))
    return regions


def _text_only_regions(source: PageSource) -> list[Region]:
    """A page with no drawn boundary still has native paragraph rectangles."""
    return [
        Region(
            region_id=f"reg_{source.page:04d}_t{index:04d}",
            kind="TEXT_SECTION",
            bbox=list(block["bbox"]),
            edge_count=0,
            horizontal=0,
            vertical=0,
        )
        for index, block in enumerate(source.paragraphs)
    ]


def _effective_size(item: Mapping[str, Any]) -> float:
    """Font size of a label, or its own height when the channel has no size.

    An annotation carries a rectangle and no font.  Using its short side keeps
    the leader rule scale-relative for annotations too, instead of falling back
    to a fixed number of points that means different things on an A1 and an A4.
    """
    size = float(item.get("size") or 0.0)
    if size > 0:
        return size
    bbox = item["bbox"]
    return max(min(float(bbox[3]) - float(bbox[1]), float(bbox[2]) - float(bbox[0])), 1e-6)


def _attached_edges(
    item: Mapping[str, Any],
    edges: np.ndarray,
    *,
    gap_em: float = LEADER_GAP_EM,
    overlap: float = LEADER_OVERLAP,
) -> np.ndarray:
    """Indices of edges drawn *along* this label."""
    if len(edges) == 0:
        return np.zeros(0, dtype=int)
    x0, y0, x1, y1 = item["bbox"]
    gap = gap_em * _effective_size(item)
    ex0 = np.minimum(edges[:, 0], edges[:, 2])
    ex1 = np.maximum(edges[:, 0], edges[:, 2])
    ey0 = np.minimum(edges[:, 1], edges[:, 3])
    ey1 = np.maximum(edges[:, 1], edges[:, 3])
    near = (ex0 <= x1 + gap) & (ex1 >= x0 - gap) & (ey0 <= y1 + gap) & (ey1 >= y0 - gap)
    if not np.any(near):
        return np.zeros(0, dtype=int)
    if item.get("vertical"):
        length = max(y1 - y0, 1e-6)
        run = np.minimum(ey1, y1) - np.maximum(ey0, y0)
        parallel = (ex1 - ex0) <= (ey1 - ey0)
    else:
        length = max(x1 - x0, 1e-6)
        run = np.minimum(ex1, x1) - np.maximum(ex0, x0)
        parallel = (ey1 - ey0) <= (ex1 - ex0)
    return np.nonzero(near & parallel & (run >= overlap * length))[0]


def _cell_of(region: Region, bbox: Sequence[float]) -> tuple[int, int] | None:
    if not region.rows or not region.columns:
        return None
    row = None
    for index in range(len(region.rows) - 1):
        if (region.rows[index] - CONTAINMENT_SLACK <= bbox[1]
                and bbox[3] <= region.rows[index + 1] + CONTAINMENT_SLACK):
            row = index
            break
    column = None
    for index in range(len(region.columns) - 1):
        if (region.columns[index] - CONTAINMENT_SLACK <= bbox[0]
                and bbox[2] <= region.columns[index + 1] + CONTAINMENT_SLACK):
            column = index
            break
    if row is None or column is None:
        return None
    return row, column


@dataclass
class RegionIndex:
    """Flattened regions of one page, ready for vectorized attribution."""

    regions: list[Region]
    local: list[int]
    edges: np.ndarray
    edge_region: np.ndarray
    boxes: list[int]
    tables: list[int]


def build_index(source: PageSource, regions: Sequence[Region]) -> RegionIndex:
    """Precompute the arrays attribution needs.

    The sheet-scale rule lives here: a region covering most of the page is
    dropped from the local set, so it can never confer ownership on anything.
    """
    page_area = max(source.width * source.height, 1e-6)
    local = [
        index for index, region in enumerate(regions)
        if region.kind != "SHEET_FRAME" and region.area / page_area < SHEET_SCALE_AREA
    ]
    chunks = [regions[index].edges for index in local if len(regions[index].edges)]
    owners = [
        np.full(len(regions[index].edges), index, dtype=np.int64)
        for index in local if len(regions[index].edges)
    ]
    return RegionIndex(
        regions=list(regions),
        local=local,
        edges=np.vstack(chunks) if chunks else np.zeros((0, 4)),
        edge_region=np.concatenate(owners) if owners else np.zeros(0, dtype=np.int64),
        boxes=[index for index in local if regions[index].kind == "BOX"],
        tables=[index for index in local if regions[index].kind in {"TABLE", "STAMP"}],
    )


def _result(
    ownership: str,
    applicability: str,
    region: Region | None = None,
    cell: tuple[int, int] | None = None,
) -> dict[str, Any]:
    cell_bbox = None
    if region is not None and cell is not None:
        cell_bbox = [
            round(float(region.columns[cell[1]]), 2),
            round(float(region.rows[cell[0]]), 2),
            round(float(region.columns[cell[1] + 1]), 2),
            round(float(region.rows[cell[0] + 1]), 2),
        ]
    return {
        "ownership": ownership,
        "applicability": applicability,
        "region_id": region.region_id if region else None,
        "region_kind": region.kind if region else None,
        "cell": list(cell) if cell else None,
        "cell_bbox": cell_bbox,
    }


def attribute(
    source: PageSource,
    index: RegionIndex,
    item: Mapping[str, Any],
    *,
    gap_em: float = LEADER_GAP_EM,
    overlap: float = LEADER_OVERLAP,
) -> dict[str, Any]:
    """Attribute one rectangle on the page to at most one region."""
    bbox = item["bbox"]
    if source.in_stamp_zone(bbox):
        return _result(STAMP_ZONE, "SHEET_SHARED")

    cells: list[tuple[Region, tuple[int, int]]] = []
    for position in index.tables:
        region = index.regions[position]
        if not _contains(region.bbox, bbox):
            continue
        cell = _cell_of(region, bbox)
        if cell is not None:
            cells.append((region, cell))
    if len(cells) == 1:
        region, cell = cells[0]
        return _result(TABLE_CELL, "FRAGMENT_LOCAL", region, cell)
    if len(cells) > 1:
        return _result(AMBIGUOUS_OWNERSHIP, "UNKNOWN")

    boxes = [
        index.regions[position] for position in index.boxes
        if _contains(index.regions[position].bbox, bbox)
    ]
    if len(boxes) == 1:
        return _result(DIRECT_CONTAINMENT, "FRAGMENT_LOCAL", boxes[0])
    if len(boxes) > 1:
        innermost = min(boxes, key=lambda region: region.area)
        if all(_contains(region.bbox, innermost.bbox) for region in boxes if region is not innermost):
            return _result(DIRECT_CONTAINMENT, "FRAGMENT_LOCAL", innermost)
        return _result(AMBIGUOUS_OWNERSHIP, "UNKNOWN")

    hit = _attached_edges(item, index.edges, gap_em=gap_em, overlap=overlap)
    if len(hit):
        owners = np.unique(index.edge_region[hit])
        if len(owners) == 1:
            return _result(CONNECTED_CALLOUT, "FRAGMENT_LOCAL", index.regions[int(owners[0])])
        return _result(AMBIGUOUS_OWNERSHIP, "UNKNOWN")
    return _result(NO_OWNERSHIP, "UNKNOWN")


def table_cells(regions: Sequence[Region]) -> list[dict[str, Any]]:
    """Every cell rectangle of every lattice on the page."""
    out: list[dict[str, Any]] = []
    for region in regions:
        if region.kind in {"TABLE", "STAMP"}:
            out.extend(region.cells())
    return out


__all__ = [
    "CONTAINMENT_SLACK",
    "LEADER_GAP_EM",
    "LEADER_OVERLAP",
    "MIN_TABLE_COLUMNS",
    "MIN_TABLE_ROWS",
    "REGION_KINDS",
    "SHEET_SCALE_AREA",
    "Region",
    "RegionIndex",
    "attribute",
    "build_index",
    "build_regions",
    "table_cells",
]
