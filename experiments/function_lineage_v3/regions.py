"""Deterministic FunctionRegion prototype (Function Lineage v3.0).

Research only.  The module turns one page of geometry into delimited regions
and attributes text to them.  It never calls a model, never uses vision and is
not wired into any production path.

The rules it obeys are the v2.9 rules, carried forward unchanged:

1. **Proximity is never proof.**  A span is attributed because a drawn boundary
   *contains* it, because a drawn boundary it *touches* belongs to exactly one
   structure, or because it sits in exactly one cell of a drawn lattice.  Being
   the nearest thing to something never attributes anything.
2. **A region that is the sheet is not a fragment.**  A region covering most of
   the page restates ``sheet == fragment`` under a new name and therefore
   resolves to ``SHEET_SHARED``, exactly as a content block did.
3. **A lone region is not evidence.**  A page with one region does not thereby
   own its facts; ownership needs a claim, never the absence of a rival.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import numpy as np

from .page_geometry import (
    INCIDENCE_TOL,
    PageGeometry,
    axis_edges,
    incidence_components,
)

SCHEMA_VERSION = "function-region.v3.0"

REGION_KINDS = (
    "SHEET_FRAME",
    "STAMP",
    "TABLE",
    "BOX",
    "EDGE_GROUP",
    "TEXT_SECTION",
)

OWNERSHIP_RELATIONS = (
    "DIRECT_CONTAINMENT",
    "TABLE_CELL",
    "CONNECTED_CALLOUT",
    "SHEET_SHARED",
    "AMBIGUOUS",
    "UNKNOWN",
)

#: Relations that establish a region-local fact.  Everything else leaves the
#: fact where it was.
PROVING_RELATIONS = frozenset({"DIRECT_CONTAINMENT", "TABLE_CELL", "CONNECTED_CALLOUT"})

#: A region covering at least this share of the page never confers ownership.
SHEET_SCALE_AREA = 0.55
#: A lattice needs at least this many rulings in each direction to be a table.
MIN_TABLE_ROWS = 3
MIN_TABLE_COLUMNS = 2
#: A ruling must span this share of the lattice bbox to be a row/column line.
RULING_SPAN = 0.55
#: Slack, in points, when testing whether a span sits inside a boundary.
CONTAINMENT_SLACK = 1.0
#: A leader line is drawn beside its label, not through it.  The gap is a
#: fraction of the label's own font size, so the rule scales with the drawing
#: instead of being a tuned number of points.  Sensitivity to this value is
#: measured and reported, never chosen for the answer it gives.
LEADER_GAP_EM = 0.3
#: The line must run *along* the label for at least this share of the label's
#: length.  This is what separates an attached leader from the nearest stroke:
#: a leader is co-extensive with its text, a neighbour merely close to it.
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
    """Levels of edges that span most of the lattice in the given direction."""
    if len(edges) == 0:
        return []
    if axis == 0:  # horizontal rulings -> y levels, measured on x
        span = edges[:, 2] - edges[:, 0]
        extent = max(bbox[2] - bbox[0], 1e-6)
        level = edges[:, 1]
    else:
        span = edges[:, 3] - edges[:, 1]
        extent = max(bbox[3] - bbox[1], 1e-6)
        level = edges[:, 0]
    keep = span >= RULING_SPAN * extent
    return sorted({round(float(value), 1) for value in level[keep]})


def build_regions(geometry: PageGeometry) -> list[Region]:
    """Regions of one page, ordered deterministically by position and size."""
    horizontal, vertical = axis_edges(geometry.segments)
    if len(horizontal) == 0 and len(vertical) == 0:
        return _text_only_regions(geometry)
    labels, count = incidence_components(horizontal, vertical, tol=INCIDENCE_TOL)
    stacked = np.vstack([horizontal, vertical]) if len(horizontal) and len(vertical) else (
        horizontal if len(horizontal) else vertical
    )
    page_area = max(geometry.width * geometry.height, 1e-6)
    regions: list[Region] = []
    for component in range(count):
        mask = labels == component
        member = stacked[mask]
        if len(member) == 0:
            continue
        bbox = _bbox(member)
        n_horizontal = int(((member[:, 1] == member[:, 3]) & mask[:len(stacked)][mask]).sum()) if False else int((member[:, 1] == member[:, 3]).sum())
        n_vertical = len(member) - n_horizontal
        area_share = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) / page_area
        rows = _rulings(member[member[:, 1] == member[:, 3]], 0, bbox)
        columns = _rulings(member[member[:, 0] == member[:, 2]], 1, bbox)
        if area_share >= SHEET_SCALE_AREA and len(member) <= 8:
            kind = "SHEET_FRAME"
        elif area_share >= SHEET_SCALE_AREA:
            kind = "EDGE_GROUP"
        elif geometry.in_stamp_zone(bbox) and len(rows) >= MIN_TABLE_ROWS:
            kind = "STAMP"
        elif len(rows) >= MIN_TABLE_ROWS and len(columns) >= MIN_TABLE_COLUMNS:
            kind = "TABLE"
        elif len(member) == 4 and n_horizontal == 2 and n_vertical == 2:
            kind = "BOX"
        else:
            kind = "EDGE_GROUP"
        regions.append(Region(
            region_id=f"reg_{geometry.page:04d}_{component:05d}",
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


def _text_only_regions(geometry: PageGeometry) -> list[Region]:
    """A page with no drawn boundary still has native paragraph rectangles."""
    regions: list[Region] = []
    for index, block in enumerate(geometry.text_blocks):
        regions.append(Region(
            region_id=f"reg_{geometry.page:04d}_t{index:04d}",
            kind="TEXT_SECTION",
            bbox=list(block["bbox"]),
            edge_count=0,
            horizontal=0,
            vertical=0,
        ))
    return regions


def _leader_attached(
    span: Mapping[str, Any],
    edges: np.ndarray,
    *,
    gap_em: float = LEADER_GAP_EM,
    overlap: float = LEADER_OVERLAP,
) -> bool:
    """Is a boundary drawn *along* the label?

    Two conditions, both required.  The edge must come within ``gap_em`` of the
    label — a leader is drawn beside the text, never through it — and it must
    run along the label's own direction for ``overlap`` of the label's length.
    The second condition is what keeps this from being "the nearest stroke":
    a stroke that merely passes nearby fails it.
    """
    if len(edges) == 0:
        return False
    return bool(len(_attached_edges(span, edges, gap_em=gap_em, overlap=overlap)))


def _attached_edges(
    span: Mapping[str, Any],
    edges: np.ndarray,
    *,
    gap_em: float = LEADER_GAP_EM,
    overlap: float = LEADER_OVERLAP,
) -> np.ndarray:
    """Indices of edges that are drawn along this label."""
    if len(edges) == 0:
        return np.zeros(0, dtype=int)
    x0, y0, x1, y1 = span["bbox"]
    gap = gap_em * float(span.get("size") or 0.0)
    ex0 = np.minimum(edges[:, 0], edges[:, 2])
    ex1 = np.maximum(edges[:, 0], edges[:, 2])
    ey0 = np.minimum(edges[:, 1], edges[:, 3])
    ey1 = np.maximum(edges[:, 1], edges[:, 3])
    near = (ex0 <= x1 + gap) & (ex1 >= x0 - gap) & (ey0 <= y1 + gap) & (ey1 >= y0 - gap)
    if not np.any(near):
        return np.zeros(0, dtype=int)
    if span.get("vertical"):
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

    def region(self, index: int) -> Region:
        return self.regions[index]


def build_index(geometry: PageGeometry, regions: Sequence[Region]) -> RegionIndex:
    """Precompute the arrays every attribution needs.

    Rule 2 lives here: a region that covers most of the sheet is dropped from
    the local set, so it can never confer ownership on anything.
    """
    page_area = max(geometry.width * geometry.height, 1e-6)
    local: list[int] = []
    for index, region in enumerate(regions):
        if region.kind == "SHEET_FRAME":
            continue
        if region.area / page_area >= SHEET_SCALE_AREA:
            continue
        local.append(index)
    chunks = [regions[index].edges for index in local if len(regions[index].edges)]
    owners = [
        np.full(len(regions[index].edges), index, dtype=np.int64)
        for index in local if len(regions[index].edges)
    ]
    edges = np.vstack(chunks) if chunks else np.zeros((0, 4))
    edge_region = np.concatenate(owners) if owners else np.zeros(0, dtype=np.int64)
    boxes = [index for index in local if regions[index].kind == "BOX"]
    tables = [index for index in local if regions[index].kind in {"TABLE", "STAMP"}]
    return RegionIndex(
        regions=list(regions), local=local, edges=edges,
        edge_region=edge_region, boxes=boxes, tables=tables,
    )


def attribute(
    geometry: PageGeometry,
    index: RegionIndex,
    span: Mapping[str, Any],
    *,
    gap_em: float = LEADER_GAP_EM,
    overlap: float = LEADER_OVERLAP,
) -> dict[str, Any]:
    """Attribute one rectangle on the page to at most one region."""
    bbox = span["bbox"]
    if geometry.in_stamp_zone(bbox):
        return {
            "relation": "SHEET_SHARED", "region_id": None, "region_kind": "STAMP_ZONE",
            "applicability": "SHEET_SHARED", "cell": None,
        }

    cells = []
    for position in index.tables:
        region = index.regions[position]
        if not _contains(region.bbox, bbox):
            continue
        cell = _cell_of(region, bbox)
        if cell is not None:
            cells.append((region, cell))
    if len(cells) == 1:
        region, cell = cells[0]
        return {
            "relation": "TABLE_CELL", "region_id": region.region_id,
            "region_kind": region.kind, "applicability": "FRAGMENT_LOCAL",
            "cell": {"row": cell[0], "column": cell[1]},
        }
    if len(cells) > 1:
        return _ambiguous()

    boxes = [
        index.regions[position] for position in index.boxes
        if _contains(index.regions[position].bbox, bbox)
    ]
    if len(boxes) == 1:
        return {
            "relation": "DIRECT_CONTAINMENT", "region_id": boxes[0].region_id,
            "region_kind": boxes[0].kind, "applicability": "FRAGMENT_LOCAL", "cell": None,
        }
    if len(boxes) > 1:
        innermost = min(boxes, key=lambda region: region.area)
        if all(_contains(region.bbox, innermost.bbox) for region in boxes if region is not innermost):
            return {
                "relation": "DIRECT_CONTAINMENT", "region_id": innermost.region_id,
                "region_kind": innermost.kind, "applicability": "FRAGMENT_LOCAL", "cell": None,
            }
        return _ambiguous()

    hit = _attached_edges(span, index.edges, gap_em=gap_em, overlap=overlap)
    if len(hit):
        owners = np.unique(index.edge_region[hit])
        if len(owners) == 1:
            region = index.regions[int(owners[0])]
            return {
                "relation": "CONNECTED_CALLOUT", "region_id": region.region_id,
                "region_kind": region.kind, "applicability": "FRAGMENT_LOCAL", "cell": None,
            }
        return _ambiguous()
    return {
        "relation": "UNKNOWN", "region_id": None, "region_kind": None,
        "applicability": "UNKNOWN", "cell": None,
    }


def _ambiguous() -> dict[str, Any]:
    return {
        "relation": "AMBIGUOUS", "region_id": None, "region_kind": None,
        "applicability": "UNKNOWN", "cell": None,
    }
