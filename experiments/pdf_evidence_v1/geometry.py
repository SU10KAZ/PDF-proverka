"""Compact geometry: the drawn structure, not the six million strokes.

A sheet of this corpus carries up to 3.8 million vector segments.  Keeping them
is pointless twice over — they do not fit in an artifact, and none of them is a
fact.  What matters is the *structure* they draw: the boundaries, the lattices,
the frames.  Welding collinear strokes into maximal edges turns hundreds of
thousands of segments into hundreds of boundaries and loses nothing a region
needs.

Two things are kept beside the edges so the compaction can be audited rather
than trusted:

* ``slanted_ink_share`` — how much of the drawn length is neither horizontal
  nor vertical.  A page whose ink is mostly slanted is a page whose structure
  this module cannot see, and it says so instead of reporting few boundaries.
* the raw counters — what came in, so the compression ratio is a measurement.

The welding algorithm is the one proven in the v3.0 feasibility track; it is
reimplemented here because this layer is meant to stand on its own.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

#: Tolerance for calling a segment horizontal or vertical, in points.
AXIS_EPS = 0.35
#: Coordinate grid a collinear chain is welded on, in points.
EDGE_GRID = 0.5
#: Shortest edge allowed to be a structural boundary, in points.
MIN_EDGE_LEN = 6.0
#: Tolerance for calling two edges incident, in points.
INCIDENCE_TOL = 1.0
#: Steps a cubic is flattened into.  Curves bound regions here; they are not
#: shapes to be reproduced.
CURVE_STEPS = 4


@dataclass
class CompactGeometry:
    """The structure of one page and the audit of its own compaction."""

    horizontal: np.ndarray = field(default_factory=lambda: np.zeros((0, 4)))
    vertical: np.ndarray = field(default_factory=lambda: np.zeros((0, 4)))
    rectangles: list[list[float]] = field(default_factory=list)
    raw_segments: int = 0
    raw_paths: int = 0
    slanted_ink_share: float = 0.0
    counters: dict[str, int] = field(default_factory=dict)

    @property
    def edges(self) -> int:
        return int(len(self.horizontal) + len(self.vertical))

    def compaction(self) -> dict[str, Any]:
        """Raw versus kept, in numbers rather than in confidence."""
        raw_floats = self.raw_segments * 4
        kept_floats = self.edges * 4 + len(self.rectangles) * 4
        return {
            "raw_segments": self.raw_segments,
            "raw_paths": self.raw_paths,
            "welded_edges": self.edges,
            "rectangles": len(self.rectangles),
            "raw_coordinate_floats": raw_floats,
            "kept_coordinate_floats": kept_floats,
            "compression": round(raw_floats / kept_floats, 2) if kept_floats else None,
            "slanted_ink_share": round(float(self.slanted_ink_share), 4),
        }


def axis_edges(
    segments: np.ndarray,
    *,
    eps: float = AXIS_EPS,
    grid: float = EDGE_GRID,
    min_len: float = MIN_EDGE_LEN,
) -> tuple[np.ndarray, np.ndarray]:
    """Maximal collinear axis-aligned edges as ``(x0, y0, x1, y1)`` rows.

    A drawn boundary is a chain of collinear strokes, not one stroke.  Slanted
    strokes are never welded: they are counted as unstructured ink instead, so
    that a page of diagonals reports low structure rather than false structure.
    """
    if len(segments) == 0:
        return np.zeros((0, 4)), np.zeros((0, 4))
    dx = segments[:, 2] - segments[:, 0]
    dy = segments[:, 3] - segments[:, 1]
    result: dict[str, np.ndarray] = {}
    for name, mask, along in (
        ("H", (np.abs(dy) <= eps) & (np.abs(dx) > eps), 0),
        ("V", (np.abs(dx) <= eps) & (np.abs(dy) > eps), 1),
    ):
        selected = segments[mask]
        if len(selected) == 0:
            result[name] = np.zeros((0, 4))
            continue
        across = 1 - along
        level = np.round(((selected[:, across] + selected[:, across + 2]) / 2.0) / grid) * grid
        low = np.minimum(selected[:, along], selected[:, along + 2])
        high = np.maximum(selected[:, along], selected[:, along + 2])
        order = np.lexsort((low, level))
        level, low, high = level[order], low[order], high[order]
        chains: list[tuple[float, float, float]] = []
        index = 0
        total = len(level)
        while index < total:
            current = level[index]
            start = low[index]
            end = high[index]
            index += 1
            while index < total and level[index] == current and low[index] <= end + grid:
                end = max(end, high[index])
                index += 1
            if end - start >= min_len:
                chains.append((current, start, end))
        if not chains:
            result[name] = np.zeros((0, 4))
            continue
        array = np.asarray(chains, dtype=np.float64)
        if name == "H":
            result[name] = np.column_stack([array[:, 1], array[:, 0], array[:, 2], array[:, 0]])
        else:
            result[name] = np.column_stack([array[:, 0], array[:, 1], array[:, 0], array[:, 2]])
    return result["H"], result["V"]


def incidence_components(
    horizontal: np.ndarray, vertical: np.ndarray, *, tol: float = INCIDENCE_TOL
) -> tuple[np.ndarray, int]:
    """Connected components of *touching* edges.

    Touching is a drawn relation — two boundaries that cross.  It is not
    distance, so nothing here is proximity.
    """
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components

    total = len(horizontal) + len(vertical)
    if total == 0:
        return np.zeros(0, dtype=int), 0
    rows: list[int] = []
    cols: list[int] = []
    if len(horizontal) and len(vertical):
        vx = vertical[:, 0]
        vy0 = vertical[:, 1]
        vy1 = vertical[:, 3]
        for index in range(len(horizontal)):
            x0, y, x1 = horizontal[index, 0], horizontal[index, 1], horizontal[index, 2]
            hit = (vx >= x0 - tol) & (vx <= x1 + tol) & (vy0 <= y + tol) & (vy1 >= y - tol)
            for other in np.nonzero(hit)[0]:
                rows.append(index)
                cols.append(len(horizontal) + int(other))
    graph = coo_matrix((np.ones(len(rows)), (rows, cols)), shape=(total, total))
    count, labels = connected_components(graph, directed=False)
    return labels, count


def slanted_ink_share(segments: np.ndarray, *, eps: float = AXIS_EPS) -> float:
    """Share of stroke length that is neither horizontal nor vertical."""
    if len(segments) == 0:
        return 0.0
    dx = segments[:, 2] - segments[:, 0]
    dy = segments[:, 3] - segments[:, 1]
    length = np.hypot(dx, dy)
    total = float(length.sum())
    if total <= 0:
        return 0.0
    axis = (np.abs(dy) <= eps) | (np.abs(dx) <= eps)
    return float(length[~axis].sum() / total)


def compact(
    segments: np.ndarray,
    rectangles: list[list[float]],
    *,
    raw_paths: int = 0,
    counters: dict[str, int] | None = None,
) -> CompactGeometry:
    horizontal, vertical = axis_edges(segments)
    return CompactGeometry(
        horizontal=horizontal,
        vertical=vertical,
        rectangles=list(rectangles),
        raw_segments=int(len(segments)),
        raw_paths=int(raw_paths),
        slanted_ink_share=slanted_ink_share(segments),
        counters=dict(counters or {}),
    )


__all__ = [
    "AXIS_EPS",
    "CURVE_STEPS",
    "EDGE_GRID",
    "INCIDENCE_TOL",
    "MIN_EDGE_LEN",
    "CompactGeometry",
    "axis_edges",
    "compact",
    "incidence_components",
    "slanted_ink_share",
]
