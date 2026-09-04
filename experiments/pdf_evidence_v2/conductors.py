"""What each stroke is, and where the drawing says two strokes are connected.

This is the module the verdict rests on, so its shape is an argument rather
than a pipeline.

**Nature first, connectivity second.**  A table lattice makes hundreds of
perfect right-angle crossings per page and a sheet frame makes four.  A
connectivity pass run before the strokes are classified spends its budget on
furniture and then reports a graph.  Every edge is therefore first told what it
is — by the region it belongs to, by the sheet border it hugs, by the text it
runs along — and only what is left is asked about electricity.

**Connectivity is seeded, not assumed.**  One mark in a schematic exists solely
to state that two conductors are joined: the junction dot.  Nobody draws a dot
on a table ruling.  Proven conductors therefore start at dots and grow along
drawn continuations, and stop at the boundary of anything already classified.

**A device is a gap with ink in it.**  A schematic does not draw a breaker next
to a wire, it cuts the wire and draws the breaker into the gap.  On the control
page of this corpus the bus and the feeder above the first breaker are
twenty-eight points apart and are one branch.  So proof also crosses a symbol
cluster — through the drawn contact at each of its terminals, never across the
gap itself.

**What is not reached stays UNKNOWN.**  Not "probably a wire".  A sheet whose
draughtsman drew no dots yields no proven conductors here, and that is the
correct output for it.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

import numpy as np

from experiments.pdf_evidence_v1 import structure as v1_structure

from . import symbols as symbols_module
from .contract import (
    COINCIDENT_ENDPOINTS,
    CONNECTED_JUNCTION,
    CONTINUOUS_POLYLINE,
    CROSSING_WITHOUT_JUNCTION,
    DECORATIVE_LINE,
    DIMENSION_LINE,
    EQUIPMENT_PORT,
    FRAME,
    HOP_PROVEN_NON_CONNECTION,
    JUNCTION_DOT,
    LEADER_CALLOUT,
    SCHEMATIC_CONDUCTOR,
    TABLE_GRID,
    TEE_TERMINATION,
    TEXT_UNDERLINE,
    UNKNOWN,
)
from .page import PageData

#: Tolerance for saying two drawn things touch, in points.  One point on a
#: sheet whose text is ten: a draughtsman's snap, not a search radius.
TOUCH_TOL = 1.0
#: Distance from an edge's own end within which a meeting is an endpoint
#: meeting rather than a tee.
END_TOL = 1.5
#: A junction dot must sit this close to the edge it is claimed to join.
DOT_TOL = 1.75
#: A crossover hop must sit this close to what it annotates.
HOP_TOL = 4.0
#: A drawing frame hugs the sheet edge and spans almost all of it.  Both halves
#: are required: a bus in the middle of the page spans as much and is not a
#: frame, and a short tick at the margin hugs as closely and is not one either.
FRAME_MARGIN = 0.075
FRAME_SPAN = 0.88
#: An underline may exceed its label by this factor and no more.  Beyond it the
#: stroke is not underlining the label — the label is written beside the
#: stroke, which is the whole difference between furniture and a feeder.
UNDERLINE_SLACK = 1.35
#: A leader shelf is short, has slanted ink on exactly one end, and carries a
#: label.  Without the third condition the rule fires on every symbol stub.
LEADER_MAX_LEN = 48.0
#: A dimension tick is a short slanted stroke at an edge's end.
TICK_MAX_LEN = 6.0
#: Endpoints tested against edge bodies per vectorized chunk.
TEE_CHUNK = 512
#: A cluster with more terminals than this is not a device that joins them.
MAX_CLUSTER_TERMINALS = 12
#: A series device sits in a gap in the wire.  These bound the rule: how long a
#: gap may be, how far off the line its ink may stray, and how much of the gap
#: that ink must actually cover before the gap is called a device rather than a
#: gap.  Collinearity does most of the work — two conductor pieces on the same
#: line, consecutive, with drawn ink between them is a switch, and a stray wire
#: elsewhere on the sheet satisfies none of the three.
SERIES_GAP_MAX = 40.0
SERIES_CORRIDOR = 6.0
SERIES_COVER = 0.6
SERIES_LEVEL_TOL = 1.0
#: Growth is a fixpoint; this bounds it against a pathological page.
MAX_ROUNDS = 12


@dataclass
class SeriesGap:
    """A break in one line, filled with the ink of the device that made it."""

    axis_low: int
    axis_high: int
    gap: float
    covered: float
    clusters: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "edges": [self.axis_low, self.axis_high],
            "gap": round(float(self.gap), 2),
            "covered": round(float(self.covered), 3),
            "clusters": list(self.clusters),
        }


@dataclass
class Junction:
    """One place where the drawing says two conductors are joined."""

    junction_id: str
    point: tuple[float, float]
    evidence: str
    edges: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "junction_id": self.junction_id,
            "point": [round(float(value), 2) for value in self.point],
            "evidence": self.evidence,
            "edges": list(self.edges),
        }


@dataclass
class Crossing:
    """One place where two edges cross, and what the drawing says about it."""

    point: tuple[float, float]
    edges: tuple[int, int]
    verdict: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "point": [round(float(value), 2) for value in self.point],
            "edges": list(self.edges),
            "verdict": self.verdict,
        }


@dataclass
class EdgeFacts:
    """Everything one page's strokes turned out to be."""

    nature: list[str]
    region: np.ndarray
    conductor: np.ndarray
    conductor_evidence: list[str | None]
    junctions: list[Junction] = field(default_factory=list)
    crossings: list[Crossing] = field(default_factory=list)
    clusters: list[symbols_module.SymbolCluster] = field(default_factory=list)
    cluster_terminals: dict[int, list[int]] = field(default_factory=dict)
    text_ink_clusters: set[int] = field(default_factory=set)
    series_gaps: list[SeriesGap] = field(default_factory=list)
    bridging_clusters: set[int] = field(default_factory=set)
    adjacency: dict[int, set[int]] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)

    def nature_counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for value in self.nature:
            out[value] = out.get(value, 0) + 1
        return dict(sorted(out.items()))


# ---------------------------------------------------------------------------
# geometry helpers
# ---------------------------------------------------------------------------


def _edge_arrays(data: PageData):
    edges = data.strokes.edges
    horizontal = data.strokes.horizontal_mask
    low = np.where(horizontal, np.minimum(edges[:, 0], edges[:, 2]), np.minimum(edges[:, 1], edges[:, 3]))
    high = np.where(horizontal, np.maximum(edges[:, 0], edges[:, 2]), np.maximum(edges[:, 1], edges[:, 3]))
    level = np.where(horizontal, edges[:, 1], edges[:, 0])
    return edges, horizontal, low, high, level


def _components(data: PageData) -> np.ndarray:
    horizontal = data.strokes.edges[data.strokes.horizontal_mask]
    vertical = data.strokes.edges[~data.strokes.horizontal_mask]
    labels, _ = v1_structure.geometry_module.incidence_components(horizontal, vertical)
    if len(labels) != len(data.strokes.edges):
        return np.full(len(data.strokes.edges), -1, dtype=np.int64)
    return labels


def _region_kind_by_component(data: PageData) -> dict[int, str]:
    """Component index to the region kind V1 gave it.

    V1 mints ``reg_<page>_<component>``; reading the component index back out
    of the identifier is what keeps the two layers from disagreeing about which
    lattice is a table.
    """
    out: dict[int, str] = {}
    for region in data.regions:
        parts = region.region_id.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            out[int(parts[1])] = region.kind
    return out


def _on_edge_mask(
    points: np.ndarray, low: np.ndarray, high: np.ndarray,
    level: np.ndarray, horizontal: np.ndarray, tol: float,
) -> np.ndarray:
    """``(len(points), len(edges))`` mask of point-lies-on-edge."""
    along = np.where(horizontal[None, :], points[:, 0:1], points[:, 1:2])
    across = np.where(horizontal[None, :], points[:, 1:2], points[:, 0:1])
    return (
        (np.abs(across - level[None, :]) <= tol)
        & (along >= low[None, :] - tol)
        & (along <= high[None, :] + tol)
    )


def _runs_along(
    box: Sequence[float], vertical: bool, size: float,
    low: np.ndarray, high: np.ndarray, level: np.ndarray, horizontal: np.ndarray,
    *, gap_em: float = v1_structure.LEADER_GAP_EM,
    overlap: float = v1_structure.LEADER_OVERLAP,
) -> np.ndarray:
    """Indices of edges drawn *along* a label — V1's rule, aimed at one edge.

    V1 asked which *region* a stroke belonged to and therefore lost every label
    on a schematic, where one component spans the sheet.  The rule is unchanged;
    only its answer is finer.
    """
    gap = gap_em * max(size, 1e-6)
    x0, y0, x1, y1 = (float(value) for value in box)
    ex0 = np.where(horizontal, low, level)
    ex1 = np.where(horizontal, high, level)
    ey0 = np.where(horizontal, level, low)
    ey1 = np.where(horizontal, level, high)
    near = (ex0 <= x1 + gap) & (ex1 >= x0 - gap) & (ey0 <= y1 + gap) & (ey1 >= y0 - gap)
    if not np.any(near):
        return np.zeros(0, dtype=int)
    if vertical:
        length = max(y1 - y0, 1e-6)
        run = np.minimum(ey1, y1) - np.maximum(ey0, y0)
        parallel = ~horizontal
    else:
        length = max(x1 - x0, 1e-6)
        run = np.minimum(ex1, x1) - np.maximum(ex0, x0)
        parallel = horizontal
    return np.nonzero(near & parallel & (run >= overlap * length))[0]


# ---------------------------------------------------------------------------
# nature
# ---------------------------------------------------------------------------


def classify_nature(data: PageData) -> tuple[list[str], np.ndarray, dict[str, int]]:
    """Tell every edge what it is, before anyone asks whether it conducts."""
    edges, horizontal, low, high, level = _edge_arrays(data)
    total = len(edges)
    nature = [UNKNOWN] * total
    region = _components(data)
    kinds = _region_kind_by_component(data)
    counters = {
        "edges": total,
        "frame_by_region": 0,
        "frame_by_margin": 0,
        "label_side_strokes": 0,
        "underline_rejected_too_long": 0,
        "leader_shelves": 0,
        "dimension_lines": 0,
    }
    for index in range(total):
        kind = kinds.get(int(region[index]))
        if kind == "SHEET_FRAME":
            nature[index] = FRAME
            counters["frame_by_region"] += 1
        elif kind in {"TABLE", "STAMP"}:
            nature[index] = TABLE_GRID

    # The drawing frame.  V1's SHEET_FRAME catches the outer page rectangle and
    # not the inner border a draughtsman rules inside it; on the control page
    # that border is the third-longest stroke and would otherwise be offered to
    # the graph as the longest bus on the sheet.  Control C of this track.
    span = high - low
    page_span = np.where(horizontal, data.width, data.height)
    margin = np.where(horizontal, data.height, data.width) * FRAME_MARGIN
    hugs = np.minimum(level, np.where(horizontal, data.height, data.width) - level) <= margin
    frame_like = hugs & (span >= FRAME_SPAN * page_span)
    for index in np.nonzero(frame_like)[0]:
        if nature[index] == UNKNOWN:
            nature[index] = FRAME
            counters["frame_by_margin"] += 1

    labelled: set[int] = set()
    for label in data.labels:
        box = label["bbox"]
        extent = (box[3] - box[1]) if label["vertical"] else (box[2] - box[0])
        for index in _runs_along(box, bool(label["vertical"]), float(label["size"]),
                                 low, high, level, horizontal):
            if nature[index] in {FRAME, TABLE_GRID}:
                continue
            counters["label_side_strokes"] += 1
            labelled.add(int(index))
            if span[index] <= UNDERLINE_SLACK * max(extent, 1e-6):
                nature[index] = TEXT_UNDERLINE
            else:
                counters["underline_rejected_too_long"] += 1

    slanted = data.strokes.slanted
    if len(slanted):
        ends = np.vstack([slanted[:, :2], slanted[:, 2:]])
        tick = np.hypot(slanted[:, 2] - slanted[:, 0], slanted[:, 3] - slanted[:, 1]) <= TICK_MAX_LEN
        short = np.concatenate([tick, tick])
        from scipy.spatial import cKDTree

        tree = cKDTree(ends)
        for index in range(total):
            if nature[index] != UNKNOWN or span[index] > LEADER_MAX_LEN:
                continue
            if horizontal[index]:
                start, stop = (low[index], level[index]), (high[index], level[index])
            else:
                start, stop = (level[index], low[index]), (level[index], high[index])
            at_start = tree.query_ball_point(start, r=TOUCH_TOL)
            at_stop = tree.query_ball_point(stop, r=TOUCH_TOL)
            if at_start and at_stop and any(short[p] for p in at_start) and any(short[p] for p in at_stop):
                nature[index] = DIMENSION_LINE
                counters["dimension_lines"] += 1
            elif bool(at_start) != bool(at_stop) and index in labelled:
                nature[index] = LEADER_CALLOUT
                counters["leader_shelves"] += 1
    return nature, region, counters


# ---------------------------------------------------------------------------
# contacts
# ---------------------------------------------------------------------------

#: Natures a conductor proof may never overwrite.  Each was decided by the
#: drawing before electricity was mentioned.
FURNITURE = frozenset({FRAME, TABLE_GRID, TEXT_UNDERLINE, DIMENSION_LINE,
                       LEADER_CALLOUT, DECORATIVE_LINE})


def _dot_junctions(
    data: PageData, eligible: np.ndarray, low, high, level, horizontal,
) -> tuple[list[Junction], dict[str, int]]:
    counters = {"dot_shaped_ink": 0, "dots_joining_conductors": 0, "dot_shaped_ink_off_any_edge": 0}
    dots = [blob for blob in data.strokes.blobs if blob.is_dot_shaped]
    counters["dot_shaped_ink"] = len(dots)
    if not dots:
        return [], counters
    centres = np.array([blob.centre for blob in dots])
    mask = _on_edge_mask(centres, low, high, level, horizontal, DOT_TOL) & eligible[None, :]
    junctions: list[Junction] = []
    for position in range(len(dots)):
        members = np.nonzero(mask[position])[0]
        orientations = {bool(horizontal[member]) for member in members}
        if len(members) < 2 and len(orientations) < 2:
            counters["dot_shaped_ink_off_any_edge"] += 1
            continue
        counters["dots_joining_conductors"] += 1
        junctions.append(Junction(
            junction_id=f"j:p{data.page:04d}:d{position:05d}",
            point=(round(float(centres[position][0]), 2), round(float(centres[position][1]), 2)),
            evidence=JUNCTION_DOT,
            edges=tuple(int(member) for member in members),
        ))
    return junctions, counters


def _hop_junctions(
    data: PageData, eligible: np.ndarray, low, high, level, horizontal,
) -> tuple[list[Junction], list[Crossing], dict[str, int]]:
    """The crossover hop, read as the draughtsman meant it.

    A hop is two statements at once and the second one is usually thrown away.
    It continues the conductor it interrupts — the wire is one wire, drawn in
    two pieces with a bridge — and it states that the wire it arches over is
    *not* joined to it.  Both are recorded.
    """
    counters = {"hops": 0, "hops_continuing_a_conductor": 0, "hops_proving_non_connection": 0}
    hops = [arc for arc in data.strokes.arcs if arc.hop]
    counters["hops"] = len(hops)
    junctions: list[Junction] = []
    crossings: list[Crossing] = []
    if not hops:
        return junctions, crossings, counters
    for position, arc in enumerate(hops):
        x0, y0, x1, y1 = arc.bbox
        wide = (x1 - x0) >= (y1 - y0)
        if wide:
            feet = np.array([[x0, (y0 + y1) / 2.0], [x1, (y0 + y1) / 2.0]])
        else:
            feet = np.array([[(x0 + x1) / 2.0, y0], [(x0 + x1) / 2.0, y1]])
        touched = _on_edge_mask(feet, low, high, level, horizontal, HOP_TOL) & eligible[None, :]
        members = sorted({int(index) for index in np.nonzero(touched.any(axis=0))[0]})
        aligned = [index for index in members if bool(horizontal[index]) == wide]
        if len(aligned) >= 2:
            counters["hops_continuing_a_conductor"] += 1
            junctions.append(Junction(
                junction_id=f"j:p{data.page:04d}:h{position:05d}",
                point=(round(float(arc.centre[0]), 2), round(float(arc.centre[1]), 2)),
                evidence=CONTINUOUS_POLYLINE,
                edges=tuple(aligned),
            ))
        centre = np.array([arc.centre])
        under = _on_edge_mask(centre, low, high, level, horizontal, HOP_TOL) & eligible[None, :]
        for index in np.nonzero(under[0])[0]:
            if bool(horizontal[index]) == wide:
                continue
            counters["hops_proving_non_connection"] += 1
            crossings.append(Crossing(
                point=(round(float(arc.centre[0]), 2), round(float(arc.centre[1]), 2)),
                edges=(int(aligned[0]) if aligned else int(index), int(index)),
                verdict=HOP_PROVEN_NON_CONNECTION,
            ))
    return junctions, crossings, counters


def _contact_junctions(
    data: PageData, eligible: np.ndarray, low, high, level, horizontal,
) -> tuple[list[Junction], dict[str, int]]:
    from scipy.spatial import cKDTree

    counters = {"endpoint_meetings": 0, "tee_meetings": 0}
    pool = np.nonzero(eligible)[0]
    junctions: list[Junction] = []
    if not len(pool):
        return junctions, counters
    starts = np.column_stack([
        np.where(horizontal[pool], low[pool], level[pool]),
        np.where(horizontal[pool], level[pool], low[pool]),
    ])
    stops = np.column_stack([
        np.where(horizontal[pool], high[pool], level[pool]),
        np.where(horizontal[pool], level[pool], high[pool]),
    ])
    points = np.vstack([starts, stops])
    owner = np.concatenate([pool, pool])
    tree = cKDTree(points)
    seen: set[tuple[int, int]] = set()
    for left, right in tree.query_pairs(r=TOUCH_TOL, output_type="ndarray"):
        a, b = int(owner[left]), int(owner[right])
        if a == b:
            continue
        key = (min(a, b), max(a, b))
        if key in seen:
            continue
        seen.add(key)
        middle = (points[left] + points[right]) / 2.0
        counters["endpoint_meetings"] += 1
        junctions.append(Junction(
            junction_id=f"j:p{data.page:04d}:e{len(junctions):05d}",
            point=(round(float(middle[0]), 2), round(float(middle[1]), 2)),
            evidence=COINCIDENT_ENDPOINTS,
            edges=(a, b),
        ))
    for start in range(0, len(points), TEE_CHUNK):
        chunk = points[start:start + TEE_CHUNK]
        mask = _on_edge_mask(chunk, low, high, level, horizontal, TOUCH_TOL) & eligible[None, :]
        for row, column in zip(*np.nonzero(mask)):
            source = int(owner[start + row])
            target = int(column)
            if source == target:
                continue
            along = chunk[row][0] if horizontal[target] else chunk[row][1]
            if abs(along - low[target]) <= END_TOL or abs(along - high[target]) <= END_TOL:
                continue
            counters["tee_meetings"] += 1
            junctions.append(Junction(
                junction_id=f"j:p{data.page:04d}:t{len(junctions):05d}",
                point=(round(float(chunk[row][0]), 2), round(float(chunk[row][1]), 2)),
                evidence=TEE_TERMINATION,
                edges=(source, target),
            ))
    return junctions, counters


def _crossings(
    data: PageData, eligible: np.ndarray, low, high, level, horizontal,
    dot_points: np.ndarray,
) -> tuple[list[Crossing], dict[str, int]]:
    counters = {"crossings": 0, "crossings_rejected": 0}
    pool = np.nonzero(eligible)[0]
    h_pool = pool[horizontal[pool]]
    v_pool = pool[~horizontal[pool]]
    crossings: list[Crossing] = []
    if not len(h_pool) or not len(v_pool):
        return crossings, counters
    step = max(1, 4_000_000 // max(len(v_pool), 1))
    for start in range(0, len(h_pool), step):
        chunk = h_pool[start:start + step]
        hx0, hx1 = low[chunk][:, None], high[chunk][:, None]
        hy = level[chunk][:, None]
        vx = level[v_pool][None, :]
        vy0, vy1 = low[v_pool][None, :], high[v_pool][None, :]
        interior = (
            (vx > hx0 + END_TOL) & (vx < hx1 - END_TOL)
            & (hy > vy0 + END_TOL) & (hy < vy1 - END_TOL)
        )
        for row, column in zip(*np.nonzero(interior)):
            point = (float(vx[0, column]), float(hy[row, 0]))
            counters["crossings"] += 1
            joined = bool(
                len(dot_points)
                and (np.hypot(dot_points[:, 0] - point[0], dot_points[:, 1] - point[1]) <= DOT_TOL).any()
            )
            if not joined:
                counters["crossings_rejected"] += 1
            crossings.append(Crossing(
                point=(round(point[0], 2), round(point[1], 2)),
                edges=(int(chunk[row]), int(v_pool[column])),
                verdict=CONNECTED_JUNCTION if joined else CROSSING_WITHOUT_JUNCTION,
            ))
    return crossings, counters


# ---------------------------------------------------------------------------
# proof
# ---------------------------------------------------------------------------


def _cluster_terminals(
    clusters: Sequence[symbols_module.SymbolCluster],
    edges: np.ndarray, eligible: np.ndarray,
    low, high, level, horizontal, slanted: np.ndarray,
) -> dict[int, list[int]]:
    """Eligible axis edges in drawn contact with a cluster's ink, from outside.

    Contact means the same two things it means everywhere else here: a shared
    endpoint, or an endpoint on a body.  A conductor merely passing through the
    cluster's bounding box is not a terminal — otherwise a bus crossing a busy
    area acquires a terminal on every symbol it passes.
    """
    from scipy.spatial import cKDTree

    if not len(clusters):
        return {}
    member_points: list[np.ndarray] = []
    member_cluster: list[np.ndarray] = []
    for cluster in clusters:
        rows: list[np.ndarray] = []
        if cluster.axis_members:
            rows.append(edges[list(cluster.axis_members)])
        if cluster.slanted_members:
            rows.append(slanted[list(cluster.slanted_members)])
        if not rows:
            continue
        stack = np.vstack(rows)
        points = np.vstack([stack[:, :2], stack[:, 2:]])
        member_points.append(points)
        member_cluster.append(np.full(len(points), cluster.index, dtype=np.int64))
    if not member_points:
        return {}
    points = np.vstack(member_points)
    owner = np.concatenate(member_cluster)
    membership: dict[int, set[int]] = defaultdict(set)
    for cluster in clusters:
        for index in cluster.axis_members:
            membership[cluster.index].add(int(index))

    terminals: dict[int, set[int]] = defaultdict(set)
    pool = np.nonzero(eligible)[0]
    if not len(pool):
        return {}
    tree = cKDTree(points)
    starts = np.column_stack([
        np.where(horizontal[pool], low[pool], level[pool]),
        np.where(horizontal[pool], level[pool], low[pool]),
    ])
    stops = np.column_stack([
        np.where(horizontal[pool], high[pool], level[pool]),
        np.where(horizontal[pool], level[pool], high[pool]),
    ])
    for position, edge_index in enumerate(pool):
        for endpoint in (starts[position], stops[position]):
            for hit in tree.query_ball_point(endpoint, r=TOUCH_TOL):
                cluster_index = int(owner[hit])
                if int(edge_index) in membership[cluster_index]:
                    continue
                terminals[cluster_index].add(int(edge_index))
    # …and a cluster stroke ending on an edge body: the stub that drops from a
    # bus into a breaker touches the bus in its middle, not at its end.
    for start in range(0, len(points), TEE_CHUNK):
        chunk = points[start:start + TEE_CHUNK]
        mask = _on_edge_mask(chunk, low, high, level, horizontal, TOUCH_TOL) & eligible[None, :]
        for row, column in zip(*np.nonzero(mask)):
            cluster_index = int(owner[start + row])
            if int(column) in membership[cluster_index]:
                continue
            terminals[cluster_index].add(int(column))
    return {key: sorted(value) for key, value in sorted(terminals.items())}


def _text_ink_clusters(
    data: PageData, clusters: Sequence[symbols_module.SymbolCluster]
) -> set[int]:
    """Clusters that are letters, not devices.

    V1's first finding was that AutoCAD draws SHX text as vectors and puts the
    readable string in an annotation with its own rectangle.  Those vectors are
    slanted ink and would otherwise cluster into thousands of tiny "symbols" —
    and where a label sits between two collinear wires, a letter would become
    the device that joins them.  The annotation rectangle is an independent
    channel saying where the letters are, so it is used to keep them out.
    """
    boxes = np.array([label["bbox"] for label in data.labels]) if data.labels else np.zeros((0, 4))
    if not len(boxes) or not clusters:
        return set()
    out: set[int] = set()
    for cluster in clusters:
        x0, y0, x1, y1 = cluster.bbox
        inside = (
            (boxes[:, 0] <= x0 + TOUCH_TOL) & (boxes[:, 2] >= x1 - TOUCH_TOL)
            & (boxes[:, 1] <= y0 + TOUCH_TOL) & (boxes[:, 3] >= y1 - TOUCH_TOL)
        )
        if bool(inside.any()):
            out.add(cluster.index)
    return out


def _series_gaps(
    clusters: Sequence[symbols_module.SymbolCluster],
    text_ink: set[int],
    eligible: np.ndarray, box_edge: np.ndarray, table_edge: np.ndarray,
    low, high, level, horizontal,
) -> list[SeriesGap]:
    """Find the breaks a series device makes in a line, and only those.

    A schematic does not draw a breaker beside the wire; it cuts the wire.  The
    two halves are therefore collinear, consecutive, and separated by a gap the
    device's own ink fills.  All three conditions are drawn facts, and the
    first two are what keeps this from being "the nearest wire": a stroke one
    point off the line fails collinearity outright.
    """
    usable = eligible & ~box_edge & ~table_edge
    pool = np.nonzero(usable)[0]
    candidates = [c for c in clusters if c.index not in text_ink and not c.oversize]
    if not len(pool) or not candidates:
        return []
    ink = np.array([cluster.bbox for cluster in candidates])
    gaps: list[SeriesGap] = []
    buckets: dict[tuple[int, int], list[int]] = defaultdict(list)
    for index in pool:
        buckets[(int(horizontal[index]), int(round(level[index] / SERIES_LEVEL_TOL)))].append(int(index))
    for (is_horizontal, _), members in sorted(buckets.items()):
        members.sort(key=lambda index: low[index])
        for first, second in zip(members, members[1:]):
            gap = float(low[second] - high[first])
            if gap <= 0 or gap > SERIES_GAP_MAX:
                continue
            start, stop = float(high[first]), float(low[second])
            line = float(level[first])
            if is_horizontal:
                corridor = (start, line - SERIES_CORRIDOR, stop, line + SERIES_CORRIDOR)
            else:
                corridor = (line - SERIES_CORRIDOR, start, line + SERIES_CORRIDOR, stop)
            hit = (
                (ink[:, 0] <= corridor[2]) & (ink[:, 2] >= corridor[0])
                & (ink[:, 1] <= corridor[3]) & (ink[:, 3] >= corridor[1])
            )
            if not bool(hit.any()):
                continue
            spans = ink[hit][:, [0, 2]] if is_horizontal else ink[hit][:, [1, 3]]
            covered = 0.0
            cursor = start
            for piece_low, piece_high in sorted(
                (max(float(a), start), min(float(b), stop)) for a, b in spans
            ):
                if piece_high <= cursor:
                    continue
                covered += piece_high - max(piece_low, cursor)
                cursor = max(cursor, piece_high)
            share = covered / max(gap, 1e-9)
            if share < SERIES_COVER:
                continue
            chosen = [cluster.index for cluster, keep in zip(candidates, hit) if keep]
            gaps.append(SeriesGap(
                axis_low=int(first), axis_high=int(second), gap=gap,
                covered=share, clusters=tuple(sorted(chosen)),
            ))
    return gaps


def prove_conductors(data: PageData) -> EdgeFacts:
    """Classify every stroke, then grow the proven conductor set from the dots."""
    nature, region, counters = classify_nature(data)
    edges, horizontal, low, high, level = _edge_arrays(data)
    total = len(nature)
    eligible = np.array([value not in FURNITURE for value in nature], dtype=bool)
    if not total:
        return EdgeFacts(nature=nature, region=region,
                         conductor=np.zeros(0, dtype=bool), conductor_evidence=[],
                         counters=counters)

    dot_junctions, dot_counters = _dot_junctions(data, eligible, low, high, level, horizontal)
    hop_junctions, hop_crossings, hop_counters = _hop_junctions(
        data, eligible, low, high, level, horizontal)
    contact_junctions, contact_counters = _contact_junctions(
        data, eligible, low, high, level, horizontal)
    dot_points = (
        np.array([junction.point for junction in dot_junctions])
        if dot_junctions else np.zeros((0, 2))
    )
    crossings, crossing_counters = _crossings(
        data, eligible, low, high, level, horizontal, dot_points)
    junctions = dot_junctions + hop_junctions + contact_junctions
    crossings = crossings + hop_crossings

    kinds = _region_kind_by_component(data)
    box_edge = np.array([kinds.get(int(region[index])) == "BOX" for index in range(total)])
    table_edge = np.array([kinds.get(int(region[index])) in {"TABLE", "STAMP"} for index in range(total)])

    length = high - low
    clusters = symbols_module.build_clusters(edges, eligible, length, data.strokes.slanted)
    terminals = _cluster_terminals(
        clusters, edges, eligible, low, high, level, horizontal, data.strokes.slanted)
    text_ink = _text_ink_clusters(data, clusters)
    series = _series_gaps(clusters, text_ink, eligible, box_edge, table_edge,
                          low, high, level, horizontal)

    adjacency: dict[int, set[int]] = defaultdict(set)
    for junction in junctions:
        for a in junction.edges:
            for b in junction.edges:
                if a != b:
                    adjacency[int(a)].add(int(b))

    conductor = np.zeros(total, dtype=bool)
    evidence: list[str | None] = [None] * total
    queue: deque[int] = deque()

    def admit(index: int, why: str) -> None:
        if conductor[index] or not eligible[index] or box_edge[index] or table_edge[index]:
            return
        conductor[index] = True
        evidence[index] = why
        queue.append(index)

    for junction in dot_junctions:
        for index in junction.edges:
            admit(int(index), JUNCTION_DOT)

    oversize = {cluster.index for cluster in clusters if cluster.oversize}
    bridging: set[int] = set()
    rounds = 0
    while queue and rounds < MAX_ROUNDS:
        rounds += 1
        while queue:
            current = queue.popleft()
            for neighbour in sorted(adjacency.get(current, ())):
                admit(neighbour, CONTINUOUS_POLYLINE
                      if evidence[current] == CONTINUOUS_POLYLINE else COINCIDENT_ENDPOINTS)
        for gap in series:
            if conductor[gap.axis_low] or conductor[gap.axis_high]:
                before = int(conductor.sum())
                admit(gap.axis_low, EQUIPMENT_PORT)
                admit(gap.axis_high, EQUIPMENT_PORT)
                if int(conductor.sum()) != before:
                    bridging.update(gap.clusters)
        for cluster_index, members in terminals.items():
            if cluster_index in oversize or cluster_index in text_ink \
                    or len(members) > MAX_CLUSTER_TERMINALS:
                continue
            if not any(conductor[index] for index in members):
                continue
            before = int(conductor.sum())
            for index in members:
                admit(index, EQUIPMENT_PORT)
            if int(conductor.sum()) != before:
                bridging.add(cluster_index)

    for index in range(total):
        if conductor[index]:
            nature[index] = SCHEMATIC_CONDUCTOR

    counters.update(dot_counters)
    counters.update(hop_counters)
    counters.update(contact_counters)
    counters.update(crossing_counters)
    counters["symbol_clusters"] = len(clusters)
    counters["symbol_clusters_text_ink"] = len(text_ink)
    counters["series_gaps"] = len(series)
    counters["symbol_clusters_oversize"] = len(oversize)
    counters["symbol_clusters_bridging"] = len(bridging)
    counters["proven_conductors"] = int(conductor.sum())
    counters["growth_rounds"] = rounds
    return EdgeFacts(
        nature=nature,
        region=region,
        conductor=conductor,
        conductor_evidence=evidence,
        junctions=junctions,
        crossings=crossings,
        clusters=clusters,
        cluster_terminals=terminals,
        text_ink_clusters=text_ink,
        series_gaps=series,
        bridging_clusters=bridging,
        adjacency={key: set(value) for key, value in adjacency.items()},
        counters=counters,
    )


__all__ = [
    "DOT_TOL", "END_TOL", "FRAME_MARGIN", "FRAME_SPAN", "FURNITURE", "HOP_TOL",
    "LEADER_MAX_LEN", "MAX_CLUSTER_TERMINALS", "MAX_ROUNDS", "TEE_CHUNK",
    "SERIES_CORRIDOR", "SERIES_COVER", "SERIES_GAP_MAX", "SERIES_LEVEL_TOL",
    "TICK_MAX_LEN", "TOUCH_TOL", "UNDERLINE_SLACK",
    "Crossing", "EdgeFacts", "Junction", "SeriesGap", "classify_nature",
    "prove_conductors",
]
