"""What the page actually draws, read once and kept with its attributes.

V1 needed boundaries, so it welded collinear axis-aligned strokes into maximal
edges and counted everything slanted as unstructured ink.  A topology needs
three things V1 threw away:

* **the attributes of a stroke** — colour and width.  They are not semantics
  and this module never reads them as semantics; they are identity.  A black
  phase bus and a cyan neutral bus drawn two points apart are two conductors,
  and welding that ignores colour fuses them into one.
* **the slanted strokes** — a breaker, an arrowhead, a transformer winding and
  a leader are all made of them.  Discarding them discards every symbol on the
  sheet.
* **the filled ink** — the junction dot, the one mark in a schematic whose
  entire purpose is to state that two conductors are connected.  AutoCAD does
  not export it as a disc: it exports a hairline circle plus several dozen
  scanline slivers.  Detecting "a small filled disc" therefore means clustering
  the slivers back into the blob they paint.

The module also keeps what it dropped.  A page whose fill count exceeds the
blob-clustering cap says so and reports no dots, rather than reporting few.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np

from .contract import PROVEN_DASHED, SOLID

#: Tolerance for calling a segment horizontal or vertical, in points.  V1's.
AXIS_EPS = 0.35
#: Coordinate grid a collinear chain is welded on, in points.  V1's.
EDGE_GRID = 0.5
#: Shortest welded piece kept at all, in points.
MIN_PIECE_LEN = 3.0
#: Shortest edge that may enter the graph, in points.  V1's boundary minimum.
MIN_EDGE_LEN = 6.0
#: Steps a cubic is flattened into when it has to become segments.
CURVE_STEPS = 4

#: Dash recovery.  A dashed conductor is a conductor, but the same chaining
#: fuses two unrelated collinear strokes, so it is allowed only where the
#: drawing is regular enough to prove that the gaps are a dash pattern.
DASH_GAP_MAX = 8.0
DASH_MIN_PIECES = 3
DASH_REGULARITY = 2.5
DASH_MIN_SPAN = 20.0

#: A filled blob is a junction dot candidate at these sizes.  The bounds are
#: the drawn ones: on this corpus the dot is 4.4–5.1 points across on sheets
#: whose text is 10 points, and both bounds are reported with a sensitivity
#: curve rather than presented as a tuned truth.
DOT_MIN = 2.0
DOT_MAX = 10.0
DOT_ASPECT = 1.35
#: Slivers of one blob touch; this is the slack when testing that.
BLOB_TOL = 0.35
#: Above this many filled paths a page reports no dots instead of few.
BLOB_PATH_CAP = 120_000

#: An arrowhead is filled ink that only widens.  Fewer scanlines than this is
#: not a shape, it is a stroke; the taper is how much wider the base must be
#: than the tip before the blob is called a triangle rather than a bar.
ARROW_MIN_SCANLINES = 4
ARROW_SLACK = 0.05
ARROW_TAPER = 2.5
#: Scanlines kept per blob.  A dot has sixteen; the cap only bites on a large
#: filled area, which is not a shape this package reads.
PROFILE_MAX = 64

#: A closed circle is four cubics with a square bounding box; a crossover hop
#: is the same construction cut in half, so its box is twice as wide as tall.
CIRCLE_ASPECT = 1.18
HOP_ASPECT_MIN = 1.55
HOP_ASPECT_MAX = 2.6
HOP_MAX_SIZE = 30.0


def _colour_key(colour: Any) -> str:
    if colour is None:
        return "none"
    try:
        return ",".join(f"{float(channel):.3f}" for channel in colour)
    except (TypeError, ValueError):
        return "none"


def _is_white(colour: Any) -> bool:
    if colour is None:
        return False
    try:
        return all(float(channel) >= 0.98 for channel in colour)
    except (TypeError, ValueError):
        return False


@dataclass
class Blob:
    """A patch of filled ink, reassembled from the slivers that paint it.

    ``profile`` keeps the scanline widths the exporter used to paint it, low
    edge first.  It is the only thing that distinguishes a disc from a triangle
    once the outline is gone: a disc widens and then narrows, an arrowhead
    widens monotonically to its base.  Keeping it costs a few dozen floats per
    blob and buys the arrowhead test, which is the only proof of direction this
    package accepts.
    """

    bbox: tuple[float, float, float, float]
    colour: str
    slivers: int
    profile: tuple[float, ...] = ()

    @property
    def width(self) -> float:
        return self.bbox[2] - self.bbox[0]

    @property
    def height(self) -> float:
        return self.bbox[3] - self.bbox[1]

    @property
    def centre(self) -> tuple[float, float]:
        return ((self.bbox[0] + self.bbox[2]) / 2.0, (self.bbox[1] + self.bbox[3]) / 2.0)

    @property
    def is_dot_shaped(self) -> bool:
        width, height = self.width, self.height
        if not (DOT_MIN <= width <= DOT_MAX and DOT_MIN <= height <= DOT_MAX):
            return False
        aspect = width / max(height, 1e-9)
        return 1.0 / DOT_ASPECT <= aspect <= DOT_ASPECT

    def widens_monotonically(self) -> bool:
        """True when the painted ink only ever gets wider — an arrowhead.

        Reported in both scan directions, because the exporter paints from
        whichever side the shape starts on.
        """
        if len(self.profile) < ARROW_MIN_SCANLINES:
            return False
        values = np.asarray(self.profile, dtype=np.float64)
        forward = bool(np.all(np.diff(values) >= -ARROW_SLACK))
        backward = bool(np.all(np.diff(values[::-1]) >= -ARROW_SLACK))
        if not (forward or backward):
            return False
        return values.max() >= ARROW_TAPER * max(values.min(), 1e-9)


@dataclass
class Arc:
    """A curved path: a closed circle, a crossover hop, or neither."""

    bbox: tuple[float, float, float, float]
    curves: int
    lines: int
    colour: str
    closed_circle: bool
    hop: bool

    @property
    def centre(self) -> tuple[float, float]:
        return ((self.bbox[0] + self.bbox[2]) / 2.0, (self.bbox[1] + self.bbox[3]) / 2.0)


@dataclass
class PageStrokes:
    """Everything one page draws, in displayed space, with its attributes."""

    page: int
    rotation: int
    width: float
    height: float
    #: welded axis edges as ``(x0, y0, x1, y1)``; horizontal first, then vertical
    edges: np.ndarray = field(default_factory=lambda: np.zeros((0, 4)))
    edge_axis: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int8))
    edge_colour: list[str] = field(default_factory=list)
    edge_pattern: list[str] = field(default_factory=list)
    edge_pieces: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int32))
    slanted: np.ndarray = field(default_factory=lambda: np.zeros((0, 4)))
    blobs: list[Blob] = field(default_factory=list)
    arcs: list[Arc] = field(default_factory=list)
    rectangles: list[list[float]] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)

    @property
    def horizontal_mask(self) -> np.ndarray:
        return self.edge_axis == 0

    def edge_length(self) -> np.ndarray:
        if not len(self.edges):
            return np.zeros(0)
        return np.hypot(self.edges[:, 2] - self.edges[:, 0], self.edges[:, 3] - self.edges[:, 1])

    def geometry_ref(self, index: int) -> str:
        axis = "h" if self.edge_axis[index] == 0 else "v"
        return f"g:p{self.page:04d}:{axis}{int(index):05d}"

    def compaction(self) -> dict[str, Any]:
        raw = int(self.counters.get("raw_segments", 0))
        kept = int(len(self.edges)) + int(len(self.slanted))
        return {
            "raw_segments": raw,
            "raw_paths": int(self.counters.get("paths", 0)),
            "welded_edges": int(len(self.edges)),
            "slanted_strokes": int(len(self.slanted)),
            "filled_blobs": len(self.blobs),
            "arcs": len(self.arcs),
            "compression": round(raw / kept, 2) if kept else None,
        }


# ---------------------------------------------------------------------------
# welding
# ---------------------------------------------------------------------------


def _weld_group(
    selected: np.ndarray, along: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Weld one colour group of one orientation into maximal pieces.

    Returns ``(level, low, high)`` plus the number of source strokes welded into
    each piece, so a later reader can tell a single long stroke from a chain.
    """
    across = 1 - along
    level = np.round(((selected[:, across] + selected[:, across + 2]) / 2.0) / EDGE_GRID) * EDGE_GRID
    low = np.minimum(selected[:, along], selected[:, along + 2])
    high = np.maximum(selected[:, along], selected[:, along + 2])
    order = np.lexsort((low, level))
    level, low, high = level[order], low[order], high[order]
    pieces: list[tuple[float, float, float, int]] = []
    index = 0
    total = len(level)
    while index < total:
        current = level[index]
        start = low[index]
        end = high[index]
        count = 1
        index += 1
        while index < total and level[index] == current and low[index] <= end + EDGE_GRID:
            end = max(end, high[index])
            count += 1
            index += 1
        if end - start >= MIN_PIECE_LEN:
            pieces.append((current, start, end, count))
    if not pieces:
        return np.zeros((0, 3)), np.zeros(0, dtype=np.int32), np.zeros(0, dtype=bool)
    array = np.asarray([(p[0], p[1], p[2]) for p in pieces], dtype=np.float64)
    counts = np.asarray([p[3] for p in pieces], dtype=np.int32)
    return array, counts, np.zeros(len(array), dtype=bool)


def _chain_dashes(
    pieces: np.ndarray, counts: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Recover dashed conductors, and only where the dashing is provable.

    A run of collinear pieces becomes one edge when there are at least three of
    them, the gaps are all small, and both the gaps and the piece lengths are
    regular within a factor.  Irregular collinear pieces are left alone: they
    are two conductors that happen to line up, and joining them would invent a
    connection out of a coincidence.
    """
    if not len(pieces):
        return pieces, counts, np.zeros(0, dtype=bool)
    keep_level: list[float] = []
    keep_low: list[float] = []
    keep_high: list[float] = []
    keep_count: list[int] = []
    keep_dashed: list[bool] = []
    index = 0
    total = len(pieces)
    while index < total:
        level = pieces[index, 0]
        run = [index]
        cursor = index + 1
        while (
            cursor < total
            and pieces[cursor, 0] == level
            and pieces[cursor, 1] - pieces[cursor - 1, 2] <= DASH_GAP_MAX
            and pieces[cursor, 1] - pieces[cursor - 1, 2] > 0
        ):
            run.append(cursor)
            cursor += 1
        if len(run) >= DASH_MIN_PIECES:
            gaps = np.array([pieces[run[k + 1], 1] - pieces[run[k], 2] for k in range(len(run) - 1)])
            lengths = np.array([pieces[k, 2] - pieces[k, 1] for k in run])
            span = pieces[run[-1], 2] - pieces[run[0], 1]
            regular = (
                gaps.min() > 0
                and gaps.max() / max(gaps.min(), 1e-9) <= DASH_REGULARITY
                and lengths.max() / max(lengths.min(), 1e-9) <= DASH_REGULARITY
                and span >= DASH_MIN_SPAN
            )
            if regular:
                keep_level.append(level)
                keep_low.append(pieces[run[0], 1])
                keep_high.append(pieces[run[-1], 2])
                keep_count.append(int(counts[run].sum()))
                keep_dashed.append(True)
                index = cursor
                continue
        keep_level.append(pieces[index, 0])
        keep_low.append(pieces[index, 1])
        keep_high.append(pieces[index, 2])
        keep_count.append(int(counts[index]))
        keep_dashed.append(False)
        index += 1
    out = np.column_stack([
        np.asarray(keep_level), np.asarray(keep_low), np.asarray(keep_high)
    ])
    return out, np.asarray(keep_count, dtype=np.int32), np.asarray(keep_dashed, dtype=bool)


def weld(
    segments: np.ndarray, colours: Sequence[str]
) -> tuple[np.ndarray, np.ndarray, list[str], list[str], np.ndarray, np.ndarray]:
    """Weld axis-aligned strokes into edges, one colour group at a time.

    Fill ink never reaches this function.  The interior of a painted disc is
    exported as several dozen hairline slivers whose hypotenuses are slanted
    strokes; left in, they drown the symbol pool at forty thousand strokes a
    page and weld into nothing.  They are separated at read time, by the one
    property that identifies them without guessing — the path fills and does
    not stroke.
    """
    if not len(segments):
        empty = np.zeros((0, 4))
        return empty, np.zeros(0, dtype=np.int8), [], [], np.zeros(0, dtype=np.int32), np.zeros((0, 4))
    colour_array = np.asarray(colours)
    dx = segments[:, 2] - segments[:, 0]
    dy = segments[:, 3] - segments[:, 1]
    horizontal = (np.abs(dy) <= AXIS_EPS) & (np.abs(dx) > AXIS_EPS)
    vertical = (np.abs(dx) <= AXIS_EPS) & (np.abs(dy) > AXIS_EPS)
    slanted = segments[~(horizontal | vertical)]
    rows: list[np.ndarray] = []
    axis_out: list[int] = []
    colour_out: list[str] = []
    pattern_out: list[str] = []
    count_out: list[int] = []
    for along, mask in ((0, horizontal), (1, vertical)):
        for colour in sorted(set(colour_array[mask].tolist())):
            selected = segments[mask & (colour_array == colour)]
            if not len(selected):
                continue
            pieces, counts, _ = _weld_group(selected, along)
            pieces, counts, dashed = _chain_dashes(pieces, counts)
            if not len(pieces):
                continue
            length = pieces[:, 2] - pieces[:, 1]
            keep = length >= MIN_EDGE_LEN
            pieces, counts, dashed = pieces[keep], counts[keep], dashed[keep]
            for row, count, is_dashed in zip(pieces, counts, dashed):
                if along == 0:
                    rows.append(np.array([row[1], row[0], row[2], row[0]]))
                else:
                    rows.append(np.array([row[0], row[1], row[0], row[2]]))
                axis_out.append(along)
                colour_out.append(colour)
                pattern_out.append(PROVEN_DASHED if is_dashed else SOLID)
                count_out.append(int(count))
    if not rows:
        return (
            np.zeros((0, 4)), np.zeros(0, dtype=np.int8), [], [],
            np.zeros(0, dtype=np.int32), slanted,
        )
    edges = np.vstack(rows)
    axis = np.asarray(axis_out, dtype=np.int8)
    order = np.lexsort((edges[:, 0], edges[:, 1], axis))
    return (
        edges[order],
        axis[order],
        [colour_out[position] for position in order],
        [pattern_out[position] for position in order],
        np.asarray(count_out, dtype=np.int32)[order],
        slanted,
    )


# ---------------------------------------------------------------------------
# filled ink
# ---------------------------------------------------------------------------


def cluster_blobs(rects: np.ndarray, colours: Sequence[str]) -> list[Blob]:
    """Reassemble filled slivers into the blobs they paint."""
    if not len(rects):
        return []
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components
    from scipy.spatial import cKDTree

    centres = np.column_stack([(rects[:, 0] + rects[:, 2]) / 2.0, (rects[:, 1] + rects[:, 3]) / 2.0])
    tree = cKDTree(centres)
    pairs = tree.query_pairs(r=DOT_MAX, output_type="ndarray")
    if len(pairs):
        left, right = pairs[:, 0], pairs[:, 1]
        touching = (
            (rects[left, 0] <= rects[right, 2] + BLOB_TOL)
            & (rects[right, 0] <= rects[left, 2] + BLOB_TOL)
            & (rects[left, 1] <= rects[right, 3] + BLOB_TOL)
            & (rects[right, 1] <= rects[left, 3] + BLOB_TOL)
        )
        colour_array = np.asarray(colours)
        same = colour_array[left] == colour_array[right]
        keep = touching & same
        left, right = left[keep], right[keep]
    else:
        left = right = np.zeros(0, dtype=int)
    graph = coo_matrix(
        (np.ones(len(left)), (left, right)), shape=(len(rects), len(rects))
    )
    count, labels = connected_components(graph, directed=False)
    blobs: list[Blob] = []
    colour_array = np.asarray(colours)
    for component in range(count):
        member = labels == component
        rows = rects[member]
        order = np.lexsort((rows[:, 0], rows[:, 1]))
        widths = (rows[order, 2] - rows[order, 0])[:PROFILE_MAX]
        blobs.append(Blob(
            bbox=(
                float(rows[:, 0].min()), float(rows[:, 1].min()),
                float(rows[:, 2].max()), float(rows[:, 3].max()),
            ),
            colour=str(colour_array[member][0]),
            slivers=int(member.sum()),
            profile=tuple(round(float(value), 3) for value in widths),
        ))
    blobs.sort(key=lambda blob: (round(blob.bbox[1], 2), round(blob.bbox[0], 2)))
    return blobs


# ---------------------------------------------------------------------------
# the page
# ---------------------------------------------------------------------------


def read_page(pdf_path: str, page_index: int) -> PageStrokes:
    """Read one page's drawn material into displayed space."""
    import fitz

    document = fitz.open(str(pdf_path))
    try:
        page = document[page_index]
        matrix = page.rotation_matrix
        rect = fitz.Rect(page.rect)
        segments: list[tuple[float, float, float, float]] = []
        colours: list[str] = []
        fill_rects: list[list[float]] = []
        fill_colours: list[str] = []
        rectangles: list[list[float]] = []
        arcs: list[Arc] = []
        counters = {
            "paths": 0, "paths_white_dropped": 0, "raw_segments": 0,
            "items_line": 0, "items_rect": 0, "items_quad": 0, "items_curve": 0,
            "fill_paths": 0, "blob_clustering_skipped": 0,
            "fill_ink_paths": 0, "fill_ink_items": 0, "hop_paths_withheld": 0,
        }
        for drawing in page.get_drawings():
            counters["paths"] += 1
            stroke = drawing.get("color")
            fill = drawing.get("fill")
            if _is_white(stroke) and (fill is None or _is_white(fill)):
                counters["paths_white_dropped"] += 1
                continue
            colour = _colour_key(stroke if stroke is not None else fill)
            line_width = float(drawing.get("width") or 0.0)
            fill_only = fill is not None and (stroke is None or line_width <= 0.0)
            curves = sum(1 for item in drawing["items"] if item[0] == "c")
            box = fitz.Rect(drawing["rect"]) * matrix
            box.normalize()
            if fill is not None:
                counters["fill_paths"] += 1
                fill_rects.append([box.x0, box.y0, box.x1, box.y1])
                fill_colours.append(_colour_key(fill))
            if fill_only:
                counters["fill_ink_paths"] += 1
                counters["fill_ink_items"] += len(drawing["items"])
                continue
            is_hop = False
            if curves:
                lines = sum(1 for item in drawing["items"] if item[0] == "l")
                aspect = box.width / max(box.height, 1e-9)
                size = max(box.width, box.height)
                is_hop = (
                    curves in (1, 2) and lines == 0 and size <= HOP_MAX_SIZE
                    and (
                        HOP_ASPECT_MIN <= aspect <= HOP_ASPECT_MAX
                        or HOP_ASPECT_MIN <= 1.0 / max(aspect, 1e-9) <= HOP_ASPECT_MAX
                    )
                )
                arcs.append(Arc(
                    bbox=(box.x0, box.y0, box.x1, box.y1),
                    curves=curves,
                    lines=lines,
                    colour=colour,
                    closed_circle=(
                        curves == 4 and lines == 0 and size <= HOP_MAX_SIZE
                        and 1.0 / CIRCLE_ASPECT <= aspect <= CIRCLE_ASPECT
                    ),
                    hop=is_hop,
                ))
            if is_hop:
                # The crossover hop is the drawing's statement that two wires do
                # *not* touch.  Its flattened arc is slanted ink, and slanted ink
                # is what symbol clusters are made of — so left in the pool, the
                # one mark that means "not connected" becomes the strongest
                # connector on the sheet.  Measured before this line existed: on
                # the control page it welded twelve independent feeders into one
                # run.  The hop is read once, by the rule that knows what it is.
                counters["hop_paths_withheld"] += 1
                continue
            for item in drawing["items"]:
                op = item[0]
                if op == "l":
                    start = fitz.Point(item[1]) * matrix
                    end = fitz.Point(item[2]) * matrix
                    segments.append((start.x, start.y, end.x, end.y))
                    colours.append(colour)
                    counters["items_line"] += 1
                elif op == "re":
                    box2 = fitz.Rect(item[1]) * matrix
                    box2.normalize()
                    rectangles.append([box2.x0, box2.y0, box2.x1, box2.y1])
                    for start, end in (
                        ((box2.x0, box2.y0), (box2.x1, box2.y0)),
                        ((box2.x1, box2.y0), (box2.x1, box2.y1)),
                        ((box2.x1, box2.y1), (box2.x0, box2.y1)),
                        ((box2.x0, box2.y1), (box2.x0, box2.y0)),
                    ):
                        segments.append((start[0], start[1], end[0], end[1]))
                        colours.append(colour)
                    counters["items_rect"] += 1
                elif op == "qu":
                    quad = item[1]
                    points = [
                        fitz.Point(point) * matrix
                        for point in (quad.ul, quad.ur, quad.lr, quad.ll)
                    ]
                    for start, end in zip(points, points[1:] + points[:1]):
                        segments.append((start.x, start.y, end.x, end.y))
                        colours.append(colour)
                    counters["items_quad"] += 1
                elif op == "c":
                    control = [fitz.Point(point) * matrix for point in item[1:5]]
                    previous = control[0]
                    for step in range(1, CURVE_STEPS + 1):
                        t = step / CURVE_STEPS
                        x = ((1 - t) ** 3 * control[0].x + 3 * (1 - t) ** 2 * t * control[1].x
                             + 3 * (1 - t) * t * t * control[2].x + t ** 3 * control[3].x)
                        y = ((1 - t) ** 3 * control[0].y + 3 * (1 - t) ** 2 * t * control[1].y
                             + 3 * (1 - t) * t * t * control[2].y + t ** 3 * control[3].y)
                        segments.append((previous.x, previous.y, x, y))
                        colours.append(colour)
                        previous = fitz.Point(x, y)
                    counters["items_curve"] += 1
        counters["raw_segments"] = len(segments)
        array = np.asarray(segments, dtype=np.float64) if segments else np.zeros((0, 4))
        edges, axis, edge_colour, edge_pattern, pieces, slanted = weld(array, colours)
        if counters["fill_paths"] > BLOB_PATH_CAP:
            counters["blob_clustering_skipped"] = 1
            blobs: list[Blob] = []
        else:
            blobs = cluster_blobs(
                np.asarray(fill_rects, dtype=np.float64) if fill_rects else np.zeros((0, 4)),
                fill_colours,
            )
        return PageStrokes(
            page=page_index + 1,
            rotation=int(page.rotation),
            width=float(rect.width),
            height=float(rect.height),
            edges=edges,
            edge_axis=axis,
            edge_colour=edge_colour,
            edge_pattern=edge_pattern,
            edge_pieces=pieces,
            slanted=slanted,
            blobs=blobs,
            arcs=arcs,
            rectangles=rectangles,
            counters=counters,
        )
    finally:
        document.close()


__all__ = [
    "AXIS_EPS", "BLOB_PATH_CAP", "BLOB_TOL", "CIRCLE_ASPECT", "CURVE_STEPS",
    "DASH_GAP_MAX", "DASH_MIN_PIECES", "DASH_MIN_SPAN", "DASH_REGULARITY",
    "DOT_ASPECT", "DOT_MAX", "DOT_MIN", "EDGE_GRID", "HOP_ASPECT_MAX",
    "HOP_ASPECT_MIN", "HOP_MAX_SIZE", "MIN_EDGE_LEN", "MIN_PIECE_LEN",
    "ARROW_MIN_SCANLINES", "ARROW_SLACK", "ARROW_TAPER", "PROFILE_MAX",
    "Arc", "Blob", "PageStrokes", "cluster_blobs", "read_page", "weld",
]
