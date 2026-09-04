"""Deterministic page geometry for the Function Lineage v3.0 feasibility audit.

Research only.  Nothing here is wired into production; the module reads the
source PDF of a comparison pair and returns what the page actually contains,
in the coordinate space the reader sees.

Three deliberate choices:

1. **Displayed space.**  Every rectangle is multiplied by ``page.rotation_matrix``
   before it is reported, so a sheet with ``/Rotate 270`` is measured the way it
   is read.  ``page.rect`` is already the displayed rectangle; the raw text and
   drawing coordinates are not.  Mixing the two reads a rotated sheet's stamp as
   if it sat in the middle of the page.
2. **CAD text is repaired, never invented.**  AutoCAD's ISOCPEUR subset maps
   Cyrillic onto U+0180..U+024F with a constant shift.  The shift is applied and
   *recorded*; a repaired span is only ever allowed to carry a fact when the same
   string is independently confirmed by the recognized Markdown of the same page.
3. **Both sides of every extraction are measured.**  A channel reports what it
   found *and* what it dropped, because a metric of the form "what we found is
   correct" cannot notice that half the page was never found.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

SCHEMA_VERSION = "function-region-page-geometry.v3.0"

#: AutoCAD ISOCPEUR/ISOCPEURItalic subsets encode Cyrillic in this block.
CAD_BLOCK = (0x0180, 0x024F)
#: Constant offset from the CAD block to Cyrillic, verified on the corpus.
CAD_SHIFT = 581
#: Control codes in the same subsets are ASCII shifted by this much.
CAD_CONTROL_SHIFT = 31
#: The shift is only ever applied when it lands in Cyrillic.  A stray glyph in
#: an ordinary font — ``Ʃ`` in ArialMT — lands in Greek or Coptic instead and is
#: left exactly as it was: a repair that has to guess is not a repair.
CYRILLIC_BLOCK = (0x0400, 0x045F)

#: Stamp cell of the displayed page — the production bound from
#: ``sheet_identity.STAMP_ZONE_MIN_Y0`` / ``STAMP_ZONE_MIN_X1``.
STAMP_ZONE_MIN_Y0 = 0.85
STAMP_ZONE_MIN_X1 = 0.55

#: Axis tolerance for calling a segment horizontal or vertical, in points.
AXIS_EPS = 0.35
#: Coordinate grid a collinear chain is welded on, in points.
EDGE_GRID = 0.5
#: Shortest edge that is allowed to be a structural boundary, in points.
MIN_EDGE_LEN = 6.0
#: Tolerance for calling two edges incident, in points.
INCIDENCE_TOL = 1.0
#: Steps a cubic is flattened into.  Curves are boundaries here, not shapes.
CURVE_STEPS = 4


def _is_cad_encoded(value: str) -> bool:
    """Does this span carry the AutoCAD subset encoding at all?

    The test is the shift's own result: a character of the block that becomes a
    Cyrillic letter.  Nothing else in the span is touched unless this holds, so
    a lone stray glyph in an ordinary font can never trigger a rewrite.
    """
    return any(
        CAD_BLOCK[0] <= ord(char) <= CAD_BLOCK[1]
        and CYRILLIC_BLOCK[0] <= ord(char) + CAD_SHIFT <= CYRILLIC_BLOCK[1]
        for char in value
    )


def repair_cad_text(value: str) -> tuple[str, bool]:
    """Return ``(text, repaired)``.  A span outside the CAD subset is untouched."""
    if not value or not _is_cad_encoded(value):
        return value, False
    out: list[str] = []
    for char in value:
        code = ord(char)
        shifted = code + CAD_SHIFT
        if CAD_BLOCK[0] <= code <= CAD_BLOCK[1] and CYRILLIC_BLOCK[0] <= shifted <= CYRILLIC_BLOCK[1]:
            out.append(chr(shifted))
        elif code < 0x20:
            out.append(chr(code + CAD_CONTROL_SHIFT))
        else:
            out.append(char)
    return "".join(out), True


def _is_white(color: Any) -> bool:
    if color is None:
        return False
    try:
        return all(float(channel) >= 0.98 for channel in color)
    except (TypeError, ValueError):
        return False


@dataclass
class PageGeometry:
    """Everything the page carries, in displayed coordinates."""

    page: int
    rotation: int
    width: float
    height: float
    spans: list[dict[str, Any]] = field(default_factory=list)
    text_blocks: list[dict[str, Any]] = field(default_factory=list)
    images: list[list[float]] = field(default_factory=list)
    annotations: list[dict[str, Any]] = field(default_factory=list)
    links: int = 0
    segments: np.ndarray = field(default_factory=lambda: np.zeros((0, 4)))
    rectangles: list[list[float]] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)

    def in_stamp_zone(self, bbox: Sequence[float]) -> bool:
        if not self.width or not self.height:
            return False
        return (
            float(bbox[1]) / self.height >= STAMP_ZONE_MIN_Y0
            and float(bbox[2]) / self.width >= STAMP_ZONE_MIN_X1
        )


def _span_rows(page: Any, matrix: Any) -> tuple[list[dict[str, Any]], dict[str, int]]:
    import fitz

    spans: list[dict[str, Any]] = []
    counters = {"spans": 0, "spans_repaired": 0, "spans_blank": 0}
    for block in page.get_text("dict")["blocks"]:
        if block["type"] != 0:
            continue
        for line in block["lines"]:
            for span in line["spans"]:
                text, repaired = repair_cad_text(span["text"])
                if not text.strip():
                    counters["spans_blank"] += 1
                    continue
                rect = fitz.Rect(span["bbox"]) * matrix
                rect.normalize()
                counters["spans"] += 1
                counters["spans_repaired"] += int(repaired)
                spans.append({
                    "text": text,
                    "bbox": [rect.x0, rect.y0, rect.x1, rect.y1],
                    "size": round(float(span["size"]), 2),
                    "font": str(span["font"]),
                    "vertical": abs(float(line["dir"][1])) > abs(float(line["dir"][0])),
                    "repaired": repaired,
                })
    return spans, counters


def _text_block_rows(page: Any, matrix: Any) -> list[dict[str, Any]]:
    import fitz

    rows: list[dict[str, Any]] = []
    for block in page.get_text("blocks"):
        text = str(block[4]).strip()
        if not text:
            continue
        rect = fitz.Rect(block[:4]) * matrix
        rect.normalize()
        rows.append({
            "text": repair_cad_text(text)[0],
            "bbox": [rect.x0, rect.y0, rect.x1, rect.y1],
        })
    return rows


def _drawing_rows(page: Any, matrix: Any) -> tuple[np.ndarray, list[list[float]], dict[str, int]]:
    import fitz

    segments: list[tuple[float, float, float, float]] = []
    rectangles: list[list[float]] = []
    counters = {
        "paths": 0, "paths_white_dropped": 0, "items": 0,
        "items_line": 0, "items_rect": 0, "items_quad": 0, "items_curve": 0,
    }
    for drawing in page.get_drawings():
        counters["paths"] += 1
        stroke = drawing.get("color")
        fill = drawing.get("fill")
        if _is_white(stroke) and (fill is None or _is_white(fill)):
            counters["paths_white_dropped"] += 1
            continue
        for item in drawing["items"]:
            counters["items"] += 1
            op = item[0]
            if op == "l":
                p0 = fitz.Point(item[1]) * matrix
                p1 = fitz.Point(item[2]) * matrix
                segments.append((p0.x, p0.y, p1.x, p1.y))
                counters["items_line"] += 1
            elif op == "re":
                rect = fitz.Rect(item[1]) * matrix
                rect.normalize()
                rectangles.append([rect.x0, rect.y0, rect.x1, rect.y1])
                segments.extend([
                    (rect.x0, rect.y0, rect.x1, rect.y0),
                    (rect.x1, rect.y0, rect.x1, rect.y1),
                    (rect.x1, rect.y1, rect.x0, rect.y1),
                    (rect.x0, rect.y1, rect.x0, rect.y0),
                ])
                counters["items_rect"] += 1
            elif op == "qu":
                quad = item[1]
                points = [fitz.Point(p) * matrix for p in (quad.ul, quad.ur, quad.lr, quad.ll)]
                for a, b in zip(points, points[1:] + points[:1]):
                    segments.append((a.x, a.y, b.x, b.y))
                counters["items_quad"] += 1
            elif op == "c":
                control = [fitz.Point(p) * matrix for p in item[1:5]]
                previous = control[0]
                for step in range(1, CURVE_STEPS + 1):
                    t = step / CURVE_STEPS
                    x = ((1 - t) ** 3 * control[0].x + 3 * (1 - t) ** 2 * t * control[1].x
                         + 3 * (1 - t) * t * t * control[2].x + t ** 3 * control[3].x)
                    y = ((1 - t) ** 3 * control[0].y + 3 * (1 - t) ** 2 * t * control[1].y
                         + 3 * (1 - t) * t * t * control[2].y + t ** 3 * control[3].y)
                    segments.append((previous.x, previous.y, x, y))
                    previous = fitz.Point(x, y)
                counters["items_curve"] += 1
    array = np.asarray(segments, dtype=np.float64) if segments else np.zeros((0, 4))
    return array, rectangles, counters


def read_page(pdf_path: str, page_index: int) -> PageGeometry:
    """Read one page.  The document is opened and closed by this call."""
    import fitz

    document = fitz.open(pdf_path)
    try:
        page = document[page_index]
        matrix = page.rotation_matrix
        rect = fitz.Rect(page.rect)
        spans, counters = _span_rows(page, matrix)
        blocks = _text_block_rows(page, matrix)
        segments, rectangles, draw_counters = _drawing_rows(page, matrix)
        images = []
        for block in page.get_text("dict")["blocks"]:
            if block["type"] == 0:
                continue
            image_rect = fitz.Rect(block["bbox"]) * matrix
            image_rect.normalize()
            images.append([image_rect.x0, image_rect.y0, image_rect.x1, image_rect.y1])
        annotations = []
        for annotation in page.annots() or []:
            annot_rect = fitz.Rect(annotation.rect) * matrix
            annot_rect.normalize()
            annotations.append({
                "type": str(annotation.type[1]),
                "bbox": [annot_rect.x0, annot_rect.y0, annot_rect.x1, annot_rect.y1],
            })
        geometry = PageGeometry(
            page=page_index + 1,
            rotation=int(page.rotation),
            width=float(rect.width),
            height=float(rect.height),
            spans=spans,
            text_blocks=blocks,
            images=images,
            annotations=annotations,
            links=len(page.get_links()),
            segments=segments,
            rectangles=rectangles,
            counters={**counters, **draw_counters},
        )
    finally:
        document.close()
    return geometry


def axis_edges(
    segments: np.ndarray,
    *,
    eps: float = AXIS_EPS,
    grid: float = EDGE_GRID,
    min_len: float = MIN_EDGE_LEN,
) -> tuple[np.ndarray, np.ndarray]:
    """Maximal collinear axis-aligned edges as ``(x0, y0, x1, y1)`` rows.

    A drawn boundary is a chain of collinear strokes, not one stroke: welding
    them is what turns 372 941 segments into a few hundred boundaries.  Slanted
    strokes are never welded — they are counted as unstructured ink instead.
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
            current_level = level[index]
            start = low[index]
            end = high[index]
            index += 1
            while index < total and level[index] == current_level and low[index] <= end + grid:
                end = max(end, high[index])
                index += 1
            if end - start >= min_len:
                chains.append((current_level, start, end))
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

    Touching is an explicit drawn relation: two boundaries that cross.  It is
    not distance, so nothing here is proximity.
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
    graph = coo_matrix(
        (np.ones(len(rows)), (rows, cols)), shape=(total, total)
    )
    count, labels = connected_components(graph, directed=False)
    return labels, count


def unstructured_ink_share(segments: np.ndarray, *, eps: float = AXIS_EPS) -> float:
    """Share of stroke length that is neither horizontal nor vertical.

    The recall half of the edge extraction: a page whose ink is mostly slanted
    is a page whose boundaries this module cannot see.
    """
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
