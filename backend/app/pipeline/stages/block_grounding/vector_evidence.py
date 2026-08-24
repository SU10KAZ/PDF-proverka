"""Common PDF-vector evidence for block-grounding profiles.

This module owns only page selection, coordinate normalization and clipping.  It
does not assign electrical (or any other discipline-specific) meaning to the
extracted primitives.  PDF text and drawing coordinates are converted from the
page data space to PyMuPDF's visual page space exactly once, before the prepared
block polygon / bbox is applied.
"""
from __future__ import annotations

import functools
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional


EXTRACTION_VERSION = "vector-evidence-v1"


@dataclass
class VectorEvidence:
    """Discipline-neutral vector evidence in visual page coordinates.

    Failed extraction is represented by the same object with ``extraction_ok``
    false and explicit ``reasons``.  Callers therefore do not have to infer a
    cause from a silent ``None``.
    """

    page_index: Optional[int] = None
    visual_words: list = field(default_factory=list)
    drawings: list[dict] = field(default_factory=list)
    paths: list[dict] = field(default_factory=list)
    lines: list[list[float]] = field(default_factory=list)
    curves: list[list[float]] = field(default_factory=list)
    polygons: list[list[list[float]]] = field(default_factory=list)
    block_polygon: list[list[float]] = field(default_factory=list)
    block_bbox: Optional[list[float]] = None
    page_size: Optional[list[float]] = None
    coordinate_system: str = "visual"
    extraction_version: str = EXTRACTION_VERSION
    extraction_gate: dict = field(default_factory=dict)
    provenance: dict = field(default_factory=dict)

    @property
    def extraction_ok(self) -> bool:
        return bool(self.extraction_gate.get("extraction_ok"))

    @property
    def reasons(self) -> list[str]:
        return list(self.extraction_gate.get("reasons") or [])


@functools.lru_cache(maxsize=1)
def _catalog_clip_margin() -> float:
    try:
        from backend.app.pipeline.stages.block_context.reference_catalog import (
            load_reference_rules,
        )

        scope = load_reference_rules().get("text_scope") or {}
        return max(0.0, float(scope.get("outside_margin", 0.0)))
    except (OSError, RuntimeError, TypeError, ValueError):
        return 0.0


def _clip_words_to_bbox(words, bbox_norm, page_w, page_h, *, margin=None):
    """Keep words whose visual-space centre is inside a normalized bbox.

    Invalid / absent prepared geometry fails closed: returning the whole page
    would leak neighbouring blocks into downstream geometry.
    """
    if not bbox_norm or len(bbox_norm) < 4 or not page_w or not page_h:
        return []
    try:
        x0, y0, x1, y1 = (
            float(bbox_norm[0]),
            float(bbox_norm[1]),
            float(bbox_norm[2]),
            float(bbox_norm[3]),
        )
    except (TypeError, ValueError):
        return []
    if not (x1 > x0 and y1 > y0):
        return []
    try:
        margin = (
            _catalog_clip_margin()
            if margin is None
            else max(0.0, float(margin or 0.0))
        )
    except (TypeError, ValueError):
        return []
    x0 -= margin
    y0 -= margin
    x1 += margin
    y1 += margin
    kept = []
    for word in words:
        try:
            cx = ((float(word[0]) + float(word[2])) / 2.0) / page_w
            cy = ((float(word[1]) + float(word[3])) / 2.0) / page_h
        except (TypeError, ValueError, ZeroDivisionError, IndexError):
            continue
        if x0 <= cx <= x1 and y0 <= cy <= y1:
            kept.append(word)
    return kept


def _point_in_polygon(x, y, poly) -> bool:
    """Even-odd test for a point and a non-self-intersecting polygon."""
    n = len(poly)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = poly[i]
        xj, yj = poly[j]
        if ((yi > y) != (yj > y)) and (
            x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi
        ):
            inside = not inside
        j = i
    return inside


def _clip_words_to_polygon(words, polygon_norm, page_w, page_h):
    """Keep words whose visual-space centre is inside a normalized polygon."""
    if not polygon_norm or len(polygon_norm) < 3 or not page_w or not page_h:
        return []
    try:
        poly = [
            (float(point[0]), float(point[1]))
            for point in polygon_norm
            if len(point) >= 2
        ]
    except (TypeError, ValueError, IndexError):
        return []
    if len(poly) < 3:
        return []
    kept = []
    for word in words:
        try:
            cx = ((float(word[0]) + float(word[2])) / 2.0) / page_w
            cy = ((float(word[1]) + float(word[3])) / 2.0) / page_h
        except (TypeError, ValueError, ZeroDivisionError, IndexError):
            continue
        if _point_in_polygon(cx, cy, poly):
            kept.append(word)
    return kept


def _convex_hull(points):
    """Monotone-chain convex hull used by geometry profiles."""
    pts = sorted(set((round(x, 2), round(y, 2)) for x, y in points))
    if len(pts) <= 2:
        return pts

    def cross(origin, a, b):
        return (a[0] - origin[0]) * (b[1] - origin[1]) - (
            a[1] - origin[1]
        ) * (b[0] - origin[0])

    lower = []
    for point in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], point) <= 0:
            lower.pop()
        lower.append(point)
    upper = []
    for point in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], point) <= 0:
            upper.pop()
        upper.append(point)
    return lower[:-1] + upper[:-1]


def _median(values):
    ordered = sorted(values)
    count = len(ordered)
    if count == 0:
        return 0.0
    if count % 2:
        return ordered[count // 2]
    return (ordered[count // 2 - 1] + ordered[count // 2]) / 2


def _near(qx, qy, values, dx, dymin, dymax):
    candidates = sorted(
        (abs(x - qx), y, text)
        for x, y, text in values
        if abs(x - qx) < dx and dymin < (y - qy) < dymax
    )
    return candidates[0][2] if candidates else None


def _near_xy(qx, qy, values, dx, dymin, dymax):
    """Like :func:`_near`, but retain the nearest token coordinates."""
    candidates = sorted(
        (abs(x - qx), y, x, text)
        for x, y, text in values
        if abs(x - qx) < dx and dymin < (y - qy) < dymax
    )
    return (
        (candidates[0][2], candidates[0][1], candidates[0][3])
        if candidates
        else None
    )


def _cluster_by_gap(values, *, gap: float) -> list[list[float]]:
    """Generic one-dimensional clustering primitive for future profiles."""
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return []
    groups = [[ordered[0]]]
    for value in ordered[1:]:
        if value - groups[-1][-1] > gap:
            groups.append([])
        groups[-1].append(value)
    return groups


def _visualize_words(page, words) -> list:
    """Translate raw PyMuPDF word bboxes to visual coordinates exactly once."""
    if not int(getattr(page, "rotation", 0) or 0) % 360:
        return list(words)
    import fitz

    matrix = page.rotation_matrix
    output = []
    for word in words:
        try:
            rect = fitz.Rect(word[:4]) * matrix
            rect.normalize()
            output.append((rect.x0, rect.y0, rect.x1, rect.y1, *word[4:]))
        except (TypeError, ValueError, IndexError):
            continue
    return output


def _as_point(value) -> Optional[tuple[float, float]]:
    try:
        if hasattr(value, "x") and hasattr(value, "y"):
            return float(value.x), float(value.y)
        if len(value) >= 2:
            return float(value[0]), float(value[1])
    except (TypeError, ValueError, IndexError):
        pass
    return None


def _visual_point(value, matrix) -> Optional[list[float]]:
    point = _as_point(value)
    if point is None:
        return None
    import fitz

    transformed = fitz.Point(*point) * matrix
    return [float(transformed.x), float(transformed.y)]


def _visual_rect_points(value, matrix) -> list[list[float]]:
    try:
        import fitz

        rect = fitz.Rect(value)
    except (TypeError, ValueError):
        return []
    points = [
        (rect.x0, rect.y0),
        (rect.x1, rect.y0),
        (rect.x1, rect.y1),
        (rect.x0, rect.y1),
    ]
    return [point for point in (_visual_point(p, matrix) for p in points) if point]


def _visual_quad_points(value, matrix) -> list[list[float]]:
    points = []
    for attr in ("ul", "ur", "lr", "ll"):
        point = _visual_point(getattr(value, attr, None), matrix)
        if point:
            points.append(point)
    if points:
        return points
    try:
        return [
            point
            for point in (_visual_point(value[index], matrix) for index in range(4))
            if point
        ]
    except (TypeError, IndexError):
        return []


def _segments(points, *, closed=False):
    if len(points) < 2:
        return []
    result = [(*points[i], *points[i + 1]) for i in range(len(points) - 1)]
    if closed and len(points) > 2:
        result.append((*points[-1], *points[0]))
    return result


def _orientation(a, b, c) -> float:
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (
        c[0] - a[0]
    )


def _segments_intersect(a, b, c, d) -> bool:
    eps = 1e-7
    o1, o2 = _orientation(a, b, c), _orientation(a, b, d)
    o3, o4 = _orientation(c, d, a), _orientation(c, d, b)
    return (
        min(a[0], b[0]) - eps <= max(c[0], d[0])
        and min(c[0], d[0]) - eps <= max(a[0], b[0])
        and min(a[1], b[1]) - eps <= max(c[1], d[1])
        and min(c[1], d[1]) - eps <= max(a[1], b[1])
        and o1 * o2 <= eps
        and o3 * o4 <= eps
    )


def _primitive_hits_region(points, region) -> bool:
    if not region:
        return True
    if any(_point_in_polygon(point[0], point[1], region) for point in points):
        return True
    if len(points) >= 3 and any(
        _point_in_polygon(point[0], point[1], points) for point in region
    ):
        return True
    primitive_edges = _segments(points, closed=len(points) >= 3)
    region_edges = _segments(region, closed=True)
    return any(
        _segments_intersect(edge[:2], edge[2:], boundary[:2], boundary[2:])
        for edge in primitive_edges
        for boundary in region_edges
    )


def _drawing_primitives(raw_drawings, matrix, region):
    paths, lines, curves, polygons = [], [], [], []
    for drawing_index, drawing in enumerate(raw_drawings):
        path_items = []
        for item in drawing.get("items") or []:
            if not item:
                continue
            kind = str(item[0])
            points: list[list[float]] = []
            primitive = None
            if kind == "l" and len(item) >= 3:
                points = [
                    point
                    for point in (
                        _visual_point(item[1], matrix),
                        _visual_point(item[2], matrix),
                    )
                    if point
                ]
                primitive = "line"
            elif kind == "c" and len(item) >= 5:
                points = [
                    point
                    for point in (_visual_point(value, matrix) for value in item[1:5])
                    if point
                ]
                primitive = "curve"
            elif kind == "re" and len(item) >= 2:
                points = _visual_rect_points(item[1], matrix)
                primitive = "polygon"
            elif kind == "qu" and len(item) >= 2:
                points = _visual_quad_points(item[1], matrix)
                primitive = "polygon"
            if not primitive or len(points) < 2 or not _primitive_hits_region(points, region):
                continue
            if primitive == "line":
                value = [*points[0], *points[1]]
                lines.append(value)
            elif primitive == "curve":
                value = [coordinate for point in points for coordinate in point]
                curves.append(value)
            else:
                value = points
                polygons.append(value)
            path_items.append({"type": primitive, "points": points})
        if not path_items:
            continue
        path = {
            "drawing_index": drawing_index,
            "type": drawing.get("type"),
            "width": drawing.get("width"),
            "color": drawing.get("color"),
            "fill": drawing.get("fill"),
            "items": path_items,
        }
        paths.append(path)
    return paths, lines, curves, polygons


def _normalized_polygon(polygon_norm, page_w, page_h) -> list[list[float]]:
    try:
        return [
            [float(point[0]) * page_w, float(point[1]) * page_h]
            for point in polygon_norm
            if len(point) >= 2
        ]
    except (TypeError, ValueError, IndexError):
        return []


def _normalized_bbox(bbox_norm, page_w, page_h) -> Optional[list[float]]:
    try:
        x0, y0, x1, y1 = (float(value) for value in bbox_norm[:4])
    except (TypeError, ValueError, IndexError):
        return None
    if not (x1 > x0 and y1 > y0):
        return None
    return [x0 * page_w, y0 * page_h, x1 * page_w, y1 * page_h]


def _bbox_polygon(bbox) -> list[list[float]]:
    if not bbox:
        return []
    x0, y0, x1, y1 = bbox
    return [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]


def evaluate_extraction_quality(
    *,
    raw_words: int,
    visual_words: list,
    paths: list,
    page_size: Optional[list[float]],
    clip_mode: str,
    initial_reasons: Optional[list[str]] = None,
) -> dict:
    """Return an independent extraction-quality decision with explicit causes."""
    reasons = list(initial_reasons or [])
    coordinates_valid = bool(
        page_size
        and len(page_size) == 2
        and all(math.isfinite(float(value)) and float(value) > 0 for value in page_size)
        and all(
            len(word) >= 4
            and all(math.isfinite(float(value)) for value in word[:4])
            for word in visual_words
        )
    )
    words_inside = len(visual_words)
    geometry_available = bool(words_inside or paths)
    coverage = round(words_inside / max(raw_words, 1), 4) if raw_words else 0.0
    if not coordinates_valid and "coordinates_invalid" not in reasons:
        reasons.append("coordinates_invalid")
    if raw_words and not words_inside and "no_words_inside_block" not in reasons:
        reasons.append("no_words_inside_block")
    if not geometry_available and "geometry_unavailable" not in reasons:
        reasons.append("geometry_unavailable")
    metrics = {
        "coordinates_valid": coordinates_valid,
        "coverage": coverage,
        "words_inside_block": words_inside,
        "raw_words": raw_words,
        "geometry_available": geometry_available,
        "paths_inside_block": len(paths),
        "clip_mode": clip_mode,
    }
    return {
        "extraction_ok": not reasons,
        "reason": reasons[0] if reasons else None,
        "reasons": reasons,
        "metrics": metrics,
    }


def _failed_evidence(reason: str, *, page_index=None, page_source="unresolved"):
    gate = evaluate_extraction_quality(
        raw_words=0,
        visual_words=[],
        paths=[],
        page_size=None,
        clip_mode="unavailable",
        initial_reasons=[reason],
    )
    return VectorEvidence(
        page_index=page_index,
        extraction_gate=gate,
        provenance={
            "page_index_source": page_source,
            "coordinate_system": "visual",
            "rotation_fixed": True,
            "rotation_applied": False,
            "extraction_version": EXTRACTION_VERSION,
        },
    )


def extract_vector_evidence(
    pdf_path: Path,
    *,
    vector_text: str = "",
    page_index: Optional[int] = None,
    block_id: str = "",
    bbox_norm: Optional[list] = None,
    polygon_norm: Optional[list] = None,
    fallback_page_finder: Optional[Callable[[Any, str], Optional[int]]] = None,
    clip_enabled: bool = True,
    include_drawings: bool = True,
) -> VectorEvidence:
    """Extract one prepared block in a single visual coordinate system.

    ``page_index`` from prepared metadata is authoritative.  The supplied legacy
    finder is called only when that metadata is absent.
    """
    try:
        import fitz

        document = fitz.open(str(pdf_path))
    except Exception as exc:
        return _failed_evidence(f"pdf_open_failed:{type(exc).__name__}")

    resolved_index = None
    page_source = "prepared_block" if page_index is not None else "legacy_fallback"
    try:
        if page_index is not None:
            try:
                resolved_index = int(page_index)
            except (TypeError, ValueError):
                return _failed_evidence(
                    "page_index_invalid", page_index=page_index, page_source=page_source
                )
        elif fallback_page_finder is not None:
            try:
                resolved_index = fallback_page_finder(document, vector_text)
            except Exception as exc:
                return _failed_evidence(
                    f"page_fallback_failed:{type(exc).__name__}",
                    page_source=page_source,
                )
        if resolved_index is None:
            return _failed_evidence("page_not_found", page_source=page_source)
        if resolved_index < 0 or resolved_index >= document.page_count:
            return _failed_evidence(
                "page_index_out_of_range",
                page_index=resolved_index,
                page_source=page_source,
            )

        page = document[resolved_index]
        page_w, page_h = float(page.rect.width), float(page.rect.height)
        raw_words = list(page.get_text("words") or [])
        visual_words = _visualize_words(page, raw_words)

        block_bbox = _normalized_bbox(bbox_norm, page_w, page_h) if bbox_norm else None
        block_polygon = (
            _normalized_polygon(polygon_norm, page_w, page_h)
            if polygon_norm
            else _bbox_polygon(block_bbox)
        )
        clip_mode = "page"
        if clip_enabled and polygon_norm:
            clip_mode = "polygon"
            visual_words = _clip_words_to_polygon(
                visual_words, polygon_norm, page_w, page_h
            )
        elif clip_enabled and bbox_norm:
            clip_mode = "bbox"
            visual_words = _clip_words_to_bbox(
                visual_words, bbox_norm, page_w, page_h
            )
        region = block_polygon if clip_enabled and block_polygon else []
        raw_drawings = list(page.get_drawings() or []) if include_drawings else []
        paths, lines, curves, polygons = _drawing_primitives(
            raw_drawings, page.rotation_matrix, region
        )
        gate = evaluate_extraction_quality(
            raw_words=len(raw_words),
            visual_words=visual_words,
            paths=paths,
            page_size=[page_w, page_h],
            clip_mode=clip_mode,
        )
        rotation = int(page.rotation or 0) % 360
        return VectorEvidence(
            page_index=resolved_index,
            visual_words=visual_words,
            drawings=paths,
            paths=paths,
            lines=lines,
            curves=curves,
            polygons=polygons,
            block_polygon=block_polygon,
            block_bbox=block_bbox,
            page_size=[page_w, page_h],
            extraction_gate=gate,
            provenance={
                "block_id": str(block_id or ""),
                "page_index_source": page_source,
                "coordinate_system": "visual",
                "rotation_degrees": rotation,
                "rotation_fixed": True,
                "rotation_applied": bool(rotation),
                "raw_words": len(raw_words),
                "visual_words_inside_block": len(visual_words),
                "raw_drawings": len(raw_drawings),
                "paths_inside_block": len(paths),
                "drawings_extracted": include_drawings,
                "extraction_version": EXTRACTION_VERSION,
            },
        )
    except Exception as exc:
        return _failed_evidence(
            f"extraction_failed:{type(exc).__name__}",
            page_index=resolved_index,
            page_source=page_source,
        )
    finally:
        document.close()
