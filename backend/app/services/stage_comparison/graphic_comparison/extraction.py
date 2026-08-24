"""Versioned page extraction in visual PDF points.

The upstream ``blocks.json`` coordinate space is normalized, visual and
top-left.  PyMuPDF drawing/text coordinates are transformed through the page
rotation matrix once, then every downstream operation stays in physical PDF
points.  In particular, there is no independent x/w and y/h normalization.
"""
from __future__ import annotations

import hashlib
import math
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import cv2
import fitz
import numpy as np

from .policy import GraphicMode1Policy


CURVE_STEPS = 8
WHITE_EPS = 0.02


@dataclass(frozen=True)
class PreparedBlock:
    pdf_path: str
    pdf_hash: str
    page_index: int
    block_id: str
    block_type: str
    bbox_visual_pt: tuple[float, float, float, float]
    polygon_visual_pt: tuple[tuple[float, float], ...] | None = None
    label: str = ""
    source: dict[str, Any] = field(default_factory=dict)

    @property
    def width(self) -> float:
        return self.bbox_visual_pt[2] - self.bbox_visual_pt[0]

    @property
    def height(self) -> float:
        return self.bbox_visual_pt[3] - self.bbox_visual_pt[1]

    def public_scope(self) -> dict[str, Any]:
        return {
            "block_id": self.block_id,
            "page_index": self.page_index,
            "block_type": self.block_type,
            "bbox_visual_pt": [round(value, 3) for value in self.bbox_visual_pt],
            "polygon_visual_pt": (
                [[round(x, 3), round(y, 3)] for x, y in self.polygon_visual_pt]
                if self.polygon_visual_pt
                else None
            ),
            "source": dict(self.source),
        }


class PageArtifactCache:
    """One page parse per PDF hash/page/extractor version in this process."""

    def __init__(self, max_pages: int = 8) -> None:
        self._lock = threading.RLock()
        self.max_pages = max(1, int(max_pages))
        self._hashes: dict[tuple[str, int, int], str] = {}
        self._documents: dict[tuple[str, str], fitz.Document] = {}
        self._pages: OrderedDict[tuple[str, int, str], dict[str, Any]] = OrderedDict()
        self.stats = {
            "document_opens": 0,
            "page_parses": 0,
            "page_hits": 0,
            "page_evictions": 0,
        }

    def pdf_hash(self, pdf_path: str | Path) -> str:
        path = Path(pdf_path).expanduser().resolve()
        stat = path.stat()
        key = (str(path), stat.st_size, stat.st_mtime_ns)
        with self._lock:
            cached = self._hashes.get(key)
        if cached:
            return cached
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        value = digest.hexdigest()
        with self._lock:
            self._hashes[key] = value
        return value

    def _document(self, pdf_path: str, pdf_hash: str) -> fitz.Document:
        key = (pdf_path, pdf_hash)
        document = self._documents.get(key)
        if document is None:
            document = fitz.open(pdf_path)
            self._documents[key] = document
            self.stats["document_opens"] += 1
        return document

    def page(self, pdf_path: str, page_index: int, extractor_version: str) -> dict[str, Any]:
        resolved = str(Path(pdf_path).expanduser().resolve())
        digest = self.pdf_hash(resolved)
        key = (digest, int(page_index), extractor_version)
        with self._lock:
            found = self._pages.get(key)
            if found is not None:
                self._pages.move_to_end(key)
                self.stats["page_hits"] += 1
                return found
            document = self._document(resolved, digest)
            if page_index < 0 or page_index >= document.page_count:
                raise ValueError(f"page_index_out_of_range:{page_index}")
            page = document[page_index]
            record = {
                "pdf_path": resolved,
                "pdf_hash": digest,
                "page": page,
                "page_rect": tuple(float(value) for value in (page.rect.x0, page.rect.y0, page.rect.x1, page.rect.y1)),
                "rotation": int(page.rotation),
                "rotation_matrix": page.rotation_matrix,
                "drawings": page.get_drawings(extended=True),
                "n_images": len(page.get_images(full=True)),
                "lock": threading.RLock(),
                "flat": {},
            }
            self._pages[key] = record
            while len(self._pages) > self.max_pages:
                self._pages.popitem(last=False)
                self.stats["page_evictions"] += 1
            self.stats["page_parses"] += 1
            return record

    def reset_stats(self) -> None:
        with self._lock:
            for key in self.stats:
                self.stats[key] = 0

    def close(self) -> None:
        with self._lock:
            documents = list(self._documents.values())
            self._documents.clear()
            self._pages.clear()
            self._hashes.clear()
        for document in documents:
            document.close()


PAGE_CACHE = PageArtifactCache()


def _page_index(record: dict[str, Any]) -> int:
    if record.get("page_index") is not None:
        return int(record["page_index"])
    for key in ("page_label", "page", "page_number"):
        if record.get(key) is not None:
            return int(record[key]) - 1
    raise ValueError("prepared block has no page_index/page contract")


def _normalized_bbox(record: dict[str, Any]) -> tuple[float, float, float, float]:
    raw = record.get("coords_norm")
    if not isinstance(raw, (list, tuple)) or len(raw) != 4:
        raise ValueError("prepared block has no valid coords_norm")
    values = tuple(float(value) for value in raw)
    if not all(math.isfinite(value) for value in values):
        raise ValueError("prepared block coords_norm is not finite")
    x0, y0, x1, y1 = values
    if x1 <= x0 or y1 <= y0:
        raise ValueError("prepared block coords_norm is empty")
    return values


def block_from_record(
    pdf_path: str | Path,
    record: dict[str, Any],
    policy: GraphicMode1Policy,
    *,
    source_artifact: str = "blocks.json",
) -> PreparedBlock:
    """Resolve one real upstream block; never detects or replaces its bbox."""
    page_index = _page_index(record)
    page = PAGE_CACHE.page(str(pdf_path), page_index, policy.extractor_version)
    px0, py0, px1, py1 = page["page_rect"]
    page_width, page_height = px1 - px0, py1 - py0
    nx0, ny0, nx1, ny1 = _normalized_bbox(record)
    bbox = (
        px0 + nx0 * page_width,
        py0 + ny0 * page_height,
        px0 + nx1 * page_width,
        py0 + ny1 * page_height,
    )
    raw_polygon = record.get("polygon_points") or record.get("polygon_norm")
    polygon = None
    if isinstance(raw_polygon, list) and len(raw_polygon) >= 3:
        points = []
        for point in raw_polygon:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                raise ValueError("prepared block polygon_points is invalid")
            points.append((px0 + float(point[0]) * page_width, py0 + float(point[1]) * page_height))
        polygon = tuple(points)
    block_id = str(record.get("block_id") or record.get("id") or "").strip()
    if not block_id:
        raise ValueError("prepared block has no block_id")
    label = str(record.get("ocr_label") or record.get("label") or record.get("title") or "").strip()
    return PreparedBlock(
        pdf_path=str(Path(pdf_path).expanduser().resolve()),
        pdf_hash=page["pdf_hash"],
        page_index=page_index,
        block_id=block_id,
        block_type=str(record.get("block_type") or "").strip().casefold(),
        bbox_visual_pt=bbox,
        polygon_visual_pt=polygon,
        label=label[:160],
        source={
            "artifact": source_artifact,
            "coordinate_space": "normalized_page_top_left_visual",
        },
    )


def _sample_cubic(p0, p1, p2, p3) -> list[tuple[float, float]]:
    points = []
    for index in range(CURVE_STEPS + 1):
        t = index / CURVE_STEPS
        mt = 1.0 - t
        points.append((
            mt**3 * p0[0] + 3 * mt**2 * t * p1[0] + 3 * mt * t**2 * p2[0] + t**3 * p3[0],
            mt**3 * p0[1] + 3 * mt**2 * t * p1[1] + 3 * mt * t**2 * p2[1] + t**3 * p3[1],
        ))
    return points


def _is_white(color: Any) -> bool:
    try:
        return color is not None and all(float(channel) >= 1.0 - WHITE_EPS for channel in color)
    except (TypeError, ValueError):
        return False


def _paint_is_invisible(drawing: dict[str, Any]) -> bool:
    kind = str(drawing.get("type") or "")
    fill_opacity = drawing.get("fill_opacity")
    stroke_opacity = drawing.get("stroke_opacity")
    fill_hidden = _is_white(drawing.get("fill")) or float(1.0 if fill_opacity is None else fill_opacity) <= 0.01
    stroke_hidden = _is_white(drawing.get("color")) or float(1.0 if stroke_opacity is None else stroke_opacity) <= 0.01
    if kind == "f":
        return fill_hidden
    if kind == "s":
        return stroke_hidden
    if kind == "fs":
        return fill_hidden and stroke_hidden
    return False


def _visual_points(points: Sequence[tuple[float, float]], matrix: fitz.Matrix) -> np.ndarray:
    source = np.asarray(points, dtype=np.float64)
    result = np.empty_like(source)
    result[:, 0] = matrix.a * source[:, 0] + matrix.c * source[:, 1] + matrix.e
    result[:, 1] = matrix.b * source[:, 0] + matrix.d * source[:, 1] + matrix.f
    return result.astype(np.float32)


def _drawing_subpaths(drawing: dict[str, Any]) -> list[list[tuple[float, float]]]:
    subpaths: list[list[tuple[float, float]]] = []
    current: list[tuple[float, float]] = []

    def flush() -> None:
        nonlocal current
        if len(current) > 1:
            subpaths.append(current)
        current = []

    for item in drawing.get("items") or []:
        operation = item[0]
        if operation == "l":
            start = (float(item[1].x), float(item[1].y))
            end = (float(item[2].x), float(item[2].y))
            if current and abs(current[-1][0] - start[0]) <= 1e-9 and abs(current[-1][1] - start[1]) <= 1e-9:
                current.append(end)
            else:
                flush()
                current = [start, end]
        elif operation == "c":
            points = _sample_cubic(
                (item[1].x, item[1].y), (item[2].x, item[2].y),
                (item[3].x, item[3].y), (item[4].x, item[4].y),
            )
            if current and abs(current[-1][0] - points[0][0]) <= 1e-9 and abs(current[-1][1] - points[0][1]) <= 1e-9:
                current.extend(points[1:])
            else:
                flush()
                current = points
        elif operation == "re":
            rectangle = item[1]
            flush()
            subpaths.append([
                (rectangle.x0, rectangle.y0), (rectangle.x1, rectangle.y0),
                (rectangle.x1, rectangle.y1), (rectangle.x0, rectangle.y1),
                (rectangle.x0, rectangle.y0),
            ])
        elif operation == "qu":
            quad = item[1]
            flush()
            subpaths.append([
                (quad.ul.x, quad.ul.y), (quad.ur.x, quad.ur.y),
                (quad.lr.x, quad.lr.y), (quad.ll.x, quad.ll.y), (quad.ul.x, quad.ul.y),
            ])
    flush()
    return subpaths


def _clip_segment_to_rect(segment: tuple[float, float, float, float], rectangle: Sequence[float]):
    x0, y0, x1, y1 = segment
    dx, dy = x1 - x0, y1 - y0
    low, high = 0.0, 1.0
    for p, q in ((-dx, x0 - rectangle[0]), (dx, rectangle[2] - x0), (-dy, y0 - rectangle[1]), (dy, rectangle[3] - y0)):
        if abs(p) < 1e-12:
            if q < 0:
                return None
            continue
        value = q / p
        if p < 0:
            low = max(low, value)
        else:
            high = min(high, value)
        if high < low:
            return None
    return (x0 + low * dx, y0 + low * dy, x0 + high * dx, y0 + high * dy)


def _inside_polygon_group(point: tuple[float, float], polygons: list[np.ndarray]) -> bool:
    inside = False
    for polygon in polygons:
        if cv2.pointPolygonTest(polygon.astype(np.float32), point, False) >= 0:
            inside = not inside
    return inside


def _clip_segment_to_polygon_group(
    segment: tuple[float, float, float, float], polygons: list[np.ndarray]
) -> list[tuple[float, float, float, float]]:
    """Even-odd clip; handles horizontal/vertical and boundary segments."""
    x0, y0, x1, y1 = segment
    dx, dy = x1 - x0, y1 - y0
    values = [0.0, 1.0]
    for polygon in polygons:
        for index in range(len(polygon)):
            ax, ay = map(float, polygon[index])
            bx, by = map(float, polygon[(index + 1) % len(polygon)])
            ex, ey = bx - ax, by - ay
            denominator = dx * ey - dy * ex
            if abs(denominator) < 1e-12:
                continue
            qx, qy = ax - x0, ay - y0
            t = (qx * ey - qy * ex) / denominator
            u = (qx * dy - qy * dx) / denominator
            if -1e-9 <= t <= 1.0 + 1e-9 and -1e-9 <= u <= 1.0 + 1e-9:
                values.append(min(1.0, max(0.0, t)))
    values = sorted(set(round(value, 12) for value in values))
    output = []
    for start, end in zip(values, values[1:]):
        if end - start <= 1e-12:
            continue
        middle = (start + end) / 2.0
        point = (x0 + middle * dx, y0 + middle * dy)
        if _inside_polygon_group(point, polygons):
            output.append((
                x0 + start * dx, y0 + start * dy,
                x0 + end * dx, y0 + end * dy,
            ))
    return output


def _axis_aligned_clip_rect(polygons: list[np.ndarray]) -> tuple[float, float, float, float] | None:
    """Recognize the common closed four-corner PDF clipping rectangle."""
    if len(polygons) != 1:
        return None
    points = np.asarray(polygons[0], dtype=np.float64)
    if len(points) == 5 and np.max(np.abs(points[0] - points[-1])) <= 1e-6:
        points = points[:-1]
    if len(points) != 4:
        return None
    x0, y0 = float(points[:, 0].min()), float(points[:, 1].min())
    x1, y1 = float(points[:, 0].max()), float(points[:, 1].max())
    if x1 - x0 <= 1e-9 or y1 - y0 <= 1e-9:
        return None
    tolerance = 1e-5
    expected = {(x0, y0), (x0, y1), (x1, y0), (x1, y1)}
    actual = {
        (
            x0 if abs(float(point[0]) - x0) <= tolerance else x1,
            y0 if abs(float(point[1]) - y0) <= tolerance else y1,
        )
        for point in points
        if min(abs(float(point[0]) - x0), abs(float(point[0]) - x1)) <= tolerance
        and min(abs(float(point[1]) - y0), abs(float(point[1]) - y1)) <= tolerance
    }
    return (x0, y0, x1, y1) if actual == expected else None


def flatten_page(block: PreparedBlock, policy: GraphicMode1Policy) -> dict[str, Any]:
    """Flatten the whole page once; unreliable drawing rects are never a gate."""
    page_record = PAGE_CACHE.page(block.pdf_path, block.page_index, policy.extractor_version)
    cache_key = (True, True)
    with page_record["lock"]:
        cached = page_record["flat"].get(cache_key)
        if cached is not None:
            return cached
        matrix = page_record["rotation_matrix"]
        segments: list[tuple[float, float, float, float]] = []
        widths: list[float] = []
        path_ids: list[int] = []
        fills: list[dict[str, Any]] = []
        clip_stack: list[
            tuple[int, list[np.ndarray], tuple[float, float, float, float] | None]
        ] = []
        invisible_paths = 0
        dropped_invisible = 0
        paths_seen = 0
        paths_kept = 0

        for drawing_index, drawing in enumerate(page_record["drawings"]):
            level = int(drawing.get("level") or 0)
            while clip_stack and clip_stack[-1][0] >= level:
                clip_stack.pop()
            if drawing.get("type") == "clip":
                polygons = [
                    _visual_points(path, matrix)
                    for path in _drawing_subpaths(drawing)
                    if len(path) >= 3
                ]
                if polygons:
                    clip_stack.append((level, polygons, _axis_aligned_clip_rect(polygons)))
                continue

            paths_seen += 1
            invisible = _paint_is_invisible(drawing)
            if invisible:
                invisible_paths += 1
            scissor = None
            if drawing.get("scissor") is not None:
                visual = fitz.Rect(drawing["scissor"]) * matrix
                scissor = (
                    min(visual.x0, visual.x1), min(visual.y0, visual.y1),
                    max(visual.x0, visual.x1), max(visual.y0, visual.y1),
                )
            width = float(drawing.get("width") or 0.0)
            visual_subpaths = [
                _visual_points(path, matrix) for path in _drawing_subpaths(drawing)
            ]
            fill_polygons = []
            is_fill = (
                drawing.get("type") in {"f", "fs"}
                and drawing.get("fill") is not None
                and not _is_white(drawing.get("fill"))
            )
            kept_here = 0
            for path in visual_subpaths:
                if is_fill and not invisible and len(path) >= 3:
                    fill_polygons.append(path)
                for index in range(len(path) - 1):
                    segment = tuple(map(float, (*path[index], *path[index + 1])))
                    if invisible:
                        dropped_invisible += 1
                        continue
                    pieces = [segment]
                    if scissor is not None:
                        clipped = _clip_segment_to_rect(segment, scissor)
                        pieces = [clipped] if clipped is not None else []
                    for _clip_level, polygons, clip_rect in clip_stack:
                        if clip_rect is not None:
                            pieces = [
                                clipped
                                for piece in pieces
                                for clipped in [_clip_segment_to_rect(piece, clip_rect)]
                                if clipped is not None
                            ]
                        else:
                            pieces = [
                                part
                                for piece in pieces
                                for part in _clip_segment_to_polygon_group(piece, polygons)
                            ]
                        if not pieces:
                            break
                    for piece in pieces:
                        if math.hypot(piece[2] - piece[0], piece[3] - piece[1]) < 1e-9:
                            # A degenerate stroked mark is paint; retain a tiny
                            # physical segment.  Horizontal/vertical paths are
                            # naturally retained and never tested via Rect.intersects.
                            if drawing.get("type") in {"s", "fs"}:
                                piece = (piece[0], piece[1], piece[0] + 1e-3, piece[1])
                            else:
                                continue
                        segments.append(piece)
                        widths.append(width)
                        path_ids.append(drawing_index)
                        kept_here += 1
            if fill_polygons and not invisible:
                clips = [polygons for _level, polygons, _rect in clip_stack]
                if scissor is not None:
                    sx0, sy0, sx1, sy1 = scissor
                    clips = [*clips, [np.asarray([(sx0, sy0), (sx1, sy0), (sx1, sy1), (sx0, sy1)], np.float32)]]
                fills.append({
                    "polys": fill_polygons,
                    "even_odd": bool(drawing.get("even_odd")),
                    "clips": clips or None,
                })
            if kept_here or fill_polygons:
                paths_kept += 1

        segment_array = np.asarray(segments, dtype=np.float32).reshape(-1, 4)
        fill_bboxes = np.asarray([
            [
                min(float(poly[:, 0].min()) for poly in group["polys"]),
                min(float(poly[:, 1].min()) for poly in group["polys"]),
                max(float(poly[:, 0].max()) for poly in group["polys"]),
                max(float(poly[:, 1].max()) for poly in group["polys"]),
            ]
            for group in fills
        ], dtype=np.float32).reshape(-1, 4)
        result = {
            "segments": segment_array,
            "widths": np.asarray(widths, dtype=np.float32),
            "path_ids": np.asarray(path_ids, dtype=np.int32),
            "fills": fills,
            "fill_bboxes": fill_bboxes,
            "paths_seen": paths_seen,
            "paths_kept": paths_kept,
            "invisible_paths": invisible_paths,
            "segments_dropped_invisible": dropped_invisible,
        }
        page_record["flat"][cache_key] = result
        return result


def _clip_segments_to_rect(segments: np.ndarray, rectangle: Sequence[float]):
    if len(segments) == 0:
        return (
            np.zeros((0, 4), np.float32),
            np.zeros(0, np.int64),
            np.zeros(0, bool),
        )
    source = np.asarray(segments, dtype=np.float64)
    dx = source[:, 2] - source[:, 0]
    dy = source[:, 3] - source[:, 1]
    low = np.zeros(len(source), np.float64)
    high = np.ones(len(source), np.float64)
    alive = np.ones(len(source), bool)
    for p, q in (
        (-dx, source[:, 0] - rectangle[0]),
        (dx, rectangle[2] - source[:, 0]),
        (-dy, source[:, 1] - rectangle[1]),
        (dy, rectangle[3] - source[:, 1]),
    ):
        parallel = np.abs(p) < 1e-12
        alive &= ~(parallel & (q < 0.0))
        active = ~parallel
        values = np.zeros(len(source), np.float64)
        np.divide(q, p, out=values, where=active)
        lower = active & (p < 0.0)
        upper = active & (p > 0.0)
        low[lower] = np.maximum(low[lower], values[lower])
        high[upper] = np.minimum(high[upper], values[upper])
        alive &= high >= low
    indices = np.nonzero(alive)[0]
    kept = np.empty((len(indices), 4), np.float64)
    kept[:, 0] = source[indices, 0] + low[indices] * dx[indices]
    kept[:, 1] = source[indices, 1] + low[indices] * dy[indices]
    kept[:, 2] = source[indices, 0] + high[indices] * dx[indices]
    kept[:, 3] = source[indices, 1] + high[indices] * dy[indices]
    clipped_flags = np.any(np.abs(source[indices] - kept) > 1e-7, axis=1)
    return (
        kept.astype(np.float32),
        indices.astype(np.int64),
        clipped_flags,
    )


def extract_ink(block: PreparedBlock, policy: GraphicMode1Policy, margin_pt: float = 0.0) -> dict[str, Any]:
    page_record = PAGE_CACHE.page(block.pdf_path, block.page_index, policy.extractor_version)
    flat = flatten_page(block, policy)
    x0, y0, x1, y1 = block.bbox_visual_pt
    rectangle = (x0 - margin_pt, y0 - margin_pt, x1 + margin_pt, y1 + margin_pt)
    segments, indices, clipped = _clip_segments_to_rect(flat["segments"], rectangle)
    if block.polygon_visual_pt and margin_pt == 0.0:
        polygon = np.asarray(block.polygon_visual_pt, dtype=np.float32)
        pieces = []
        piece_indices = []
        piece_clipped = []
        for local_index, segment in enumerate(segments):
            clipped_parts = _clip_segment_to_polygon_group(tuple(map(float, segment)), [polygon])
            for part in clipped_parts:
                pieces.append(part)
                piece_indices.append(indices[local_index])
                piece_clipped.append(True)
        segments = np.asarray(pieces, dtype=np.float32).reshape(-1, 4)
        indices = np.asarray(piece_indices, dtype=np.int64)
        clipped = np.asarray(piece_clipped, dtype=bool)
    fill_bboxes = flat["fill_bboxes"]
    if len(fill_bboxes):
        keep = ~(
            (fill_bboxes[:, 2] < rectangle[0]) | (fill_bboxes[:, 0] > rectangle[2])
            | (fill_bboxes[:, 3] < rectangle[1]) | (fill_bboxes[:, 1] > rectangle[3])
        )
        fills = [flat["fills"][index] for index in np.nonzero(keep)[0]]
    else:
        fills = []
    return {
        "segments": segments,
        "widths": flat["widths"][indices] if len(indices) else np.zeros(0, np.float32),
        "path_ids": flat["path_ids"][indices] if len(indices) else np.zeros(0, np.int32),
        "fills": fills,
        "clipped": clipped,
        "bbox_visual_pt": tuple(block.bbox_visual_pt),
        "polygon_visual_pt": block.polygon_visual_pt,
        "page_rotation": page_record["rotation"],
        "page_rect": page_record["page_rect"],
        "paths_seen": flat["paths_seen"],
        "paths_kept": flat["paths_kept"],
        "invisible_paths": flat["invisible_paths"],
        "segments_dropped_invisible": flat["segments_dropped_invisible"],
        "n_page_images": page_record["n_images"],
    }


def ink_length(segments: np.ndarray) -> float:
    if len(segments) == 0:
        return 0.0
    return float(np.hypot(segments[:, 2] - segments[:, 0], segments[:, 3] - segments[:, 1]).sum())


def text_spans(block: PreparedBlock, policy: GraphicMode1Policy, margin_pt: float = 0.0) -> list[dict[str, Any]]:
    record = PAGE_CACHE.page(block.pdf_path, block.page_index, policy.extractor_version)
    page, matrix = record["page"], record["rotation_matrix"]
    x0, y0, x1, y1 = block.bbox_visual_pt
    visual_rect = fitz.Rect(x0 - margin_pt, y0 - margin_pt, x1 + margin_pt, y1 + margin_pt)
    data_rect = visual_rect * page.derotation_matrix
    output = []
    with record["lock"]:
        data = page.get_text("dict", clip=data_rect)
    for text_block in data.get("blocks") or []:
        if text_block.get("type") != 0:
            continue
        for line in text_block.get("lines") or []:
            for span in line.get("spans") or []:
                text = str(span.get("text") or "").strip()
                if not text:
                    continue
                rect = fitz.Rect(span["bbox"]) * matrix
                output.append({
                    "text": text,
                    "bbox": [min(rect.x0, rect.x1), min(rect.y0, rect.y1), max(rect.x0, rect.x1), max(rect.y0, rect.y1)],
                })
    return output


def image_rects(block: PreparedBlock, policy: GraphicMode1Policy) -> list[list[float]]:
    record = PAGE_CACHE.page(block.pdf_path, block.page_index, policy.extractor_version)
    page, matrix = record["page"], record["rotation_matrix"]
    output = []
    seen: set[int] = set()
    with record["lock"]:
        images = page.get_images(full=True)
        for image in images:
            xref = int(image[0])
            if xref in seen:
                continue
            seen.add(xref)
            try:
                rectangles = page.get_image_rects(xref)
            except Exception:
                continue
            for rectangle in rectangles:
                rect = fitz.Rect(rectangle) * matrix
                output.append([min(rect.x0, rect.x1), min(rect.y0, rect.y1), max(rect.x0, rect.x1), max(rect.y0, rect.y1)])
    return output


def image_coverage(block: PreparedBlock, policy: GraphicMode1Policy) -> float:
    bx = block.bbox_visual_pt
    area = max(1e-9, block.width * block.height)
    covered = 0.0
    for rect in image_rects(block, policy):
        x0, y0 = max(bx[0], rect[0]), max(bx[1], rect[1])
        x1, y1 = min(bx[2], rect[2]), min(bx[3], rect[3])
        if x1 > x0 and y1 > y0:
            covered += (x1 - x0) * (y1 - y0)
    return min(1.0, covered / area)


def _polygon_mask(frame: Sequence[float], cell_pt: float, polygon: Sequence[Sequence[float]] | None, shape: tuple[int, int]) -> np.ndarray | None:
    if not polygon:
        return None
    points = np.asarray([
        [(point[0] - frame[0]) / cell_pt, (point[1] - frame[1]) / cell_pt]
        for point in polygon
    ], dtype=np.float64)
    mask = np.zeros(shape, np.uint8)
    cv2.fillPoly(mask, [np.round(points).astype(np.int32)], 1)
    return mask


def rasterize(
    segments: np.ndarray,
    widths: np.ndarray | None,
    frame: Sequence[float],
    cell_pt: float,
    *,
    min_width_pt: float = 0.0,
    fills: list[dict[str, Any]] | None = None,
    clip_polygon: Sequence[Sequence[float]] | None = None,
) -> np.ndarray:
    """Binary vector ink in one physical frame, including fill/clip semantics."""
    x0, y0, x1, y1 = map(float, frame)
    width = max(1, int(math.ceil((x1 - x0) / cell_pt)))
    height = max(1, int(math.ceil((y1 - y0) / cell_pt)))
    canvas = np.zeros((height, width), np.uint8)
    for group in fills or []:
        polygons = []
        for polygon in group.get("polys") or []:
            points = np.empty((len(polygon), 2), np.float64)
            points[:, 0] = (polygon[:, 0] - x0) / cell_pt
            points[:, 1] = (polygon[:, 1] - y0) / cell_pt
            polygons.append(np.round(points).astype(np.int32))
        if not polygons:
            continue
        layer = np.zeros_like(canvas)
        if group.get("even_odd") and len(polygons) > 1:
            one = np.zeros_like(canvas)
            for polygon in polygons:
                one[:] = 0
                cv2.fillPoly(one, [polygon], 1)
                np.bitwise_xor(layer, one, out=layer)
        else:
            cv2.fillPoly(layer, polygons, 1)
        for clip_group in group.get("clips") or []:
            clip_layer = np.zeros_like(canvas)
            clip_polygons = []
            for polygon in clip_group:
                points = np.empty((len(polygon), 2), np.float64)
                points[:, 0] = (polygon[:, 0] - x0) / cell_pt
                points[:, 1] = (polygon[:, 1] - y0) / cell_pt
                clip_polygons.append(np.round(points).astype(np.int32))
            cv2.fillPoly(clip_layer, clip_polygons, 1)
            np.bitwise_and(layer, clip_layer, out=layer)
        np.bitwise_or(canvas, layer, out=canvas)

    if len(segments):
        points = np.empty((len(segments), 2, 2), np.int32)
        points[:, 0, 0] = np.round((segments[:, 0] - x0) / cell_pt)
        points[:, 0, 1] = np.round((segments[:, 1] - y0) / cell_pt)
        points[:, 1, 0] = np.round((segments[:, 2] - x0) / cell_pt)
        points[:, 1, 1] = np.round((segments[:, 3] - y0) / cell_pt)
        if widths is None:
            thicknesses = np.ones(len(segments), np.int32)
        else:
            thicknesses = np.maximum(1, np.round(np.maximum(widths, min_width_pt) / cell_pt)).astype(np.int32)
        for thickness in np.unique(thicknesses):
            selected = points[np.nonzero(thicknesses == thickness)[0]]
            cv2.polylines(canvas, [line for line in selected], False, 1, int(thickness), lineType=cv2.LINE_8)
    polygon_mask = _polygon_mask(frame, cell_pt, clip_polygon, canvas.shape)
    if polygon_mask is not None:
        np.bitwise_and(canvas, polygon_mask, out=canvas)
    return canvas


def render_gray(block: PreparedBlock, policy: GraphicMode1Policy, cell_pt: float) -> np.ndarray:
    record = PAGE_CACHE.page(block.pdf_path, block.page_index, policy.extractor_version)
    page = record["page"]
    with record["lock"]:
        pixmap = page.get_pixmap(
            clip=fitz.Rect(block.bbox_visual_pt),
            matrix=fitz.Matrix(1.0 / cell_pt, 1.0 / cell_pt),
            colorspace=fitz.csGRAY,
            alpha=False,
        )
    return np.frombuffer(pixmap.samples, dtype=np.uint8).reshape(pixmap.height, pixmap.width).copy()


__all__ = [
    "PAGE_CACHE",
    "PreparedBlock",
    "block_from_record",
    "extract_ink",
    "image_coverage",
    "image_rects",
    "ink_length",
    "rasterize",
    "render_gray",
    "text_spans",
]
