#!/usr/bin/env python3
"""Build an experimental, domain-neutral description of one vector PDF block.

The extractor intentionally uses only the PDF vector and text layers.  Raster
rendering is available solely for a human diagnostic crop and is never read by
the extraction code.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Iterable, Sequence

import fitz


SCHEMA_VERSION = "vector-block-research-v0.1"
DEFAULT_TOPOLOGY_CAP = 8_000
DEFAULT_STORAGE_CAP = 20_000
CURVE_STEPS = 6

_VALUE_RE = re.compile(
    r"(?:[+−-]?\d+(?:[.,]\d+)?)\s*(?:mm|мм|m|м|A|А|kW|кВт|V|В|%|°|DN\d*|Ø|ø|R)?",
    re.IGNORECASE,
)
_ENGINEERING_RE = re.compile(
    r"(?:Ø|ø|DN|R\s*\d|[+−-]\d+[.,]\d+|\d+(?:[.,]\d+)?\s*(?:мм|mm|м|m|A|А|кВт|kW|%|°))",
    re.IGNORECASE,
)


def _round(value: float, digits: int = 5) -> float:
    return round(float(value), digits)


def _point(value: Any) -> tuple[float, float]:
    return float(value.x), float(value.y)


def _bbox(points: Iterable[Sequence[float]]) -> list[float]:
    pts = list(points)
    if not pts:
        return [0.0, 0.0, 0.0, 0.0]
    xs = [float(point[0]) for point in pts]
    ys = [float(point[1]) for point in pts]
    return [_round(min(xs)), _round(min(ys)), _round(max(xs)), _round(max(ys))]


def _rect_intersects(left: Sequence[float], right: Sequence[float]) -> bool:
    return not (
        float(left[2]) < float(right[0])
        or float(left[0]) > float(right[2])
        or float(left[3]) < float(right[1])
        or float(left[1]) > float(right[3])
    )


def _point_in_polygon(point: Sequence[float], polygon: Sequence[Sequence[float]] | None) -> bool:
    if not polygon:
        return True
    x, y = float(point[0]), float(point[1])
    inside = False
    previous = polygon[-1]
    for current in polygon:
        x1, y1 = float(previous[0]), float(previous[1])
        x2, y2 = float(current[0]), float(current[1])
        crosses = (y1 > y) != (y2 > y)
        if crosses and x < (x2 - x1) * (y - y1) / (y2 - y1) + x1:
            inside = not inside
        previous = current
    return inside


def _clip_line(
    start: Sequence[float], end: Sequence[float], rect: Sequence[float]
) -> tuple[tuple[float, float], tuple[float, float]] | None:
    """Liang-Barsky clipping against the block bbox."""
    x0, y0 = float(start[0]), float(start[1])
    x1, y1 = float(end[0]), float(end[1])
    dx, dy = x1 - x0, y1 - y0
    p = (-dx, dx, -dy, dy)
    q = (x0 - rect[0], rect[2] - x0, y0 - rect[1], rect[3] - y0)
    low, high = 0.0, 1.0
    for pi, qi in zip(p, q):
        if abs(pi) < 1e-12:
            if qi < 0:
                return None
            continue
        ratio = qi / pi
        if pi < 0:
            low = max(low, ratio)
        else:
            high = min(high, ratio)
        if low > high:
            return None
    return ((x0 + low * dx, y0 + low * dy), (x0 + high * dx, y0 + high * dy))


def _norm_point(point: Sequence[float], rect: Sequence[float]) -> list[float]:
    width = max(float(rect[2]) - float(rect[0]), 1e-9)
    height = max(float(rect[3]) - float(rect[1]), 1e-9)
    return [
        _round((float(point[0]) - float(rect[0])) / width),
        _round((float(point[1]) - float(rect[1])) / height),
    ]


def _norm_bbox(bbox: Sequence[float], rect: Sequence[float]) -> list[float]:
    p0 = _norm_point(bbox[:2], rect)
    p1 = _norm_point(bbox[2:], rect)
    return [*p0, *p1]


def _distance(left: Sequence[float], right: Sequence[float]) -> float:
    return math.hypot(float(left[0]) - float(right[0]), float(left[1]) - float(right[1]))


def _point_segment_distance(
    point: Sequence[float], segment: Sequence[Sequence[float]]
) -> float:
    px, py = float(point[0]), float(point[1])
    (x1, y1), (x2, y2) = segment
    dx, dy = x2 - x1, y2 - y1
    if abs(dx) + abs(dy) < 1e-12:
        return math.hypot(px - x1, py - y1)
    position = max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (x1 + position * dx), py - (y1 + position * dy))


def _sample_cubic(item: Sequence[Any], steps: int = CURVE_STEPS) -> list[tuple[float, float]]:
    p0, p1, p2, p3 = (_point(item[index]) for index in range(1, 5))
    result = []
    for index in range(steps + 1):
        t = index / steps
        u = 1.0 - t
        result.append(
            (
                u**3 * p0[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t**3 * p3[0],
                u**3 * p0[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t**3 * p3[1],
            )
        )
    return result


def _segments_from_points(points: Sequence[Sequence[float]], closed: bool = False) -> list[list[list[float]]]:
    pairs = [[list(points[index]), list(points[index + 1])] for index in range(len(points) - 1)]
    if closed and len(points) > 2 and _distance(points[0], points[-1]) > 1e-6:
        pairs.append([list(points[-1]), list(points[0])])
    return pairs


def _color(value: Any) -> list[float] | None:
    if value is None:
        return None
    return [_round(component, 3) for component in value]


def _style(drawing: dict[str, Any]) -> dict[str, Any]:
    return {
        "path_type": drawing.get("type"),
        "stroke": _color(drawing.get("color")),
        "fill": _color(drawing.get("fill")),
        "stroke_width": _round(drawing.get("width") or 0.0, 4),
        "dashes": str(drawing.get("dashes") or ""),
        "line_cap": list(drawing.get("lineCap") or []),
        "line_join": drawing.get("lineJoin"),
        "stroke_opacity": drawing.get("stroke_opacity"),
        "fill_opacity": drawing.get("fill_opacity"),
        "layer": drawing.get("layer") or "",
    }


def _ellipse_kind(drawing: dict[str, Any]) -> str | None:
    items = drawing.get("items") or []
    if len(items) < 2 or any(item[0] != "c" for item in items):
        return None
    rect = drawing.get("rect")
    if rect is None or float(rect.width) <= 0 or float(rect.height) <= 0:
        return None
    points = [point for item in items for point in _sample_cubic(item, 3)]
    cx, cy = (float(rect.x0) + float(rect.x1)) / 2, (float(rect.y0) + float(rect.y1)) / 2
    rx, ry = float(rect.width) / 2, float(rect.height) / 2
    error = sum(abs(((x - cx) / rx) ** 2 + ((y - cy) / ry) ** 2 - 1.0) for x, y in points) / len(points)
    if error > 0.16:
        return None
    return "circle" if abs(rx - ry) / max(rx, ry) <= 0.08 else "ellipse"


def _primitive(
    *,
    kind: str,
    raw_segments: Sequence[Sequence[Sequence[float]]],
    block_rect: Sequence[float],
    drawing_index: int,
    item_indexes: Sequence[int],
    drawing: dict[str, Any],
    closed: bool,
    source_kinds: Sequence[str],
    polygon_abs: Sequence[Sequence[float]] | None = None,
) -> dict[str, Any] | None:
    clipped = []
    for start, end in raw_segments:
        segment = _clip_line(start, end, block_rect)
        if segment is None:
            continue
        midpoint = ((segment[0][0] + segment[1][0]) / 2, (segment[0][1] + segment[1][1]) / 2)
        if polygon_abs and not any(
            _point_in_polygon(point, polygon_abs) for point in (segment[0], midpoint, segment[1])
        ):
            continue
        clipped.append([[*map(_round, segment[0])], [*map(_round, segment[1])], midpoint])
    if not clipped:
        return None
    raw = [[segment[0], segment[1]] for segment in clipped]
    normalized = [[_norm_point(segment[0], block_rect), _norm_point(segment[1], block_rect)] for segment in raw]
    raw_points = [point for segment in raw for point in segment]
    norm_points = [point for segment in normalized for point in segment]
    raw_bbox = _bbox(raw_points)
    norm_bbox = _bbox(norm_points)
    length = sum(_distance(*segment) for segment in raw)
    norm_length = sum(_distance(*segment) for segment in normalized)
    first, last = raw[0][0], raw[-1][1]
    angle = math.degrees(math.atan2(last[1] - first[1], last[0] - first[0])) if first != last else None
    return {
        "id": "",
        "type": kind,
        "source_kinds": list(source_kinds),
        "drawing_index": drawing_index,
        "item_indexes": list(item_indexes),
        "raw": {"bbox": raw_bbox, "segments": raw},
        "normalized": {"bbox": norm_bbox, "segments": normalized},
        "length": _round(length),
        "length_norm": _round(norm_length),
        "angle_degrees": _round(angle, 3) if angle is not None else None,
        "segment_count": len(raw),
        "closed": bool(closed),
        "style": _style(drawing),
    }


def _drawing_primitives(
    drawing: dict[str, Any],
    drawing_index: int,
    block_rect: Sequence[float],
    polygon_abs: Sequence[Sequence[float]] | None = None,
) -> list[dict[str, Any]]:
    items = drawing.get("items") or []
    if not items:
        return []
    ellipse_kind = _ellipse_kind(drawing)
    if ellipse_kind:
        rect = drawing["rect"]
        points = [
            (
                (float(rect.x0) + float(rect.x1)) / 2 + float(rect.width) / 2 * math.cos(2 * math.pi * i / 24),
                (float(rect.y0) + float(rect.y1)) / 2 + float(rect.height) / 2 * math.sin(2 * math.pi * i / 24),
            )
            for i in range(24)
        ]
        primitive = _primitive(
            kind=ellipse_kind,
            raw_segments=_segments_from_points(points, closed=True),
            block_rect=block_rect,
            drawing_index=drawing_index,
            item_indexes=range(len(items)),
            drawing=drawing,
            closed=True,
            source_kinds=["c"] * len(items),
            polygon_abs=polygon_abs,
        )
        return [primitive] if primitive else []

    # Keep a PDF path as one primitive even when its line commands are disjoint.  Expanding a
    # CAD export with 50k hatch/grid commands into 50k Python objects bloats the description
    # without adding information: the complete segment list remains available inside the path.
    if len(items) > 1 and all(item[0] == "l" for item in items):
        points = [_point(items[0][1]), _point(items[0][2])]
        contiguous = 0
        for item in items[1:]:
            start, end = _point(item[1]), _point(item[2])
            if _distance(points[-1], start) <= 0.05:
                points.append(end)
                contiguous += 1
            elif _distance(points[-1], end) <= 0.05:
                points.append(start)
                contiguous += 1
        if contiguous == len(items) - 1:
            closed = bool(drawing.get("closePath")) or _distance(points[0], points[-1]) <= 0.05
            kind = "filled_polygon" if drawing.get("fill") is not None and closed else "polyline"
            primitive = _primitive(
                kind=kind,
                raw_segments=_segments_from_points(points, closed=closed),
                block_rect=block_rect,
                drawing_index=drawing_index,
                item_indexes=range(len(items)),
                drawing=drawing,
                closed=closed,
                source_kinds=["l"] * len(items),
                polygon_abs=polygon_abs,
            )
            return [primitive] if primitive else []
        primitive = _primitive(
            kind="path",
            raw_segments=[[_point(item[1]), _point(item[2])] for item in items],
            block_rect=block_rect,
            drawing_index=drawing_index,
            item_indexes=range(len(items)),
            drawing=drawing,
            closed=bool(drawing.get("closePath")),
            source_kinds=["l"] * len(items),
            polygon_abs=polygon_abs,
        )
        return [primitive] if primitive else []

    result = []
    for item_index, item in enumerate(items):
        kind = item[0]
        points: list[tuple[float, float]]
        closed = False
        output_kind = "path"
        if kind == "l":
            points = [_point(item[1]), _point(item[2])]
            output_kind = "line"
        elif kind == "re":
            rect = item[1]
            points = [
                (float(rect.x0), float(rect.y0)),
                (float(rect.x1), float(rect.y0)),
                (float(rect.x1), float(rect.y1)),
                (float(rect.x0), float(rect.y1)),
            ]
            closed = True
            output_kind = "filled_polygon" if drawing.get("fill") is not None else "rectangle"
        elif kind == "qu":
            quad = item[1]
            points = [_point(quad.ul), _point(quad.ur), _point(quad.lr), _point(quad.ll)]
            closed = True
            output_kind = "filled_polygon" if drawing.get("fill") is not None else "polyline"
        elif kind == "c":
            points = _sample_cubic(item)
            output_kind = "curve"
        else:
            continue
        primitive = _primitive(
            kind=output_kind,
            raw_segments=_segments_from_points(points, closed=closed),
            block_rect=block_rect,
            drawing_index=drawing_index,
            item_indexes=[item_index],
            drawing=drawing,
            closed=closed,
            source_kinds=[kind],
            polygon_abs=polygon_abs,
        )
        if primitive:
            result.append(primitive)
    return result


def _extract_primitives(
    page: fitz.Page,
    block_rect: Sequence[float],
    storage_cap: int,
    polygon_abs: Sequence[Sequence[float]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    drawings = page.get_drawings()
    candidates = [
        drawing
        for drawing in drawings
        if drawing.get("rect") is not None
        and _rect_intersects(
            [drawing["rect"].x0, drawing["rect"].y0, drawing["rect"].x1, drawing["rect"].y1],
            block_rect,
        )
    ]
    primitives = []
    source_item_counts: collections.Counter[str] = collections.Counter()
    for drawing_index, drawing in enumerate(candidates):
        source_item_counts.update(item[0] for item in drawing.get("items") or [])
        primitives.extend(
            _drawing_primitives(drawing, drawing_index, block_rect, polygon_abs=polygon_abs)
        )
    uncapped_count = len(primitives)
    if len(primitives) > storage_cap:
        # Keep uncommon/closed geometry and the longest remaining primitives.  The cap is explicit.
        primitives = sorted(
            primitives,
            key=lambda item: (
                item["type"] not in {"line", "polyline"},
                item["closed"],
                item["length_norm"],
            ),
            reverse=True,
        )[:storage_cap]
    for index, primitive in enumerate(primitives, 1):
        primitive["id"] = f"primitive-{index}"
    return primitives, {
        "page_drawings_total": len(drawings),
        "drawings_intersecting_block": len(candidates),
        "source_item_counts": dict(sorted(source_item_counts.items())),
        "primitives_uncapped": uncapped_count,
        "storage_cap": storage_cap,
        "storage_capped": uncapped_count > storage_cap,
    }


def _extract_text(
    page: fitz.Page,
    block_rect: Sequence[float],
    polygon_abs: Sequence[Sequence[float]] | None,
) -> list[dict[str, Any]]:
    result = []
    data = page.get_text("dict", clip=fitz.Rect(*block_rect))
    for block in data.get("blocks") or []:
        if block.get("type") != 0:
            continue
        for line in block.get("lines") or []:
            direction = line.get("dir") or (1.0, 0.0)
            rotation = math.degrees(math.atan2(float(direction[1]), float(direction[0])))
            for span in line.get("spans") or []:
                text = re.sub(r"\s+", " ", str(span.get("text") or "")).strip()
                if not text:
                    continue
                bbox = [float(value) for value in span.get("bbox")]
                center = ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2)
                if not _point_in_polygon(center, polygon_abs):
                    continue
                normalized_bbox = _norm_bbox(bbox, block_rect)
                category = "engineering_value" if _ENGINEERING_RE.search(text) else (
                    "numeric" if _VALUE_RE.fullmatch(text) else "label"
                )
                result.append(
                    {
                        "id": f"text-{len(result) + 1}",
                        "text": text,
                        "bbox": [_round(value) for value in bbox],
                        "bbox_norm": normalized_bbox,
                        "x_norm": _round((normalized_bbox[0] + normalized_bbox[2]) / 2),
                        "y_norm": _round((normalized_bbox[1] + normalized_bbox[3]) / 2),
                        "rotation": _round(rotation, 3),
                        "font_size": _round(span.get("size") or 0.0, 3),
                        "font": span.get("font") or "",
                        "category": category,
                    }
                )
    return result


def _all_segments(primitives: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for primitive in primitives:
        for segment in primitive["normalized"]["segments"]:
            if _distance(*segment) <= 1e-8:
                continue
            result.append(
                {
                    "primitive_id": primitive["id"],
                    "p1": segment[0],
                    "p2": segment[1],
                    "length": _round(_distance(*segment)),
                }
            )
    return result


def _segment_intersection(
    first: dict[str, Any], second: dict[str, Any], epsilon: float = 1e-9
) -> tuple[float, float, float, float] | None:
    x1, y1 = first["p1"]
    x2, y2 = first["p2"]
    x3, y3 = second["p1"]
    x4, y4 = second["p2"]
    denominator = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
    if abs(denominator) <= epsilon:
        return None
    t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denominator
    u = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3)) / denominator
    if epsilon < t < 1 - epsilon and epsilon < u < 1 - epsilon:
        return x1 + t * (x2 - x1), y1 + t * (y2 - y1), t, u
    return None


def _topology(
    primitives: Sequence[dict[str, Any]], tolerance: float, topology_cap: int
) -> dict[str, Any]:
    all_segments = _all_segments(primitives)
    segments = all_segments
    capped = len(segments) > topology_cap
    if capped:
        segments = sorted(segments, key=lambda item: item["length"], reverse=True)[:topology_cap]

    node_cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    nodes: list[list[float]] = []

    def cell(point: Sequence[float], size: float = tolerance) -> tuple[int, int]:
        return round(float(point[0]) / size), round(float(point[1]) / size)

    def node_id(point: Sequence[float]) -> int:
        cx, cy = cell(point)
        for gx in range(cx - 1, cx + 2):
            for gy in range(cy - 1, cy + 2):
                for candidate in node_cells.get((gx, gy), []):
                    if _distance(nodes[candidate], point) <= tolerance:
                        return candidate
        index = len(nodes)
        nodes.append([_round(point[0]), _round(point[1])])
        node_cells[(cx, cy)].append(index)
        return index

    edges = []
    adjacency: dict[int, set[int]] = collections.defaultdict(set)
    parent: list[int] = []

    def ensure(index: int) -> None:
        while len(parent) <= index:
            parent.append(len(parent))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left, right = find(left), find(right)
        if left != right:
            parent[right] = left

    for segment_index, segment in enumerate(segments):
        left, right = node_id(segment["p1"]), node_id(segment["p2"])
        ensure(max(left, right))
        if left == right:
            continue
        adjacency[left].add(right)
        adjacency[right].add(left)
        union(left, right)
        edges.append({"segment_index": segment_index, "left": left, "right": right})

    edge_by_segment = {edge["segment_index"]: edge for edge in edges}

    # Spatially indexed T-junctions.  Pure X crossings are recorded, not connected.
    index_cell = max(tolerance * 4, 0.005)
    segment_cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for index, segment in enumerate(segments):
        bbox = _bbox([segment["p1"], segment["p2"]])
        x0, y0 = math.floor(bbox[0] / index_cell), math.floor(bbox[1] / index_cell)
        x1, y1 = math.floor(bbox[2] / index_cell), math.floor(bbox[3] / index_cell)
        if (x1 - x0 + 1) * (y1 - y0 + 1) > 600:
            continue
        for gx in range(x0, x1 + 1):
            for gy in range(y0, y1 + 1):
                segment_cells[(gx, gy)].append(index)

    t_junctions = []
    for current_node, point in enumerate(nodes):
        gx, gy = math.floor(point[0] / index_cell), math.floor(point[1] / index_cell)
        candidates = set()
        for x in range(gx - 1, gx + 2):
            for y in range(gy - 1, gy + 2):
                candidates.update(segment_cells.get((x, y), []))
        for segment_index in candidates:
            edge = edge_by_segment.get(segment_index)
            if edge is None or current_node in {edge["left"], edge["right"]}:
                continue
            segment = segments[segment_index]
            if _point_segment_distance(point, [segment["p1"], segment["p2"]]) <= tolerance:
                adjacency[current_node].update((edge["left"], edge["right"]))
                adjacency[edge["left"]].add(current_node)
                adjacency[edge["right"]].add(current_node)
                union(current_node, edge["left"])
                t_junctions.append({"node": current_node, "segment_index": segment_index})

    crossings = []
    compared_pairs = set()
    for members in segment_cells.values():
        if len(members) > 150:
            continue
        for offset, first_index in enumerate(members):
            for second_index in members[offset + 1 :]:
                pair = (min(first_index, second_index), max(first_index, second_index))
                if pair in compared_pairs:
                    continue
                compared_pairs.add(pair)
                if segments[first_index]["primitive_id"] == segments[second_index]["primitive_id"]:
                    continue
                hit = _segment_intersection(segments[first_index], segments[second_index])
                if hit:
                    crossings.append(
                        {
                            "point": [_round(hit[0]), _round(hit[1])],
                            "segments": list(pair),
                            "connected": False,
                        }
                    )
                    if len(crossings) >= 5_000:
                        break
            if len(crossings) >= 5_000:
                break
        if len(crossings) >= 5_000:
            break

    by_root: dict[int, set[int]] = collections.defaultdict(set)
    for node in range(len(nodes)):
        by_root[find(node)].add(node)
    component_rows = []
    edge_by_component: dict[int, list[int]] = collections.defaultdict(list)
    for edge in edges:
        edge_by_component[find(edge["left"])].append(edge["segment_index"])
    for root, members in by_root.items():
        segment_ids = edge_by_component.get(root, [])
        points = [nodes[node] for node in members]
        component_rows.append(
            {
                "id": "",
                "node_count": len(members),
                "segment_count": len(segment_ids),
                "bbox_norm": _bbox(points),
                "endpoints": sum(1 for node in members if len(adjacency[node]) == 1),
                "branch_points": sum(1 for node in members if len(adjacency[node]) >= 3),
                "max_degree": max((len(adjacency[node]) for node in members), default=0),
                "segment_indexes": segment_ids[:200],
                "segment_indexes_truncated": len(segment_ids) > 200,
            }
        )
    component_rows.sort(key=lambda item: item["segment_count"], reverse=True)
    for index, component in enumerate(component_rows, 1):
        component["id"] = f"component-{index}"

    degree_histogram = collections.Counter(len(adjacency[node]) for node in range(len(nodes)))
    closed_primitives = [item for item in primitives if item["closed"]]
    nested = 0
    for index, inner in enumerate(closed_primitives[:1000]):
        a = inner["normalized"]["bbox"]
        for outer in closed_primitives[:1000]:
            if inner is outer:
                continue
            b = outer["normalized"]["bbox"]
            if b[0] < a[0] and b[1] < a[1] and b[2] > a[2] and b[3] > a[3]:
                nested += 1
                break
    return {
        "tolerance_norm": tolerance,
        "segments_total": len(all_segments),
        "segments_used": len(segments),
        "segments_capped": capped,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "connected_components": len(component_rows),
        "endpoints": degree_histogram.get(1, 0),
        "branch_points": sum(value for degree, value in degree_histogram.items() if degree >= 3),
        "degree_histogram": {str(key): value for key, value in sorted(degree_histogram.items())},
        "t_junctions": len(t_junctions),
        "x_crossings_unconnected": len(crossings),
        "crossings_truncated": len(crossings) >= 5_000,
        "closed_contours": len(closed_primitives),
        "nested_contours": nested,
        "components": component_rows[:50],
        "components_truncated": len(component_rows) > 50,
    }


def _anchors(
    texts: Sequence[dict[str, Any]], primitives: Sequence[dict[str, Any]], max_distance: float = 0.035
) -> list[dict[str, Any]]:
    segments = _all_segments(primitives)
    # Index segment bboxes expanded by the admissible anchor distance.  This changes the dense
    # plan case from O(texts * segments) to a local lookup while preserving the same distance test.
    cell_size = max(max_distance, 0.01)
    segment_cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for index, segment in enumerate(segments):
        bbox = _bbox([segment["p1"], segment["p2"]])
        x0 = math.floor((bbox[0] - max_distance) / cell_size)
        y0 = math.floor((bbox[1] - max_distance) / cell_size)
        x1 = math.floor((bbox[2] + max_distance) / cell_size)
        y1 = math.floor((bbox[3] + max_distance) / cell_size)
        for gx in range(x0, x1 + 1):
            for gy in range(y0, y1 + 1):
                segment_cells[(gx, gy)].append(index)
    result = []
    for text in texts:
        point = (text["x_norm"], text["y_norm"])
        nearest = None
        candidates = segment_cells.get(
            (math.floor(point[0] / cell_size), math.floor(point[1] / cell_size)), []
        )
        for segment_index in candidates:
            segment = segments[segment_index]
            distance = _point_segment_distance(point, [segment["p1"], segment["p2"]])
            if nearest is None or distance < nearest[0]:
                nearest = (distance, segment["primitive_id"])
        if nearest is None or nearest[0] > max_distance:
            result.append(
                {
                    "text_id": text["id"],
                    "geometry_id": None,
                    "relation": "unbound",
                    "distance_norm": None if nearest is None else _round(nearest[0]),
                    "confidence": "none",
                }
            )
            continue
        result.append(
            {
                "text_id": text["id"],
                "geometry_id": nearest[1],
                "relation": "nearest_geometry",
                "distance_norm": _round(nearest[0]),
                "confidence": "high" if nearest[0] <= 0.012 else "candidate",
            }
        )
    return result


def _primitive_pattern(primitive: dict[str, Any]) -> str:
    bbox = primitive["normalized"]["bbox"]
    width, height = max(bbox[2] - bbox[0], 1e-9), max(bbox[3] - bbox[1], 1e-9)
    local = []
    for start, end in primitive["normalized"]["segments"][:64]:
        pair = []
        for point in (start, end):
            pair.append((round((point[0] - bbox[0]) / width, 1), round((point[1] - bbox[1]) / height, 1)))
        local.append(tuple(sorted(pair)))
    payload = (
        primitive["type"],
        round(width / height, 1),
        primitive["segment_count"],
        tuple(sorted(local)),
    )
    return hashlib.sha1(repr(payload).encode("utf-8")).hexdigest()[:12]


def _repeated_elements(primitives: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for primitive in primitives:
        if primitive["type"] == "line" or primitive["length_norm"] < 0.002:
            continue
        groups[_primitive_pattern(primitive)].append(primitive)
    result = []
    for signature, members in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        if len(members) < 2:
            continue
        result.append(
            {
                "pattern_id": f"pattern_{signature}",
                "count": len(members),
                "primitive_type": members[0]["type"],
                "segment_count": members[0]["segment_count"],
                "instances": [item["normalized"]["bbox"] for item in members[:100]],
                "instances_truncated": len(members) > 100,
            }
        )
    return result[:100]


def _hatch_like_structures(primitives: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return conservative parallel-segment clusters; these are candidates, not semantics."""
    groups: dict[tuple[int, int], list[float]] = collections.defaultdict(list)
    for primitive in primitives:
        for start, end in primitive["normalized"]["segments"]:
            length = _distance(start, end)
            if length < 0.001 or length > 0.35:
                continue
            angle = math.degrees(math.atan2(end[1] - start[1], end[0] - start[0])) % 180
            angle_bucket = int(round(angle / 5.0) * 5) % 180
            length_bucket = int(round(math.log10(max(length, 1e-6)) * 5))
            groups[(angle_bucket, length_bucket)].append(length)
    result = []
    for (angle, length_bucket), lengths in sorted(
        groups.items(), key=lambda item: (-len(item[1]), item[0])
    ):
        if len(lengths) < 20:
            continue
        ordered = sorted(lengths)
        result.append(
            {
                "candidate_id": f"hatch_{angle}_{length_bucket}",
                "segment_count": len(lengths),
                "angle_degrees": angle,
                "median_length_norm": _round(ordered[len(ordered) // 2]),
                "interpretation": "parallel_segment_cluster_candidate",
            }
        )
    return result[:30]


def _canonical_primitive(primitive: dict[str, Any], normalized: bool, quantum: float) -> Any:
    space = primitive["normalized" if normalized else "raw"]
    segments = []
    for segment in space["segments"]:
        points = []
        for point in segment:
            points.append(tuple(round(float(value) / quantum) for value in point))
        segments.append(tuple(sorted(points)))
    return (
        primitive["type"],
        tuple(sorted(segments)),
        round(float(primitive["style"]["stroke_width"]) / max(quantum, 1e-9)),
        tuple(primitive["style"].get("stroke") or []),
        tuple(primitive["style"].get("fill") or []),
        primitive["style"].get("dashes"),
    )


def _hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _signatures(
    primitives: Sequence[dict[str, Any]], texts: Sequence[dict[str, Any]], topology: dict[str, Any]
) -> dict[str, Any]:
    raw_tokens = sorted(repr(_canonical_primitive(item, False, 0.001)) for item in primitives)
    normalized_tokens = sorted(repr(_canonical_primitive(item, True, 0.001)) for item in primitives)
    exact_text = sorted((item["text"], tuple(round(value, 3) for value in item["bbox"])) for item in texts)
    normalized_text = sorted((item["text"], round(item["x_norm"], 3), round(item["y_norm"], 3)) for item in texts)
    structural = {
        "primitive_types": collections.Counter(item["type"] for item in primitives),
        "closed": sum(1 for item in primitives if item["closed"]),
        "degree_histogram": topology["degree_histogram"],
        "component_segment_counts": sorted(
            (item["segment_count"] for item in topology["components"]), reverse=True
        )[:50],
        "text_categories": collections.Counter(item["category"] for item in texts),
    }
    return {
        "level_1_exact_vector": _hash([raw_tokens, exact_text]),
        "level_2_normalized_geometry": _hash([normalized_tokens, normalized_text]),
        "level_3_structural_topology": _hash(structural),
        "level_3_payload": structural,
    }


def _summary(
    primitives: Sequence[dict[str, Any]], texts: Sequence[dict[str, Any]], topology: dict[str, Any]
) -> dict[str, Any]:
    types = collections.Counter(item["type"] for item in primitives)
    return {
        "primitive_count": len(primitives),
        "primitive_types": dict(sorted(types.items())),
        "stroke_paths": sum(1 for item in primitives if item["style"].get("stroke") is not None),
        "filled_paths": sum(1 for item in primitives if item["style"].get("fill") is not None),
        "closed_paths": sum(1 for item in primitives if item["closed"]),
        "total_segment_count": sum(item["segment_count"] for item in primitives),
        "text_items": len(texts),
        "engineering_values": sum(1 for item in texts if item["category"] == "engineering_value"),
        "connected_components": topology["connected_components"],
    }


def _size_metrics(description: dict[str, Any]) -> dict[str, Any]:
    primitives = description["geometry"]["primitives"]
    raw = [
        {
            "type": item["type"],
            "raw": item["raw"],
            "style": item["style"],
            "closed": item["closed"],
        }
        for item in primitives
    ]
    normalized = [
        {
            "type": item["type"],
            "normalized": item["normalized"],
            "style": item["style"],
            "closed": item["closed"],
        }
        for item in primitives
    ]
    grouped = {
        "summary": description["primitive_summary"],
        "topology": description["topology"],
        "anchors": description["anchors"],
        "repeated_elements": description["repeated_elements"],
        "hatch_like_structures": description["hatch_like_structures"],
        "dimensions": description["dimensions"],
        "labels": description["labels"],
    }
    compact = {
        "quality": description["vector_quality"],
        "summary": description["primitive_summary"],
        "texts": [(item["text"], item["category"]) for item in description["texts"]],
        "topology": {
            key: description["topology"][key]
            for key in (
                "connected_components",
                "endpoints",
                "branch_points",
                "closed_contours",
                "nested_contours",
                "degree_histogram",
            )
        },
        "patterns": [
            (item["pattern_id"], item["count"]) for item in description["repeated_elements"]
        ],
        "hatch_candidates": [
            (item["angle_degrees"], item["segment_count"])
            for item in description["hatch_like_structures"][:10]
        ],
        "signatures": description["structural_signature"],
    }

    def metrics(value: Any) -> dict[str, int]:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        pretty = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)
        return {
            "bytes": len(payload.encode("utf-8")),
            "lines": pretty.count("\n") + 1,
            "estimated_tokens": math.ceil(len(payload) / 4),
        }

    return {
        "level_0_raw_vector": metrics(raw),
        "level_1_normalized_primitives": metrics(normalized),
        "level_2_groups_topology": metrics(grouped),
        "level_3_compact_description": metrics(compact),
        "compact_payload": compact,
    }


def extract_block(
    pdf_path: str | Path,
    *,
    page_index: int,
    bbox_norm: Sequence[float],
    block_id: str,
    polygon_norm: Sequence[Sequence[float]] | None = None,
    topology_tolerance: float = 0.0025,
    topology_cap: int = DEFAULT_TOPOLOGY_CAP,
    storage_cap: int = DEFAULT_STORAGE_CAP,
) -> dict[str, Any]:
    pdf_path = Path(pdf_path)
    document = fitz.open(pdf_path)
    if page_index < 0 or page_index >= len(document):
        raise ValueError(f"page_index {page_index} outside PDF with {len(document)} pages")
    if len(bbox_norm) != 4 or bbox_norm[2] <= bbox_norm[0] or bbox_norm[3] <= bbox_norm[1]:
        raise ValueError("bbox_norm must be [x0, y0, x1, y1] with positive area")
    page = document[page_index]
    block_rect = [
        float(bbox_norm[0]) * page.rect.width,
        float(bbox_norm[1]) * page.rect.height,
        float(bbox_norm[2]) * page.rect.width,
        float(bbox_norm[3]) * page.rect.height,
    ]
    polygon_abs = None
    if polygon_norm:
        polygon_abs = [
            [float(point[0]) * page.rect.width, float(point[1]) * page.rect.height]
            for point in polygon_norm
        ]
    primitives, extraction = _extract_primitives(
        page, block_rect, storage_cap, polygon_abs=polygon_abs
    )
    texts = _extract_text(page, block_rect, polygon_abs)
    topology = _topology(primitives, topology_tolerance, topology_cap)
    anchors = _anchors(texts, primitives)
    repeated = _repeated_elements(primitives)
    hatch_like = _hatch_like_structures(primitives)
    dimensions = [
        {
            "text_id": item["id"],
            "text": item["text"],
            "bbox_norm": item["bbox_norm"],
            "geometry_id": next(
                (anchor["geometry_id"] for anchor in anchors if anchor["text_id"] == item["id"]),
                None,
            ),
            "classification": "dimension_or_engineering_value_candidate",
        }
        for item in texts
        if item["category"] == "engineering_value"
    ]
    labels = [
        {"text_id": item["id"], "text": item["text"], "bbox_norm": item["bbox_norm"]}
        for item in texts
        if item["category"] == "label"
    ]
    if not primitives or topology["segments_total"] < 3:
        quality = "VECTOR_DATA_INSUFFICIENT"
    elif extraction["storage_capped"] or topology["segments_capped"]:
        quality = "LIMITED_CAPPED"
    elif topology["segments_total"] < 30:
        quality = "LIMITED"
    else:
        quality = "GOOD"
    description: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "block_id": block_id,
        "page": page_index + 1,
        "page_index": page_index,
        "bbox": [_round(value) for value in block_rect],
        "bbox_norm_on_page": [_round(value) for value in bbox_norm],
        "polygon_norm_on_page": polygon_norm,
        "source": {
            "pdf": str(pdf_path),
            "pdf_sha256": hashlib.sha256(pdf_path.read_bytes()).hexdigest(),
            "page_width": _round(page.rect.width),
            "page_height": _round(page.rect.height),
            "source_layers": ["PyMuPDF page.get_drawings", "PyMuPDF text spans"],
            "excluded_sources": ["OCR", "Vision", "raster recognition", "embeddings"],
        },
        "vector_quality": quality,
        "quality_notes": [
            note
            for condition, note in (
                (extraction["storage_capped"], "Primitive storage cap reached; longest/salient paths retained."),
                (topology["segments_capped"], "Topology cap reached; graph uses the longest segments."),
                (not texts, "No usable vector text spans in the block."),
                (quality == "VECTOR_DATA_INSUFFICIENT", "Useful PDF vector geometry is absent or insufficient."),
            )
            if condition
        ],
        "coordinate_system": {
            "raw": "PDF points in page coordinates",
            "normalized": "block-relative [0,1] x [0,1]",
            "formula": {
                "x_norm": "(x - block_x0) / block_width",
                "y_norm": "(y - block_y0) / block_height",
            },
            "normalization_removes": ["page position", "uniform presentation scale"],
            "normalization_does_not_use": ["affine warp", "free deformation", "pair alignment"],
        },
        "geometry": {
            "extraction": extraction,
            "primitives": primitives,
        },
        "primitive_summary": {},
        "texts": texts,
        "anchors": anchors,
        "topology": topology,
        "repeated_elements": repeated,
        "hatch_like_structures": hatch_like,
        "dimensions": dimensions,
        "labels": labels,
        "structural_signature": {},
        "size_metrics": {},
        "ambiguities": [
            "X-crossings are recorded but not treated as connected without a junction marker.",
            "Text-to-geometry anchors are proximity candidates, not semantic assertions.",
            "Repeated patterns identify geometric motifs, not discipline-specific object classes.",
            "Hatch-like structures are parallel-segment candidates and may also represent grids or repeated linework.",
            "Polygon clipping keeps segments with an endpoint or midpoint inside the polygon; boundary-only intersections remain approximate.",
        ],
    }
    description["primitive_summary"] = _summary(primitives, texts, topology)
    description["structural_signature"] = _signatures(primitives, texts, topology)
    description["size_metrics"] = _size_metrics(description)
    document.close()
    return description


def render_markdown(description: dict[str, Any]) -> str:
    summary = description["primitive_summary"]
    topology = description["topology"]
    types = ", ".join(f"{key}: {value}" for key, value in summary["primitive_types"].items()) or "нет"
    labels = [item["text"] for item in description["labels"]]
    values = [item["text"] for item in description["dimensions"]]
    repeated = description["repeated_elements"]
    hatch_like = description["hatch_like_structures"]
    sizes = description["size_metrics"]
    lines = [
        f"# Vector block `{description['block_id']}`",
        "",
        f"- Источник: `{description['source']['pdf']}`, страница {description['page']}",
        f"- Качество vector layer: **{description['vector_quality']}**",
        f"- Bbox PDF: `{description['bbox']}`",
        "",
        "## Общая структура",
        "",
        f"- Примитивов: {summary['primitive_count']} ({types})",
        f"- Сегментов: {summary['total_segment_count']}",
        f"- Замкнутых paths: {summary['closed_paths']}",
        f"- Компонентов: {topology['connected_components']}",
        f"- Конечных точек: {topology['endpoints']}; ветвлений: {topology['branch_points']}",
        f"- T-соединений: {topology['t_junctions']}; X-пересечений без подтверждённого junction: {topology['x_crossings_unconnected']}",
        "",
        "## Текст и значения",
        "",
        f"- Text spans: {len(description['texts'])}; привязано к ближайшей геометрии: {sum(1 for item in description['anchors'] if item['geometry_id'])}",
        f"- Инженерные значения-кандидаты: {', '.join(values[:40]) or 'нет'}{' …' if len(values) > 40 else ''}",
        f"- Основные подписи: {', '.join(labels[:40]) or 'нет'}{' …' if len(labels) > 40 else ''}",
        "",
        "## Повторяющиеся геометрические мотивы",
        "",
    ]
    if repeated:
        lines.extend(
            f"- `{item['pattern_id']}`: {item['count']} × {item['primitive_type']} ({item['segment_count']} сегм.)"
            for item in repeated[:30]
        )
    else:
        lines.append("- Устойчивые повторы на уровне отдельных paths не найдены.")
    lines.extend(["", "## Hatch-like candidates", ""])
    if hatch_like:
        lines.extend(
            f"- `{item['candidate_id']}`: {item['segment_count']} сегм., угол {item['angle_degrees']}°"
            for item in hatch_like[:15]
        )
    else:
        lines.append("- Консервативные кластеры параллельных сегментов не найдены.")
    lines.extend(
        [
            "",
            "## Многоуровневый размер",
            "",
            f"- Level 0 raw: {sizes['level_0_raw_vector']['bytes']} байт (~{sizes['level_0_raw_vector']['estimated_tokens']} токенов)",
            f"- Level 1 normalized: {sizes['level_1_normalized_primitives']['bytes']} байт (~{sizes['level_1_normalized_primitives']['estimated_tokens']} токенов)",
            f"- Level 2 groups/topology: {sizes['level_2_groups_topology']['bytes']} байт (~{sizes['level_2_groups_topology']['estimated_tokens']} токенов)",
            f"- Level 3 compact: {sizes['level_3_compact_description']['bytes']} байт (~{sizes['level_3_compact_description']['estimated_tokens']} токенов)",
            "",
            "## Неоднозначности",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in description["ambiguities"] + description["quality_notes"])
    return "\n".join(lines) + "\n"


def save_description(
    description: dict[str, Any], output_dir: str | Path, *, diagnostic_png: bool = False
) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "vector_block.json").write_text(
        json.dumps(description, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    (output_dir / "vector_block.md").write_text(render_markdown(description), encoding="utf-8")
    if diagnostic_png:
        document = fitz.open(description["source"]["pdf"])
        page = document[description["page_index"]]
        pixmap = page.get_pixmap(matrix=fitz.Matrix(1.35, 1.35), clip=fitz.Rect(*description["bbox"]), alpha=False)
        pixmap.save(output_dir / "diagnostic_crop.png")
        document.close()


def _parse_bbox(value: str) -> list[float]:
    result = [float(item) for item in value.split(",")]
    if len(result) != 4:
        raise argparse.ArgumentTypeError("bbox must contain four comma-separated numbers")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdf", required=True)
    parser.add_argument("--page", required=True, type=int, help="1-based PDF page")
    parser.add_argument("--bbox-norm", required=True, type=_parse_bbox)
    parser.add_argument("--block-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--diagnostic-png", action="store_true")
    parser.add_argument("--topology-tolerance", type=float, default=0.0025)
    args = parser.parse_args()
    description = extract_block(
        args.pdf,
        page_index=args.page - 1,
        bbox_norm=args.bbox_norm,
        block_id=args.block_id,
        topology_tolerance=args.topology_tolerance,
    )
    save_description(description, args.output, diagnostic_png=args.diagnostic_png)


if __name__ == "__main__":
    main()
