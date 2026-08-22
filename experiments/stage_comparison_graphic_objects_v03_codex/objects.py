"""Generic graphical-object formation for an already prepared block."""
from __future__ import annotations

import collections
import hashlib
import json
import math
from typing import Any, Iterable, Sequence

import fitz

from experiments.stage_comparison_vector_blocks import extractor as vector_v01

from .input_contract import public_input_contract
from .page_cache import PageDrawingCache


DESCRIPTION_SCHEMA = "graphic-block-description-v0.3-codex"
SEGMENT_CAP = 30_000
RELATION_CAP = 8_000


def _round(value: float, digits: int = 6) -> float:
    return round(float(value), digits)


def _distance(a: Sequence[float], b: Sequence[float]) -> float:
    return math.hypot(float(a[0]) - float(b[0]), float(a[1]) - float(b[1]))


def _bbox(points: Iterable[Sequence[float]]) -> list[float]:
    rows = list(points)
    if not rows:
        return [0.0, 0.0, 0.0, 0.0]
    return [min(p[0] for p in rows), min(p[1] for p in rows), max(p[0] for p in rows), max(p[1] for p in rows)]


def _bbox_union(a: Sequence[float], b: Sequence[float]) -> list[float]:
    return [min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])]


def _bbox_diag(box: Sequence[float]) -> float:
    return math.hypot(box[2] - box[0], box[3] - box[1])


def _bbox_gap(a: Sequence[float], b: Sequence[float]) -> float:
    dx = max(0.0, max(a[0], b[0]) - min(a[2], b[2]))
    dy = max(0.0, max(a[1], b[1]) - min(a[3], b[3]))
    return math.hypot(dx, dy)


def _center(box: Sequence[float]) -> tuple[float, float]:
    return ((box[0] + box[2]) / 2, (box[1] + box[3]) / 2)


def _point_in_polygon(point: Sequence[float], polygon: Sequence[Sequence[float]] | None) -> bool:
    if not polygon:
        return True
    x, y = point; inside = False; previous = polygon[-1]
    for current in polygon:
        x1, y1 = previous; x2, y2 = current
        if (y1 > y) != (y2 > y):
            cross = (x2 - x1) * (y - y1) / (y2 - y1) + x1
            if x < cross:
                inside = not inside
        previous = current
    return inside


def _clip_rect(a: Sequence[float], b: Sequence[float], rect: Sequence[float]) -> tuple[list[float], list[float]] | None:
    """Liang–Barsky strict line clipping."""
    x0, y0 = map(float, a); x1, y1 = map(float, b); dx, dy = x1 - x0, y1 - y0
    p = (-dx, dx, -dy, dy); q = (x0 - rect[0], rect[2] - x0, y0 - rect[1], rect[3] - y0)
    lo, hi = 0.0, 1.0
    for pi, qi in zip(p, q):
        if abs(pi) < 1e-12:
            if qi < 0:
                return None
            continue
        t = qi / pi
        if pi < 0:
            lo = max(lo, t)
        else:
            hi = min(hi, t)
        if lo > hi:
            return None
    return ([x0 + lo * dx, y0 + lo * dy], [x0 + hi * dx, y0 + hi * dy])


def _intersection_t(a: Sequence[float], b: Sequence[float], c: Sequence[float], d: Sequence[float]) -> float | None:
    ax, ay = a; bx, by = b; cx, cy = c; dx, dy = d
    rx, ry = bx - ax, by - ay; sx, sy = dx - cx, dy - cy
    den = rx * sy - ry * sx
    if abs(den) < 1e-12:
        return None
    t = ((cx - ax) * sy - (cy - ay) * sx) / den
    u = ((cx - ax) * ry - (cy - ay) * rx) / den
    return t if -1e-9 <= t <= 1 + 1e-9 and -1e-9 <= u <= 1 + 1e-9 else None


def _clip_polygon(a: Sequence[float], b: Sequence[float], polygon: Sequence[Sequence[float]] | None) -> list[list[list[float]]]:
    if not polygon:
        return [[list(a), list(b)]]
    ts = [0.0, 1.0]
    for index, c in enumerate(polygon):
        t = _intersection_t(a, b, c, polygon[(index + 1) % len(polygon)])
        if t is not None:
            ts.append(min(1.0, max(0.0, t)))
    ts = sorted(set(round(t, 10) for t in ts)); result = []
    for lo, hi in zip(ts, ts[1:]):
        mid = (lo + hi) / 2
        point = [a[0] + (b[0] - a[0]) * mid, a[1] + (b[1] - a[1]) * mid]
        if not _point_in_polygon(point, polygon):
            continue
        result.append([
            [a[0] + (b[0] - a[0]) * lo, a[1] + (b[1] - a[1]) * lo],
            [a[0] + (b[0] - a[0]) * hi, a[1] + (b[1] - a[1]) * hi],
        ])
    return result


def _white(color: Any) -> bool:
    return isinstance(color, (list, tuple)) and len(color) >= 3 and all(float(channel) >= 0.98 for channel in color[:3])


def _invisible_fill_only(style: dict[str, Any]) -> bool:
    return style.get("stroke") is None and _white(style.get("fill")) and float(style.get("fill_opacity") or 1.0) > 0


def _matrix(value: Sequence[float]) -> fitz.Matrix:
    return fitz.Matrix(*[float(item) for item in value])


def _transform_point(point: Sequence[float], matrix: Sequence[float]) -> tuple[float, float]:
    x, y = float(point[0]), float(point[1]); a, b, c, d, e, f = matrix
    return x * a + y * c + e, x * b + y * d + f


def _control_points(item: Sequence[Any]) -> list[tuple[float, float]]:
    kind = item[0]
    if kind == "l":
        return [vector_v01._point(item[1]), vector_v01._point(item[2])]
    if kind == "re":
        rect = item[1]; return [(rect.x0, rect.y0), (rect.x1, rect.y0), (rect.x1, rect.y1), (rect.x0, rect.y1)]
    if kind == "qu":
        quad = item[1]; return [vector_v01._point(quad.ul), vector_v01._point(quad.ur), vector_v01._point(quad.lr), vector_v01._point(quad.ll)]
    if kind == "c":
        return [vector_v01._point(value) for value in item[1:5]]
    return []


def _visual_bbox(points: Sequence[Sequence[float]], matrix: Sequence[float]) -> list[float]:
    return _bbox(_transform_point(point, matrix) for point in points)


def _item_segments(item: Sequence[Any]) -> tuple[list[list[list[float]]], str, bool]:
    kind = item[0]
    if kind == "l":
        points = [vector_v01._point(item[1]), vector_v01._point(item[2])]; closed = False
    elif kind == "re":
        rect = item[1]; points = [(rect.x0, rect.y0), (rect.x1, rect.y0), (rect.x1, rect.y1), (rect.x0, rect.y1)]; closed = True
    elif kind == "qu":
        quad = item[1]; points = [vector_v01._point(quad.ul), vector_v01._point(quad.ur), vector_v01._point(quad.lr), vector_v01._point(quad.ll)]; closed = True
    elif kind == "c":
        points = vector_v01._sample_cubic(item); closed = False
    else:
        return [], str(kind), False
    return vector_v01._segments_from_points(points, closed=closed), str(kind), closed


class _Dsu:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]; value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        a, b = self.find(left), self.find(right)
        if a != b:
            self.parent[b] = a


def _split_components(segments: list[list[list[float]]], tolerance: float) -> list[list[list[list[float]]]]:
    if len(segments) <= 1:
        return [segments] if segments else []
    dsu = _Dsu(len(segments)); buckets: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for index, segment in enumerate(segments):
        for point in segment:
            key = (round(point[0] / tolerance), round(point[1] / tolerance))
            for other in buckets[key]:
                dsu.union(index, other)
            buckets[key].append(index)
    groups: dict[int, list[list[list[float]]]] = collections.defaultdict(list)
    for index, segment in enumerate(segments):
        groups[dsu.find(index)].append(segment)
    return list(groups.values())


def _spatial_cap(atoms: list[dict[str, Any]], cap: int, width: float, height: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    total = sum(len(atom["segments"]) for atom in atoms)
    if total <= cap:
        return atoms, {"segments_capped": False, "segments_before": total, "segments_after": total, "occupied_cells_before": None, "occupied_cells_after": None}
    buckets: dict[tuple[int, int], list[tuple[int, int]]] = collections.defaultdict(list)
    for atom_index, atom in enumerate(atoms):
        for segment_index, segment in enumerate(atom["segments"]):
            midpoint = ((segment[0][0] + segment[1][0]) / 2, (segment[0][1] + segment[1][1]) / 2)
            cell = (min(15, max(0, int(midpoint[0] / max(width, 1e-9) * 16))), min(15, max(0, int(midpoint[1] / max(height, 1e-9) * 16))))
            buckets[cell].append((atom_index, segment_index))
    keep: set[tuple[int, int]] = set(); cells = sorted(buckets); offset = 0
    while len(keep) < cap:
        progressed = False
        for cell in cells:
            if offset < len(buckets[cell]):
                keep.add(buckets[cell][offset]); progressed = True
                if len(keep) >= cap:
                    break
        if not progressed:
            break
        offset += 1
    result = []
    for atom_index, atom in enumerate(atoms):
        segments = [segment for segment_index, segment in enumerate(atom["segments"]) if (atom_index, segment_index) in keep]
        if segments:
            result.append({**atom, "segments": segments, "bbox": _bbox(point for segment in segments for point in segment), "cap_sampled": len(segments) < len(atom["segments"])})
    occupied_after = set()
    for atom in result:
        for segment in atom["segments"]:
            midpoint = ((segment[0][0] + segment[1][0]) / 2, (segment[0][1] + segment[1][1]) / 2)
            occupied_after.add((min(15, max(0, int(midpoint[0] / max(width, 1e-9) * 16))), min(15, max(0, int(midpoint[1] / max(height, 1e-9) * 16)))))
    return result, {"segments_capped": True, "segments_before": total, "segments_after": sum(len(a["segments"]) for a in result), "occupied_cells_before": len(cells), "occupied_cells_after": len(occupied_after), "policy": "round-robin spatial cells; never longest-lines-only"}


def _extract_atoms(payload: dict[str, Any], block: dict[str, Any], cap: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    page_rect = payload["page_rect"]; page_width, page_height = page_rect[2] - page_rect[0], page_rect[3] - page_rect[1]
    coords = block["coords_norm"]
    clip = [coords[0] * page_width, coords[1] * page_height, coords[2] * page_width, coords[3] * page_height]
    polygon_page = None
    if block.get("polygon_points_norm"):
        polygon_page = [[float(x) * page_width, float(y) * page_height] for x, y in block["polygon_points_norm"]]
    rotation = tuple(float(value) for value in payload["rotation_matrix"])
    tolerance = max(0.5, math.hypot(clip[2] - clip[0], clip[3] - clip[1]) * 0.0008)
    atoms = []; invisible = 0; candidates = 0; source_paths = 0
    for drawing_index, drawing in enumerate(payload["drawings"]):
        rect = drawing.get("rect")
        if rect is None:
            continue
        visual_rect = _visual_bbox(((rect.x0,rect.y0),(rect.x1,rect.y0),(rect.x1,rect.y1),(rect.x0,rect.y1)),rotation)
        if visual_rect[2] < clip[0] or visual_rect[0] > clip[2] or visual_rect[3] < clip[1] or visual_rect[1] > clip[3]:
            continue
        candidates += 1
        source_paths += 1; style = vector_v01._style(drawing)
        if _invisible_fill_only(style):
            invisible += 1; continue
        visible_segments = []; source_kinds = set()
        for item in drawing.get("items") or []:
            controls = _control_points(item)
            if not controls:
                continue
            item_box = _visual_bbox(controls, rotation)
            if item_box[2] < clip[0] or item_box[0] > clip[2] or item_box[3] < clip[1] or item_box[1] > clip[3]:
                continue
            raw_segments, source_kind, _ = _item_segments(item); source_kinds.add(source_kind)
            for raw_segment in raw_segments:
                first = _transform_point(raw_segment[0], rotation); second = _transform_point(raw_segment[1], rotation)
                clipped = _clip_rect(first, second, clip)
                if clipped is None:
                    continue
                for segment in _clip_polygon(*clipped, polygon_page):
                    local = [[segment[0][0] - clip[0], segment[0][1] - clip[1]], [segment[1][0] - clip[0], segment[1][1] - clip[1]]]
                    if _distance(*local) > 1e-6:
                        visible_segments.append(local)
        for component_index, segments in enumerate(_split_components(visible_segments, tolerance)):
            closed_component = bool(drawing.get("closePath")) or (bool(segments) and _distance(segments[0][0], segments[-1][1]) <= tolerance)
            atoms.append({
                "atom_id": f"path-{drawing_index}-{component_index}",
                "source_drawing": drawing_index,
                "primitive_type": "path:" + ",".join(sorted(source_kinds)),
                "segments": segments,
                "bbox": _bbox(point for segment in segments for point in segment),
                "closed_hint": closed_component,
                "style": style,
                "formation": ["source_path_items", "spatial_connectivity_split"],
                "cap_sampled": False,
            })
    width, height = clip[2] - clip[0], clip[3] - clip[1]
    atoms, cap_info = _spatial_cap(atoms, cap, width, height)
    unrotated_visual = fitz.Rect(*clip) * _matrix(payload["derotation_matrix"]); unrotated_visual.normalize()
    offset = payload["cropbox_position"]
    return atoms, {
        "page_drawings_total": len(payload["drawings"]), "drawings_intersecting_block": candidates,
        "source_paths": source_paths, "invisible_white_fill_only_filtered": invisible,
        "block_visual_rect": [_round(value) for value in clip],
        "block_unrotated_rect": [_round(value) for value in unrotated_visual],
        "block_cropbox_space_rect": [_round(unrotated_visual.x0 + offset[0]), _round(unrotated_visual.y0 + offset[1]), _round(unrotated_visual.x1 + offset[0]), _round(unrotated_visual.y1 + offset[1])],
        "coordinate_method": "upstream visual coords + page.rotation_matrix; inverse audit via derotation_matrix + cropbox_position, matching production pdf_crop",
        **cap_info,
    }


def _angle(segment: Sequence[Sequence[float]]) -> float:
    return math.degrees(math.atan2(segment[1][1] - segment[0][1], segment[1][0] - segment[0][0])) % 180


def _group_atoms(atoms: list[dict[str, Any]], block_diag: float) -> tuple[list[list[dict[str, Any]]], dict[str, Any]]:
    if not atoms:
        return [], {"symbol_candidate_cap": False, "symbol_candidate_lists_truncated": 0}
    dsu = _Dsu(len(atoms)); endpoint_map: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    endpoint_tol = max(0.6, block_diag * 0.001)
    for index, atom in enumerate(atoms):
        for segment in atom["segments"]:
            for point in segment:
                key = (round(point[0] / endpoint_tol), round(point[1] / endpoint_tol))
                for other in endpoint_map[key]:
                    first_angle, second_angle = _angle(atom["segments"][0]), _angle(atoms[other]["segments"][0])
                    if min(abs(first_angle - second_angle), 180 - abs(first_angle - second_angle)) <= 4:
                        dsu.union(index, other)
                    # A small enclosure or symbol can be emitted as one path or
                    # several perpendicular paths.  Join those export variants
                    # only inside a hard symbol-scale bound; doing this without
                    # the bound collapses a whole plan into one component.
                    elif _bbox_diag(_bbox_union(atom["bbox"], atoms[other]["bbox"])) <= block_diag * 0.12:
                        dsu.union(index, other)
                endpoint_map[key].append(index)
    # A second, bounded rule groups detached strokes of a symbol.  It cannot
    # chain into a page-wide foreground/background component.
    small = [i for i, atom in enumerate(atoms) if _bbox_diag(atom["bbox"]) <= block_diag * 0.05]
    eligible_before = len(small)
    # Detached-stroke grouping is optional evidence, not permission for an
    # unbounded all-pairs glyph search.  On dense blocks retain spatial
    # coverage with a round-robin sample and mark formation partial.
    if len(small) > 2_000:
        bounds = _bbox(point for index in small for point in ((atoms[index]["bbox"][0], atoms[index]["bbox"][1]), (atoms[index]["bbox"][2], atoms[index]["bbox"][3])))
        grid_sample: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
        for index in small:
            cx, cy = _center(atoms[index]["bbox"])
            gx = min(31, max(0, int((cx - bounds[0]) / max(bounds[2] - bounds[0], 1e-9) * 32)))
            gy = min(31, max(0, int((cy - bounds[1]) / max(bounds[3] - bounds[1], 1e-9) * 32)))
            grid_sample[(gx, gy)].append(index)
        selected = []; offset = 0; cells = sorted(grid_sample)
        while len(selected) < 2_000:
            progressed = False
            for cell in cells:
                if offset < len(grid_sample[cell]):
                    selected.append(grid_sample[cell][offset]); progressed = True
                    if len(selected) >= 2_000:
                        break
            if not progressed:
                break
            offset += 1
        small = selected
    cell_size = max(1.0, block_diag * 0.025); grid: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    truncated = max(0, eligible_before - len(small))
    for index in small:
        cx, cy = _center(atoms[index]["bbox"]); key = (int(cx / cell_size), int(cy / cell_size))
        for gx in range(key[0] - 1, key[0] + 2):
            for gy in range(key[1] - 1, key[1] + 2):
                candidates = grid.get((gx, gy), [])
                if len(candidates) > 64:
                    truncated += 1
                for other in candidates[-64:]:
                    combined = _bbox_union(atoms[index]["bbox"], atoms[other]["bbox"])
                    if _bbox_diag(combined) <= block_diag * 0.08 and _bbox_gap(atoms[index]["bbox"], atoms[other]["bbox"]) <= max(0.75, block_diag * 0.003):
                        dsu.union(index, other)
        grid[key].append(index)
    groups: dict[int, list[dict[str, Any]]] = collections.defaultdict(list)
    for index, atom in enumerate(atoms):
        groups[dsu.find(index)].append(atom)
    return list(groups.values()), {"symbol_candidate_cap": truncated > 0, "symbol_candidates_before": eligible_before, "symbol_candidates_after": len(small), "symbol_candidate_lists_truncated": truncated, "candidate_limit_per_neighbor_cell": 64, "selection": "32x32 spatial round-robin when over 2000 eligible atoms"}


def _node_stats(segments: list[list[list[float]]], tolerance: float) -> dict[str, Any]:
    degree: collections.Counter[tuple[int, int]] = collections.Counter()
    for segment in segments:
        for point in segment:
            degree[(round(point[0] / tolerance), round(point[1] / tolerance))] += 1
    return {"nodes": len(degree), "endpoints": sum(value == 1 for value in degree.values()), "branch_points": sum(value >= 3 for value in degree.values()), "degree_histogram": dict(sorted(collections.Counter(degree.values()).items()))}


def _geometry_signature(segments: list[list[list[float]]], box: Sequence[float]) -> str:
    scale = max(box[2] - box[0], box[3] - box[1], 1e-6)
    canonical = []
    for segment in segments:
        points = [tuple(round((point[index] - box[index]) / scale / 0.01) for index in (0, 1)) for point in segment]
        canonical.append(tuple(sorted(points)))
    return hashlib.sha256(json.dumps(sorted(canonical), separators=(",", ":")).encode()).hexdigest()[:20]


def _style_summary(atoms: list[dict[str, Any]]) -> dict[str, Any]:
    fields = ("stroke", "fill", "stroke_width", "dashes", "stroke_opacity", "fill_opacity", "line_cap", "line_join")
    result = {}
    for field in fields:
        values = [json.dumps(atom["style"].get(field), sort_keys=True) for atom in atoms]
        if values:
            value, count = collections.Counter(values).most_common(1)[0]
            result[field] = json.loads(value); result[f"{field}_coverage"] = _round(count / len(values))
    return result


def _object_type(box: Sequence[float], segments: list[list[list[float]]], atoms: list[dict[str, Any]], nodes: dict[str, Any], block_diag: float) -> str:
    width, height = box[2] - box[0], box[3] - box[1]; diag = _bbox_diag(box); aspect = max(width, height) / max(min(width, height), 1e-6)
    closed = any(atom["closed_hint"] for atom in atoms)
    if closed and diag > block_diag * 0.015:
        return "CLOSED_REGION"
    if len(segments) >= 20 and (nodes["branch_points"] >= 2 or diag > block_diag * 0.2):
        return "CONNECTED_NETWORK"
    if aspect >= 8 or (len(segments) <= 3 and diag > block_diag * 0.04):
        return "LINEAR_OBJECT"
    if diag <= block_diag * 0.1 and 2 <= len(segments) <= 200:
        return "SYMBOL_OBJECT"
    if len(atoms) > 1:
        return "COMPOSITE_GRAPHIC"
    return "UNKNOWN_OBJECT"


def _build_objects(groups: list[list[dict[str, Any]]], width: float, height: float) -> list[dict[str, Any]]:
    block_diag = math.hypot(width, height); objects = []
    for members in groups:
        segments = [segment for atom in members for segment in atom["segments"]]
        box = _bbox(point for segment in segments for point in segment)
        nodes = _node_stats(segments, max(0.5, block_diag * 0.001))
        object_type = _object_type(box, segments, members, nodes, block_diag)
        angle_hist = collections.Counter(int(_angle(segment) // 15) for segment in segments)
        length = sum(_distance(*segment) for segment in segments)
        aspect = max(box[2]-box[0], box[3]-box[1]) / max(min(box[2]-box[0], box[3]-box[1]), 1e-6)
        family_basis = {"type": object_type, "segments_bin": min(20, round(math.log2(len(segments)+1))), "aspect_bin": min(20, round(math.log2(aspect+1)*2)), "angles": sorted(angle_hist.items()), "closed": any(a["closed_hint"] for a in members)}
        object_id = f"obj_{len(objects)+1:05d}"
        objects.append({
            "object_id": object_id, "type": object_type,
            "bbox_norm": [_round(box[0]/max(width,1e-9)), _round(box[1]/max(height,1e-9)), _round(box[2]/max(width,1e-9)), _round(box[3]/max(height,1e-9))],
            "center_isotropic": [_round(_center(box)[0]/max(block_diag,1e-9)), _round(_center(box)[1]/max(block_diag,1e-9))],
            "size_isotropic": [_round((box[2]-box[0])/max(block_diag,1e-9)), _round((box[3]-box[1])/max(block_diag,1e-9))],
            "geometry_signature": _geometry_signature(segments, box),
            "family_signature": hashlib.sha256(json.dumps(family_basis,sort_keys=True).encode()).hexdigest()[:16],
            "geometry": {"segment_count": len(segments), "length_isotropic": _round(length/max(block_diag,1e-9)), "angle_histogram_15deg": dict(sorted(angle_hist.items())), **nodes},
            "style": _style_summary(members),
            "label_anchor_ids": [],
            "relation_ids": [],
            "formation": {"atom_count": len(members), "rules": sorted({rule for atom in members for rule in atom["formation"]} | ({"bounded_symbol_group"} if len(members)>1 else set())), "cap_sampled": any(atom["cap_sampled"] for atom in members)},
            "provenance": [{"source": "page.get_drawings", "drawing_index": atom["source_drawing"]} for atom in members[:20]],
            "_segments": segments, "_bbox_local": box,
        })
    return objects


def _relations(objects: list[dict[str, Any]], anchors: list[dict[str, Any]], width: float, height: float, cap: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    relations = []; block_diag = math.hypot(width, height); tol = max(0.75, block_diag * 0.002); candidate_capped = False
    def add(kind: str, source: str, target: str, confidence: float, evidence: str) -> None:
        if len(relations) >= cap:
            return
        relations.append({"relation_id": f"rel_{len(relations)+1:06d}", "type": kind, "source_object": source, "target_object": target, "confidence": _round(confidence), "provenance": [{"source": "vector_geometry", "evidence": evidence}]})
    endpoints: dict[tuple[int,int], list[str]] = collections.defaultdict(list)
    for obj in objects:
        for segment in obj["_segments"]:
            for point in segment:
                key=(round(point[0]/tol),round(point[1]/tol)); endpoints[key].append(obj["object_id"])
    connected=set()
    for ids in endpoints.values():
        unique=sorted(set(ids))
        if len(unique) > 64:
            candidate_capped = True; unique = unique[:64]
        for i,left in enumerate(unique):
            for right in unique[i+1:]: connected.add((left,right))
    for left,right in sorted(connected): add("CONNECTED_TO",left,right,0.98,"shared endpoint within isotropic tolerance")
    # Containment, adjacency and alignment are addressing relations, never a
    # block score.  Candidate generation is spatially indexed and explicitly
    # bounded so a dense plan cannot trigger quadratic relation formation.
    containers = sorted(objects, key=lambda obj: _bbox_diag(obj["_bbox_local"]), reverse=True)
    if len(containers) > 64:
        candidate_capped = True; containers = containers[:64]
    for target in objects:
        tc = _center(target["_bbox_local"])
        for container in containers:
            if container is target:
                continue
            box = container["_bbox_local"]
            if box[0] <= tc[0] <= box[2] and box[1] <= tc[1] <= box[3] and _bbox_diag(box) > _bbox_diag(target["_bbox_local"]) * 1.2:
                add("CONTAINS",container["object_id"],target["object_id"],0.9,"target center inside one of 64 largest local bboxes")
    cell_size=max(1.0,block_diag*.015); spatial:dict[tuple[int,int],list[dict[str,Any]]]=collections.defaultdict(list)
    axis_x:dict[int,list[dict[str,Any]]]=collections.defaultdict(list); axis_y:dict[int,list[dict[str,Any]]]=collections.defaultdict(list)
    seen_pairs:set[tuple[str,str,str]]=set()
    for obj in objects:
        box=obj["_bbox_local"]; cx,cy=_center(box); key=(int(cx/cell_size),int(cy/cell_size))
        neighbors=[]
        for gx in range(key[0]-1,key[0]+2):
            for gy in range(key[1]-1,key[1]+2): neighbors.extend(spatial.get((gx,gy),[])[-64:])
        for other in neighbors:
            pair=tuple(sorted((obj["object_id"],other["object_id"])))
            if _bbox_gap(box,other["_bbox_local"]) <= block_diag*.01 and ("ADJACENT_TO",*pair) not in seen_pairs:
                add("ADJACENT_TO",pair[0],pair[1],0.75,"spatial-index bbox gap <=1% block diagonal");seen_pairs.add(("ADJACENT_TO",*pair))
        spatial[key].append(obj)
        for kind,bucket,bucket_key in (("x",axis_x,round(cx/max(block_diag*.003,1e-9))),("y",axis_y,round(cy/max(block_diag*.003,1e-9)))):
            previous=bucket[bucket_key]
            if len(previous)>64: candidate_capped=True
            for other in previous[-64:]:
                pair=tuple(sorted((obj["object_id"],other["object_id"])))
                if ("ALIGNED_WITH",*pair) not in seen_pairs:
                    add("ALIGNED_WITH",pair[0],pair[1],0.72,f"spatial-index {kind}-center alignment");seen_pairs.add(("ALIGNED_WITH",*pair))
            previous.append(obj)
    families: dict[str,list[dict[str,Any]]]=collections.defaultdict(list)
    for obj in objects: families[obj["family_signature"]].append(obj)
    object_families=[]
    for signature,members in sorted(families.items()):
        if len(members)<2: continue
        family_id=f"family_{len(object_families)+1:04d}"; ids=[m["object_id"] for m in members]
        object_families.append({"family_id":family_id,"family_signature":signature,"object_ids":ids,"count":len(ids),"provenance":"generic shape/topology signature"})
        for member in members[1:]: add("REPEATS_WITH",members[0]["object_id"],member["object_id"],0.88,"same generic family signature")
    # Prepared text is only an address hint.  It never becomes a change event.
    for anchor in anchors:
        box=anchor["bbox_norm"]; center=( ((box[0]+box[2])/2)*width, ((box[1]+box[3])/2)*height )
        if not objects: continue
        nearest=min(objects,key=lambda obj:_distance(center,_center(obj["_bbox_local"])))
        if _distance(center,_center(nearest["_bbox_local"])) <= block_diag*.12:
            nearest["label_anchor_ids"].append(anchor["anchor_id"])
            add("LABEL_ANCHOR",nearest["object_id"],anchor["anchor_id"],0.8,"existing prepared text metadata nearest object")
    relation_map=collections.defaultdict(list)
    for relation in relations: relation_map[relation["source_object"]].append(relation["relation_id"]); relation_map[relation["target_object"]].append(relation["relation_id"])
    for obj in objects: obj["relation_ids"]=relation_map[obj["object_id"]]
    return relations,object_families,len(relations)>=cap or candidate_capped


def build_graphic_block_description(block: dict[str, Any], cache: PageDrawingCache, *, segment_cap: int = SEGMENT_CAP) -> dict[str, Any]:
    if block["graphic_applicability"] == "GRAPHIC_NOT_APPLICABLE":
        return {"schema_version":DESCRIPTION_SCHEMA,"research_only":True,"input":public_input_contract(block),"quality":{"status":"GRAPHIC_NOT_APPLICABLE","reasons":[f"upstream block_type={block['block_type']}"]},"objects":[],"object_families":[],"relations":[],"visible_geometry_summary":{},"uncertainties":[]}
    payload=cache.get(block["source_pdf_path"],block["page_index"])
    atoms,extraction=_extract_atoms(payload,block,segment_cap)
    coords=block["coords_norm"]; page_rect=payload["page_rect"]; width=(coords[2]-coords[0])*(page_rect[2]-page_rect[0]); height=(coords[3]-coords[1])*(page_rect[3]-page_rect[1])
    groups,grouping=_group_atoms(atoms,math.hypot(width,height)); objects=_build_objects(groups,width,height)
    relations,families,relations_capped=_relations(objects,block["prepared_text_metadata"],width,height,RELATION_CAP)
    insufficient=sum(obj["geometry"]["segment_count"] for obj in objects)<3
    uncertainties=[]
    if extraction["segments_capped"]: uncertainties.append("spatial_segment_cap")
    if grouping["symbol_candidate_cap"]: uncertainties.append("symbol_group_candidate_cap")
    if relations_capped: uncertainties.append("relations_cap")
    if not block["prepared_text_metadata"]: uncertainties.append("prepared_label_metadata_unavailable")
    if insufficient: uncertainties.append("vector_layer_insufficient")
    status="VECTOR_DATA_INSUFFICIENT" if insufficient else ("OBJECT_FORMATION_PARTIAL" if extraction["segments_capped"] or grouping["symbol_candidate_cap"] or relations_capped else "OBJECT_FORMATION_COMPLETE")
    for obj in objects:
        obj.pop("_segments",None); obj.pop("_bbox_local",None)
    return {
        "schema_version":DESCRIPTION_SCHEMA,"research_only":True,"input":public_input_contract(block),
        "quality":{"status":status,"extraction_reliable":not insufficient,"bbox_reliable":True,"object_formation_complete":status=="OBJECT_FORMATION_COMPLETE","dangerous_cap":extraction["segments_capped"] or grouping["symbol_candidate_cap"] or relations_capped},
        "objects":objects,"object_families":families,"relations":relations,
        "visible_geometry_summary":{"objects":len(objects),"object_types":dict(sorted(collections.Counter(obj["type"] for obj in objects).items())),"segments":sum(obj["geometry"]["segment_count"] for obj in objects),"extraction":extraction,"grouping":grouping,"relations_capped":relations_capped,"normalization":"positions use bbox axes; angles/lengths/object shape use one isotropic block-diagonal scale"},
        "uncertainties":uncertainties,
    }


__all__=["build_graphic_block_description","DESCRIPTION_SCHEMA","SEGMENT_CAP"]
