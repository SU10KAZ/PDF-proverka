#!/usr/bin/env python3
"""VectorBlockDescription v0.2 proof of concept: graphical objects, relations, change ledger.

This is the orchestrator's own PoC for the contract proposed in
OPUS_VECTOR_ARCHITECTURE_REPORT.md.  It is deliberately small and generic: no discipline knows
about "автомат", "дверь" or "стояк" anywhere in this file.  Everything is derived from the PDF
vector and text layers.

Layers produced:

    L1  normalized strokes + text runs      (cache, never sent to a model)
    L2  graphical objects + relation graph  (the new layer this research argues for)
    L3  deterministic change ledger         (object-level, the unit the expert reads)
    L4  compact AI payload                  (only what changed, with uncertainty)

Run from the repository root:
    python -m experiments.stage_comparison_vector_architecture_opus.poc.objects_v02 \
        --pair eom_singleline_changed
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import io
import json
import math
import re
from pathlib import Path
from typing import Any, Iterable, Sequence

import fitz

try:
    from fontTools.pens.recordingPen import DecomposingRecordingPen
    from fontTools.ttLib import TTFont

    FONTTOOLS = True
except Exception:  # pragma: no cover - optional dependency
    FONTTOOLS = False

SCHEMA_VERSION = "vector-block-objects-research-v0.2"
CURVE_STEPS = 6

BENCHMARK = Path("experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json")
OUTPUT = Path("experiments/stage_comparison_vector_architecture_opus/artifacts/v02")


# --------------------------------------------------------------------------------------
# L1: strokes and text runs
# --------------------------------------------------------------------------------------


def _distance(a: Sequence[float], b: Sequence[float]) -> float:
    return math.hypot(float(a[0]) - float(b[0]), float(a[1]) - float(b[1]))


def _clip(start, end, rect):
    """Liang-Barsky clip of one segment to the block rectangle."""
    x0, y0, x1, y1 = float(start[0]), float(start[1]), float(end[0]), float(end[1])
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
    return (x0 + low * dx, y0 + low * dy), (x0 + high * dx, y0 + high * dy)


def _sample_cubic(item, steps: int = CURVE_STEPS):
    p0, p1, p2, p3 = ((float(item[i].x), float(item[i].y)) for i in range(1, 5))
    for index in range(steps + 1):
        t = index / steps
        u = 1.0 - t
        yield (
            u**3 * p0[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t**3 * p3[0],
            u**3 * p0[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t**3 * p3[1],
        )


def strokes(page: fitz.Page, rect: Sequence[float]) -> list[dict[str, Any]]:
    """Flat, normalized segment list with the style facts that matter."""
    width = max(rect[2] - rect[0], 1e-9)
    height = max(rect[3] - rect[1], 1e-9)
    # Isotropic normalisation: one scale for both axes.  Dividing x by width and y by height
    # independently would distort every angle and shape whenever the two paired crops have
    # different aspect ratios (orchestrator finding O10).
    scale = max(width, height)
    result: list[dict[str, Any]] = []
    for drawing_index, drawing in enumerate(page.get_drawings()):
        box = drawing.get("rect")
        if box is None:
            continue
        if box.x1 < rect[0] or box.x0 > rect[2] or box.y1 < rect[1] or box.y0 > rect[3]:
            continue
        fill = drawing.get("fill")
        stroke_color = drawing.get("color")
        stroke_width = float(drawing.get("width") or 0.0)
        raw: list[tuple[tuple[float, float], tuple[float, float]]] = []
        for item in drawing.get("items") or []:
            kind = item[0]
            if kind == "l":
                raw.append(((float(item[1].x), float(item[1].y)), (float(item[2].x), float(item[2].y))))
            elif kind == "re":
                r = item[1]
                corners = [
                    (float(r.x0), float(r.y0)),
                    (float(r.x1), float(r.y0)),
                    (float(r.x1), float(r.y1)),
                    (float(r.x0), float(r.y1)),
                ]
                raw.extend(zip(corners, corners[1:] + corners[:1]))
            elif kind == "qu":
                q = item[1]
                corners = [
                    (float(q.ul.x), float(q.ul.y)),
                    (float(q.ur.x), float(q.ur.y)),
                    (float(q.lr.x), float(q.lr.y)),
                    (float(q.ll.x), float(q.ll.y)),
                ]
                raw.extend(zip(corners, corners[1:] + corners[:1]))
            elif kind == "c":
                points = list(_sample_cubic(item))
                raw.extend(zip(points, points[1:]))
        for start, end in raw:
            clipped = _clip(start, end, rect)
            if clipped is None:
                continue
            (ax, ay), (bx, by) = clipped
            nx0, ny0 = (ax - rect[0]) / scale, (ay - rect[1]) / scale
            nx1, ny1 = (bx - rect[0]) / scale, (by - rect[1]) / scale
            length = math.hypot(nx1 - nx0, ny1 - ny0)
            if length <= 1e-7:
                continue
            result.append(
                {
                    "p1": [round(nx0, 6), round(ny0, 6)],
                    "p2": [round(nx1, 6), round(ny1, 6)],
                    "length": round(length, 6),
                    "angle": round(math.degrees(math.atan2(ny1 - ny0, nx1 - nx0)) % 180, 2),
                    "drawing": drawing_index,
                    "filled": fill is not None,
                    "stroke_width": round(stroke_width, 3),
                    "color": tuple(round(c, 2) for c in (stroke_color or fill or ())),
                }
            )
    return result


def _glyph_table(document: fitz.Document, page: fitz.Page) -> dict[str, dict[int, str]]:
    """Canonical glyph-outline hashes, so subset fonts with a broken /ToUnicode stay comparable."""
    if not FONTTOOLS:
        return {}
    tables: dict[str, dict[int, str]] = {}
    for entry in page.get_fonts(full=True):
        try:
            _, extension, _, buffer = document.extract_font(entry[0])
        except Exception:
            continue
        if not buffer or extension != "ttf":
            continue
        try:
            font = TTFont(io.BytesIO(buffer))
            glyph_set = font.getGlyphSet()
            upem = float(font["head"].unitsPerEm)
        except Exception:
            continue
        cmap: dict[int, str] = {}
        for table in font["cmap"].tables if "cmap" in font else []:
            if table.platformID == 1:
                cmap.update(table.cmap)
        if not cmap:
            for table in font["cmap"].tables if "cmap" in font else []:
                if table.platformID == 3 and table.platEncID == 0:
                    cmap.update({code & 0xFF: name for code, name in table.cmap.items()})
        codes: dict[int, str] = {}
        for code, name in cmap.items():
            if name not in glyph_set:
                continue
            pen = DecomposingRecordingPen(glyph_set)
            try:
                glyph_set[name].draw(pen)
            except Exception:
                continue

            def q(value):
                if isinstance(value, (int, float)):
                    return round(float(value) / upem, 3)
                if isinstance(value, (tuple, list)):
                    return tuple(q(item) for item in value)
                return value

            codes[code] = hashlib.sha1(
                repr([(op, q(args)) for op, args in pen.value]).encode("utf-8")
            ).hexdigest()[:8]
        tables[str(entry[3]).split("+", 1)[-1]] = codes
    return tables


_NUMBER = re.compile(r"^[+\-−]?\d+(?:[.,]\d+)?$")


def text_runs(document: fitz.Document, page: fitz.Page, rect: Sequence[float]) -> list[dict[str, Any]]:
    """Text merged into runs at line level, with a per-span readability verdict."""
    width = max(rect[2] - rect[0], 1e-9)
    height = max(rect[3] - rect[1], 1e-9)
    scale = max(width, height)
    tables = _glyph_table(document, page)
    runs: list[dict[str, Any]] = []
    data = page.get_text("dict", clip=fitz.Rect(*rect))
    for block in data.get("blocks") or []:
        if block.get("type") != 0:
            continue
        for line in block.get("lines") or []:
            direction = line.get("dir") or (1.0, 0.0)
            rotation = round(math.degrees(math.atan2(float(direction[1]), float(direction[0]))), 1)
            pieces, canonical, boxes, sizes = [], [], [], []
            for span in line.get("spans") or []:
                raw = str(span.get("text") or "")
                if not raw.strip():
                    continue
                font = str(span.get("font") or "").split("+", 1)[-1]
                table = tables.get(font, {})
                pieces.append(raw)
                canonical.append(
                    "".join(
                        " " if ord(c) == 32 else table.get(ord(c), f"?{ord(c):02x}") for c in raw
                    )
                )
                boxes.append([float(v) for v in span.get("bbox")])
                sizes.append(float(span.get("size") or 0.0))
            if not pieces:
                continue
            text = re.sub(r"\s+", " ", "".join(pieces)).strip()
            bbox = [
                min(b[0] for b in boxes),
                min(b[1] for b in boxes),
                max(b[2] for b in boxes),
                max(b[3] for b in boxes),
            ]
            broken = sum(1 for c in text if ord(c) < 32 and not c.isspace())
            runs.append(
                {
                    "id": f"t{len(runs) + 1}",
                    "text": text,
                    # identity is what a diff must compare: readable text when it decodes,
                    # glyph-outline hashes when the CAD subset font has no /ToUnicode entry
                    "identity": text if broken == 0 else "".join(canonical),
                    "readable": broken == 0,
                    "numeric": bool(_NUMBER.match(text.replace(" ", ""))),
                    "bbox": [
                        round((bbox[0] - rect[0]) / scale, 5),
                        round((bbox[1] - rect[1]) / scale, 5),
                        round((bbox[2] - rect[0]) / scale, 5),
                        round((bbox[3] - rect[1]) / scale, 5),
                    ],
                    "rotation": rotation,
                    "font_size": round(sum(sizes) / len(sizes), 2),
                }
            )
    for run in runs:
        run["cx"] = round((run["bbox"][0] + run["bbox"][2]) / 2, 5)
        run["cy"] = round((run["bbox"][1] + run["bbox"][3]) / 2, 5)
    return runs


# --------------------------------------------------------------------------------------
# L2: graphical objects
# --------------------------------------------------------------------------------------


class _Union:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, index: int) -> int:
        while self.parent[index] != index:
            self.parent[index] = self.parent[self.parent[index]]
            index = self.parent[index]
        return index

    def union(self, left: int, right: int) -> None:
        left, right = self.find(left), self.find(right)
        if left != right:
            self.parent[right] = left


def _grid(items: Iterable[tuple[int, Sequence[float]]], cell: float):
    buckets: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for index, point in items:
        buckets[(int(point[0] // cell), int(point[1] // cell))].append(index)
    return buckets


def _shape_signature(members: Sequence[dict[str, Any]], rotation_invariant: bool = True) -> str:
    """Scale/translation (and optionally rotation/mirror) invariant signature of a stroke group."""
    xs = [p[0] for s in members for p in (s["p1"], s["p2"])]
    ys = [p[1] for s in members for p in (s["p1"], s["p2"])]
    x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
    w, h = max(x1 - x0, 1e-9), max(y1 - y0, 1e-9)
    scale = max(w, h)
    tokens = []
    for stroke in members:
        length = round(stroke["length"] / scale, 2)
        if rotation_invariant:
            tokens.append(("s", length))
        else:
            tokens.append(("s", length, round(stroke["angle"] / 15)))
    counter = collections.Counter(tokens)
    payload = (
        len(members),
        round(w / h, 1),
        tuple(sorted(counter.items())),
    )
    return hashlib.sha1(repr(payload).encode("utf-8")).hexdigest()[:10]


def _chains(strokes_list: list[dict[str, Any]], tolerance: float) -> list[list[int]]:
    """Merge strokes into maximal polyline chains through degree-2 endpoints.

    A drawing is not a bag of segments: connectors, wires, walls and contours are *runs*.  Peeling
    the runs off first is what stops a proximity clustering from chaining every symbol on a
    schematic together through the wiring that links them.
    """
    nodes: list[tuple[float, float]] = []
    buckets: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    cell = max(tolerance, 1e-6)

    def node_id(point: Sequence[float]) -> int:
        gx, gy = int(point[0] / cell), int(point[1] / cell)
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for candidate in buckets.get((gx + dx, gy + dy), ()):
                    if _distance(nodes[candidate], point) <= tolerance:
                        return candidate
        nodes.append((float(point[0]), float(point[1])))
        buckets[(gx, gy)].append(len(nodes) - 1)
        return len(nodes) - 1

    ends: list[tuple[int, int]] = []
    incident: dict[int, list[int]] = collections.defaultdict(list)
    for index, stroke in enumerate(strokes_list):
        a, b = node_id(stroke["p1"]), node_id(stroke["p2"])
        ends.append((a, b))
        incident[a].append(index)
        incident[b].append(index)

    visited = [False] * len(strokes_list)
    chains: list[list[int]] = []
    for index in range(len(strokes_list)):
        if visited[index]:
            continue
        visited[index] = True
        chain = [index]
        for end_side in (0, 1):
            current_node = ends[index][end_side]
            while True:
                neighbours = [j for j in incident[current_node] if not visited[j]]
                # extend only through a clean degree-2 junction; a branch ends the run
                if len(incident[current_node]) != 2 or len(neighbours) != 1:
                    break
                nxt = neighbours[0]
                visited[nxt] = True
                if end_side == 0:
                    chain.insert(0, nxt)
                else:
                    chain.append(nxt)
                current_node = ends[nxt][1] if ends[nxt][0] == current_node else ends[nxt][0]
        chains.append(chain)
    return chains


def build_objects(
    strokes_list: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    shared_scale: float | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Cluster strokes into generic graphical objects.  No discipline semantics."""
    if not strokes_list:
        return [], {"stats": {"stroke_count": 0}, "repeated": [], "note": "no vector geometry"}

    lengths = sorted(s["length"] for s in strokes_list)
    # The clustering scale must be identical on both sides of a pair, otherwise the two
    # descriptions are built in different metric spaces and are not comparable at all.
    median = shared_scale if shared_scale is not None else lengths[len(lengths) // 2]
    weld = max(median * 0.4, 0.0004)

    chains = _chains(strokes_list, weld)
    units = []
    for chain in chains:
        parts = [strokes_list[i] for i in chain]
        xs = [p[0] for s in parts for p in (s["p1"], s["p2"])]
        ys = [p[1] for s in parts for p in (s["p1"], s["p2"])]
        bbox = (min(xs), min(ys), max(xs), max(ys))
        total = sum(s["length"] for s in parts)
        diagonal = math.hypot(bbox[2] - bbox[0], bbox[3] - bbox[1])
        units.append(
            {
                "members": chain,
                "parts": parts,
                "bbox": bbox,
                "total_length": total,
                "diagonal": diagonal,
                "straightness": diagonal / max(total, 1e-9),
            }
        )

    span = max(
        max(u["bbox"][2] for u in units) - min(u["bbox"][0] for u in units),
        max(u["bbox"][3] for u in units) - min(u["bbox"][1] for u in units),
        1e-9,
    )
    # A connector/linear object is a long run that actually goes somewhere.  Both tests are
    # relative to the block, so nothing here depends on the CAD exporter's segment granularity.
    long_threshold = 0.08 * span

    linear_units = [u for u in units if u["total_length"] >= long_threshold and u["straightness"] >= 0.3]
    other_units = [u for u in units if u not in linear_units]

    # Symbol candidates: single-linkage clustering of the remaining units.
    radius = max(0.012 * span, median * 1.5)
    union = _Union(len(other_units))
    grid: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for index, unit in enumerate(other_units):
        gx0, gy0 = int(unit["bbox"][0] / radius), int(unit["bbox"][1] / radius)
        gx1, gy1 = int(unit["bbox"][2] / radius), int(unit["bbox"][3] / radius)
        for gx in range(gx0, gx1 + 1):
            for gy in range(gy0, gy1 + 1):
                grid[(gx, gy)].append(index)
    for members in grid.values():
        for offset, a in enumerate(members):
            for b in members[offset + 1 :]:
                ba, bb = other_units[a]["bbox"], other_units[b]["bbox"]
                gap_x = max(0.0, max(ba[0] - bb[2], bb[0] - ba[2]))
                gap_y = max(0.0, max(ba[1] - bb[3], bb[1] - ba[3]))
                if math.hypot(gap_x, gap_y) <= radius:
                    union.union(a, b)

    groups: dict[int, list[int]] = collections.defaultdict(list)
    for index in range(len(other_units)):
        groups[union.find(index)].append(index)

    objects: list[dict[str, Any]] = []
    for members in groups.values():
        parts = [part for index in members for part in other_units[index]["parts"]]
        xs = [p[0] for s in parts for p in (s["p1"], s["p2"])]
        ys = [p[1] for s in parts for p in (s["p1"], s["p2"])]
        bbox = [round(min(xs), 5), round(min(ys), 5), round(max(xs), 5), round(max(ys), 5)]
        objects.append(
            {
                "id": "",
                "class": "symbol_candidate" if len(parts) >= 3 else "mark",
                "stroke_count": len(parts),
                "bbox": bbox,
                "cx": round((bbox[0] + bbox[2]) / 2, 5),
                "cy": round((bbox[1] + bbox[3]) / 2, 5),
                "extent": round(max(bbox[2] - bbox[0], bbox[3] - bbox[1]), 5),
                "filled": any(s["filled"] for s in parts),
                "signature": _shape_signature(parts),
                "signature_oriented": _shape_signature(parts, rotation_invariant=False),
            }
        )

    for unit in linear_units:
        bbox = [round(v, 5) for v in unit["bbox"]]
        objects.append(
            {
                "id": "",
                "class": "linear_object",
                "stroke_count": len(unit["parts"]),
                "bbox": bbox,
                "cx": round((bbox[0] + bbox[2]) / 2, 5),
                "cy": round((bbox[1] + bbox[3]) / 2, 5),
                "extent": round(unit["total_length"], 5),
                "filled": any(s["filled"] for s in unit["parts"]),
                "straightness": round(unit["straightness"], 3),
                "signature": f"lin{round(unit['total_length'] / span, 2)}/{round(unit['straightness'], 1)}",
                "signature_oriented": f"lin{round(unit['total_length'] / span, 2)}/{round(unit['straightness'], 1)}",
            }
        )

    objects.sort(key=lambda item: (item["cy"], item["cx"]))
    for index, obj in enumerate(objects, 1):
        obj["id"] = f"o{index}"

    families: dict[str, list[str]] = collections.defaultdict(list)
    for obj in objects:
        if obj["class"] in {"symbol_candidate", "mark"}:
            families[obj["signature"]].append(obj["id"])
    repeated = [
        {"family": key, "count": len(value), "members": value}
        for key, value in sorted(families.items(), key=lambda kv: -len(kv[1]))
        if len(value) >= 2
    ]
    by_id = {o["id"]: o for o in objects}
    for family in repeated:
        for member in family["members"]:
            by_id[member]["family"] = family["family"]

    stats = {
        "stroke_count": len(strokes_list),
        "own_median_stroke_length": round(lengths[len(lengths) // 2], 6),
        "scale_used": round(median, 6),
        "chains": len(chains),
        "long_run_threshold": round(long_threshold, 5),
        "cluster_radius": round(radius, 5),
        "symbol_candidates": sum(1 for o in objects if o["class"] == "symbol_candidate"),
        "marks": sum(1 for o in objects if o["class"] == "mark"),
        "linear_objects": sum(1 for o in objects if o["class"] == "linear_object"),
        "repeated_families": len(repeated),
        "largest_family": repeated[0]["count"] if repeated else 0,
    }
    return objects, {"stats": stats, "repeated": repeated[:50]}


# --------------------------------------------------------------------------------------
# L2: relations
# --------------------------------------------------------------------------------------


def build_relations(
    objects: list[dict[str, Any]], runs: list[dict[str, Any]], strokes_list: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    relations: list[dict[str, Any]] = []
    symbols = [o for o in objects if o["class"] in {"symbol_candidate", "mark"}]
    lines = [o for o in objects if o["class"] == "linear_object"]

    # 1. text inside an object bbox -> centered_inside (strongest binding, no ambiguity)
    for run in runs:
        inside = [
            o
            for o in symbols
            if o["bbox"][0] <= run["cx"] <= o["bbox"][2] and o["bbox"][1] <= run["cy"] <= o["bbox"][3]
        ]
        if len(inside) == 1:
            relations.append(
                {"type": "labels", "evidence": "centered_inside", "text": run["id"], "object": inside[0]["id"]}
            )
        elif len(inside) > 1:
            smallest = min(inside, key=lambda o: (o["bbox"][2] - o["bbox"][0]) * (o["bbox"][3] - o["bbox"][1]))
            relations.append(
                {"type": "labels", "evidence": "innermost_enclosure", "text": run["id"], "object": smallest["id"]}
            )

    bound = {relation["text"] for relation in relations}

    # 2. nearest object, but only when it wins by a clear margin - otherwise the binding is
    #    honestly reported as ambiguous instead of being asserted with high confidence.
    for run in runs:
        if run["id"] in bound:
            continue
        ranked = sorted(
            ((math.hypot(o["cx"] - run["cx"], o["cy"] - run["cy"]), o) for o in symbols),
            key=lambda row: row[0],
        )[:3]
        if not ranked:
            relations.append({"type": "labels", "evidence": "unbound", "text": run["id"], "object": None})
            continue
        best_distance, best = ranked[0]
        runner_up = ranked[1][0] if len(ranked) > 1 else float("inf")
        if best_distance > 0.06:
            relations.append({"type": "labels", "evidence": "unbound", "text": run["id"], "object": None})
        elif runner_up < best_distance * 1.6:
            relations.append(
                {
                    "type": "labels",
                    "evidence": "ambiguous_proximity",
                    "text": run["id"],
                    "object": best["id"],
                    "rivals": [row[1]["id"] for row in ranked[1:]],
                }
            )
        else:
            relations.append(
                {"type": "labels", "evidence": "unique_proximity", "text": run["id"], "object": best["id"]}
            )

    # 3. connectivity: a symbol touching a long run
    tolerance = 0.004
    for symbol in symbols:
        for line in lines:
            if (
                symbol["bbox"][0] - tolerance <= line["bbox"][2]
                and symbol["bbox"][2] + tolerance >= line["bbox"][0]
                and symbol["bbox"][1] - tolerance <= line["bbox"][3]
                and symbol["bbox"][3] + tolerance >= line["bbox"][1]
            ):
                relations.append({"type": "touches", "object": symbol["id"], "other": line["id"]})

    # 4. containment between objects
    for outer in objects:
        area = (outer["bbox"][2] - outer["bbox"][0]) * (outer["bbox"][3] - outer["bbox"][1])
        if area < 0.01:
            continue
        for inner in symbols:
            if inner["id"] == outer["id"]:
                continue
            if (
                outer["bbox"][0] < inner["bbox"][0]
                and outer["bbox"][1] < inner["bbox"][1]
                and outer["bbox"][2] > inner["bbox"][2]
                and outer["bbox"][3] > inner["bbox"][3]
            ):
                relations.append({"type": "contains", "object": outer["id"], "other": inner["id"]})
    return relations


# --------------------------------------------------------------------------------------
# alignment + change ledger
# --------------------------------------------------------------------------------------


def estimate_alignment(
    left: list[dict[str, Any]], right: list[dict[str, Any]]
) -> tuple[tuple[float, float], tuple[float, float], float]:
    """Four-parameter fit (translation + per-axis scale) from object centres."""
    if not left or not right:
        return (0.0, 0.0), (1.0, 1.0), 0.0
    sample = sorted(left, key=lambda o: -o["extent"])[:300]
    target = sorted(right, key=lambda o: -o["extent"])[:300]
    grid: dict[tuple[int, int], list[dict[str, Any]]] = collections.defaultdict(list)
    for t in target:
        grid[(int(t["cx"] / 0.01), int(t["cy"] / 0.01))].append(t)

    def score(tx: float, ty: float, s: float, radius: float) -> float:
        hit = 0
        for o in sample:
            cx, cy = o["cx"] * s + tx, o["cy"] * s + ty
            found = False
            gx, gy = int(cx / 0.01), int(cy / 0.01)
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    for t in grid.get((gx + dx, gy + dy), ()):
                        if abs(t["cx"] - cx) < radius and abs(t["cy"] - cy) < radius:
                            found = True
                            break
                    if found:
                        break
                if found:
                    break
            hit += 1 if found else 0
        return hit / max(len(sample), 1)

    # Coarse pass over a wide uniform-scale range, then a fine pass around the winner.
    # Seed with the identity so that ties never invent a transform: on an identical pair the
    # search would otherwise report the first equally-scoring candidate it happened to try.
    best = ((0.0, 0.0), 1.0, score(0.0, 0.0, 1.0, 0.008))
    for s in [0.85 + i * 0.01 for i in range(31)]:
        for tx in [i * 0.01 for i in range(-12, 13)]:
            for ty in [i * 0.01 for i in range(-12, 13)]:
                value = score(tx, ty, s, 0.008)
                if value > best[2]:
                    best = ((tx, ty), s, value)
    (tx0, ty0), s0, _ = best
    best = ((0.0, 0.0), 1.0, score(0.0, 0.0, 1.0, 0.003))
    for s in [s0 + i * 0.002 for i in range(-5, 6)]:
        for tx in [tx0 + i * 0.001 for i in range(-8, 9)]:
            for ty in [ty0 + i * 0.001 for i in range(-8, 9)]:
                value = score(tx, ty, s, 0.003)
                if value > best[2]:
                    best = ((tx, ty), s, value)
    (tx, ty), s, fit = best
    return (tx, ty), (s, s), fit


def match_objects(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    shift: tuple[float, float],
    scale: tuple[float, float],
    radius: float = 0.01,
) -> tuple[list[tuple[str, str]], list[str], list[str]]:
    proposals = []
    for a in left:
        ax, ay = a["cx"] * scale[0] + shift[0], a["cy"] * scale[1] + shift[1]
        for b in right:
            if a["class"] != b["class"]:
                continue
            distance = math.hypot(b["cx"] - ax, b["cy"] - ay)
            if distance > radius:
                continue
            same_shape = a["signature"] == b["signature"]
            proposals.append((distance - (0.005 if same_shape else 0.0), a["id"], b["id"]))
    used_left, used_right, pairs = set(), set(), []
    for _, a_id, b_id in sorted(proposals):
        if a_id in used_left or b_id in used_right:
            continue
        used_left.add(a_id)
        used_right.add(b_id)
        pairs.append((a_id, b_id))
    return (
        pairs,
        [o["id"] for o in left if o["id"] not in used_left],
        [o["id"] for o in right if o["id"] not in used_right],
    )


def change_ledger(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    shift, scale, fit = estimate_alignment(left["objects"], right["objects"])
    pairs, only_left, only_right = match_objects(left["objects"], right["objects"], shift, scale)
    left_by_id = {o["id"]: o for o in left["objects"]}
    right_by_id = {o["id"]: o for o in right["objects"]}

    changes: list[dict[str, Any]] = []

    # 1. repeated-family counts: the sentence "12 -> 14 devices" lives here
    left_families = {f["family"]: f["count"] for f in left["repeated"]}
    right_families = {f["family"]: f["count"] for f in right["repeated"]}
    for family in sorted(set(left_families) | set(right_families)):
        a, b = left_families.get(family, 0), right_families.get(family, 0)
        if a != b and max(a, b) >= 2:
            example = next(
                (o for o in (left["objects"] + right["objects"]) if o.get("family") == family), None
            )
            changes.append(
                {
                    "kind": "repeated_group_count",
                    "family": family,
                    "before": a,
                    "after": b,
                    "example_bbox": example["bbox"] if example else None,
                    "confidence": "high" if min(a, b) > 0 else "medium",
                }
            )

    # 2. objects present on one side only
    if only_left or only_right:
        changes.append(
            {
                "kind": "objects_only_on_one_side",
                "removed": len(only_left),
                "added": len(only_right),
                "removed_sample": [
                    {"id": i, "bbox": left_by_id[i]["bbox"], "class": left_by_id[i]["class"]}
                    for i in only_left[:8]
                ],
                "added_sample": [
                    {"id": i, "bbox": right_by_id[i]["bbox"], "class": right_by_id[i]["class"]}
                    for i in only_right[:8]
                ],
                "confidence": "high" if fit > 0.6 else "low",
            }
        )

    # 3. text identity changes, reported per bound object where possible
    def label_map(side: dict[str, Any]) -> dict[str, list[str]]:
        runs = {r["id"]: r for r in side["texts"]}
        mapping: dict[str, list[str]] = collections.defaultdict(list)
        for relation in side["relations"]:
            if relation["type"] == "labels" and relation.get("object"):
                mapping[relation["object"]].append(runs[relation["text"]]["identity"])
        return mapping

    left_labels, right_labels = label_map(left), label_map(right)
    for a_id, b_id in pairs:
        before = sorted(left_labels.get(a_id, []))
        after = sorted(right_labels.get(b_id, []))
        if before != after:
            changes.append(
                {
                    "kind": "object_label_changed",
                    "object_before": a_id,
                    "object_after": b_id,
                    "bbox": right_by_id[b_id]["bbox"],
                    "before": before,
                    "after": after,
                    "readable": all(
                        r["readable"] for r in left["texts"] + right["texts"] if r["identity"] in before + after
                    ),
                    "confidence": "high",
                }
            )

    # 4. free text that changed without an object binding
    def bound_text_ids(side: dict[str, Any]) -> set[str]:
        return {
            relation["text"]
            for relation in side["relations"]
            if relation["type"] == "labels" and relation.get("object")
        }

    left_bound, right_bound = bound_text_ids(left), bound_text_ids(right)
    left_free = collections.Counter(r["identity"] for r in left["texts"] if r["id"] not in left_bound)
    right_free = collections.Counter(r["identity"] for r in right["texts"] if r["id"] not in right_bound)
    removed, added = left_free - right_free, right_free - left_free
    if removed or added:
        changes.append(
            {
                "kind": "unbound_text_changed",
                "removed": list(removed.elements())[:20],
                "added": list(added.elements())[:20],
                "confidence": "medium",
            }
        )

    quality = {
        "alignment_fit": round(fit, 3),
        "alignment_shift": [round(v, 4) for v in shift],
        "alignment_scale": [round(v, 4) for v in scale],
        "matched_objects": len(pairs),
        "left_objects": len(left["objects"]),
        "right_objects": len(right["objects"]),
        "left_text_readable_ratio": round(
            sum(1 for r in left["texts"] if r["readable"]) / max(len(left["texts"]), 1), 3
        ),
        "right_text_readable_ratio": round(
            sum(1 for r in right["texts"] if r["readable"]) / max(len(right["texts"]), 1), 3
        ),
    }
    return {"schema_version": SCHEMA_VERSION, "quality": quality, "changes": changes}


# --------------------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------------------


def describe(
    pdf: str,
    page_index: int,
    bbox_norm: Sequence[float],
    block_id: str,
    shared_scale: float | None = None,
) -> dict[str, Any]:
    document = fitz.open(pdf)
    page = document[page_index]
    rect = [
        bbox_norm[0] * page.rect.width,
        bbox_norm[1] * page.rect.height,
        bbox_norm[2] * page.rect.width,
        bbox_norm[3] * page.rect.height,
    ]
    stroke_list = strokes(page, rect)
    runs = text_runs(document, page, rect)
    objects, grouping = build_objects(stroke_list, runs, shared_scale=shared_scale)
    relations = build_relations(objects, runs, stroke_list)
    document.close()
    return {
        "schema_version": SCHEMA_VERSION,
        "block_id": block_id,
        "source": {"pdf": pdf, "page_index": page_index, "bbox_norm": list(bbox_norm)},
        "stats": grouping.get("stats", {}),
        "objects": objects,
        "repeated": grouping.get("repeated", []),
        "texts": runs,
        "relations": relations,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair", required=True)
    parser.add_argument("--output", default=str(OUTPUT))
    arguments = parser.parse_args()

    manifest = json.loads(BENCHMARK.read_text(encoding="utf-8"))
    pair = next(p for p in manifest["pairs"] if p["pair_id"] == arguments.pair)
    output = Path(arguments.output) / arguments.pair
    output.mkdir(parents=True, exist_ok=True)

    # Pass 1 measures each side's own stroke scale; pass 2 rebuilds both sides on the joint
    # scale so the two object layers live in the same metric space.
    probe = {
        side: describe(
            pair[side]["pdf"], pair[side]["page_index"], pair[side]["bbox_norm"], pair[side]["block_id"]
        )
        for side in ("left", "right")
    }
    joint_scale = min(probe[side]["stats"]["own_median_stroke_length"] for side in ("left", "right"))
    sides = {}
    for side in ("left", "right"):
        spec = pair[side]
        description = describe(
            spec["pdf"], spec["page_index"], spec["bbox_norm"], spec["block_id"], shared_scale=joint_scale
        )
        sides[side] = description
        (output / f"{side}.json").write_text(
            json.dumps(description, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(side, json.dumps(description["stats"], ensure_ascii=False))

    ledger = change_ledger(sides["left"], sides["right"])
    (output / "ledger.json").write_text(
        json.dumps(ledger, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("quality", json.dumps(ledger["quality"], ensure_ascii=False))
    for change in ledger["changes"]:
        print(" -", change["kind"], json.dumps({k: v for k, v in change.items() if k != "kind"}, ensure_ascii=False)[:220])
    print("written", output)


if __name__ == "__main__":
    main()
