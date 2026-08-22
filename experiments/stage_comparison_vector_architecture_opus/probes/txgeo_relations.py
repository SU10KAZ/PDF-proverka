#!/usr/bin/env python3
"""TXGEO probe — deterministic TEXT<->GEOMETRY relation types for VectorBlockDescription.

Replaces the v0.1 `nearest_geometry` anchor (orchestrator finding O3: its `confidence`
is inverted) with eight typed relations that are computed in RAW PDF point space
(the v0.1 `normalized` space destroys the aspect ratio, so angles there are wrong).

Pure geometry, no OCR / no ML.  Run from the repository root:

    python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_relations \
        --description <path/to/vector_block.json> --out <out.json>
"""
from __future__ import annotations

import argparse
import collections
import json
import math
import re
from pathlib import Path
from typing import Any, Iterable, Sequence

PROBE_VERSION = "txgeo-relations-v0.2-probe"

# ---------------------------------------------------------------------------
# small geometry helpers (raw PDF points, y grows downwards)
# ---------------------------------------------------------------------------


def _hypot(ax: float, ay: float, bx: float, by: float) -> float:
    return math.hypot(ax - bx, ay - by)


def _seg_angle(p1: Sequence[float], p2: Sequence[float]) -> float:
    """Angle of the segment in degrees, folded to [0, 180)."""
    ang = math.degrees(math.atan2(p2[1] - p1[1], p2[0] - p1[0])) % 180.0
    return ang


def _angle_delta(a: float, b: float) -> float:
    """Smallest difference between two undirected angles in degrees."""
    d = abs((a - b) % 180.0)
    return min(d, 180.0 - d)


def _point_segment_distance(point: Sequence[float], p1: Sequence[float], p2: Sequence[float]) -> float:
    px, py = point
    x1, y1 = p1
    x2, y2 = p2
    dx, dy = x2 - x1, y2 - y1
    if abs(dx) + abs(dy) < 1e-12:
        return math.hypot(px - x1, py - y1)
    t = max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (x1 + t * dx), py - (y1 + t * dy))


def _projection_param(point: Sequence[float], p1: Sequence[float], p2: Sequence[float]) -> float:
    px, py = point
    x1, y1 = p1
    x2, y2 = p2
    dx, dy = x2 - x1, y2 - y1
    denom = dx * dx + dy * dy
    if denom < 1e-12:
        return 0.0
    return ((px - x1) * dx + (py - y1) * dy) / denom


def _perp_distance_to_line(point: Sequence[float], p1: Sequence[float], p2: Sequence[float]) -> float:
    px, py = point
    x1, y1 = p1
    x2, y2 = p2
    dx, dy = x2 - x1, y2 - y1
    n = math.hypot(dx, dy)
    if n < 1e-12:
        return math.hypot(px - x1, py - y1)
    return abs(dy * (px - x1) - dx * (py - y1)) / n


def _polygon_area(points: Sequence[Sequence[float]]) -> float:
    area = 0.0
    for i in range(len(points)):
        x1, y1 = points[i]
        x2, y2 = points[(i + 1) % len(points)]
        area += x1 * y2 - x2 * y1
    return abs(area) / 2.0


def _point_in_polygon(point: Sequence[float], polygon: Sequence[Sequence[float]]) -> bool:
    x, y = point
    inside = False
    prev = polygon[-1]
    for cur in polygon:
        x1, y1 = prev
        x2, y2 = cur
        if (y1 > y) != (y2 > y):
            if x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-15) + x1:
                inside = not inside
        prev = cur
    return inside


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, a: int) -> int:
        while self.parent[a] != a:
            self.parent[a] = self.parent[self.parent[a]]
            a = self.parent[a]
        return a

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


class _Grid:
    """Uniform grid over segment indexes."""

    def __init__(self, cell: float) -> None:
        self.cell = max(cell, 1e-6)
        self.buckets: dict[tuple[int, int], list[int]] = collections.defaultdict(list)

    def _key(self, x: float, y: float) -> tuple[int, int]:
        return (int(math.floor(x / self.cell)), int(math.floor(y / self.cell)))

    def insert_segment(self, index: int, p1: Sequence[float], p2: Sequence[float]) -> None:
        length = _hypot(p1[0], p1[1], p2[0], p2[1])
        steps = int(length / (self.cell * 0.5)) + 1
        steps = min(steps, 4000)
        seen: set[tuple[int, int]] = set()
        for i in range(steps + 1):
            t = i / steps
            key = self._key(p1[0] + (p2[0] - p1[0]) * t, p1[1] + (p2[1] - p1[1]) * t)
            if key in seen:
                continue
            seen.add(key)
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    self.buckets[(key[0] + dx, key[1] + dy)].append(index)

    def query_point(self, x: float, y: float) -> list[int]:
        return self.buckets.get(self._key(x, y), [])

    def query_rect(self, x0: float, y0: float, x1: float, y1: float) -> set[int]:
        out: set[int] = set()
        kx0, ky0 = self._key(x0, y0)
        kx1, ky1 = self._key(x1, y1)
        for gx in range(kx0, kx1 + 1):
            for gy in range(ky0, ky1 + 1):
                out.update(self.buckets.get((gx, gy), ()))
        return out


# ---------------------------------------------------------------------------
# model built from a v0.1 description
# ---------------------------------------------------------------------------

_NUM_RE = re.compile(r"[-+−]?\d+(?:[.,]\d+)?")
# A dimension value is a bare number, optionally with Ø / R / a unit suffix.  A leading
# sign means an elevation mark (отметка), which is a different relation entirely.
_DIM_TEXT_RE = re.compile(r"^(?:Ø|ø|R)?\s*\d{1,5}(?:[.,]\d+)?\s*(?:мм|mm|м|m)?$")


class BlockModel:
    def __init__(self, description: dict[str, Any], unit_mode: str = "span") -> None:
        self.description = description
        self.unit_mode = unit_mode
        self.prims = description["geometry"]["primitives"]
        self._build_segments()
        self._build_units()
        self._build_scale()
        self._build_index()
        self._build_chains()
        self._build_nodes_and_components()

    # -- geometry ---------------------------------------------------------
    def _build_segments(self) -> None:
        segs: list[dict[str, Any]] = []
        for prim in self.prims:
            raw = prim.get("raw") or {}
            for local, seg in enumerate(raw.get("segments", [])):
                p1, p2 = (float(seg[0][0]), float(seg[0][1])), (float(seg[1][0]), float(seg[1][1]))
                length = _hypot(p1[0], p1[1], p2[0], p2[1])
                if length < 1e-9:
                    continue
                segs.append(
                    {
                        "i": len(segs),
                        "prim": prim["id"],
                        "local": local,
                        "p1": p1,
                        "p2": p2,
                        "len": length,
                        "ang": _seg_angle(p1, p2),
                    }
                )
        self.segments = segs

    def _build_units(self) -> None:
        texts = [t for t in self.description["texts"] if (t.get("text") or "").strip()]
        if self.unit_mode == "span":
            self.units = [
                {
                    "id": t["id"],
                    "ids": [t["id"]],
                    "text": t["text"],
                    "bbox": [float(v) for v in t["bbox"]],
                    "rotation": float(t.get("rotation") or 0.0),
                    "font_size": float(t.get("font_size") or 0.0),
                    "category": t.get("category"),
                }
                for t in texts
            ]
            return
        # line mode: merge spans on the same baseline of the same rotation/size
        buckets: dict[tuple[int, int], list[dict[str, Any]]] = collections.defaultdict(list)
        for t in texts:
            rot = round(float(t.get("rotation") or 0.0) / 5.0)
            h = max(float(t["bbox"][3]) - float(t["bbox"][1]), 1e-6)
            if abs((float(t.get("rotation") or 0.0)) % 180.0) < 45 or abs((float(t.get("rotation") or 0.0)) % 180.0) > 135:
                key_axis = round(float(t["bbox"][1]) / max(h * 0.5, 0.5))
            else:
                key_axis = round(float(t["bbox"][0]) / max(h * 0.5, 0.5))
            buckets[(rot, key_axis)].append(t)
        units: list[dict[str, Any]] = []
        for (_rot, _axis), group in buckets.items():
            horizontal = abs(float(group[0].get("rotation") or 0.0) % 180.0) < 45 or abs(
                float(group[0].get("rotation") or 0.0) % 180.0
            ) > 135
            group.sort(key=lambda t: t["bbox"][0] if horizontal else t["bbox"][1])
            run: list[dict[str, Any]] = []
            for t in group:
                if not run:
                    run = [t]
                    continue
                prev = run[-1]
                h = max(float(prev["bbox"][3]) - float(prev["bbox"][1]), 1e-6)
                gap = (
                    float(t["bbox"][0]) - float(prev["bbox"][2])
                    if horizontal
                    else float(t["bbox"][1]) - float(prev["bbox"][3])
                )
                if gap <= 1.2 * h:
                    run.append(t)
                else:
                    units.append(self._merge_run(run))
                    run = [t]
            if run:
                units.append(self._merge_run(run))
        units.sort(key=lambda u: (u["bbox"][1], u["bbox"][0]))
        for i, u in enumerate(units):
            u["id"] = f"line-{i + 1}"
        self.units = units

    @staticmethod
    def _merge_run(run: list[dict[str, Any]]) -> dict[str, Any]:
        bbox = [
            min(float(t["bbox"][0]) for t in run),
            min(float(t["bbox"][1]) for t in run),
            max(float(t["bbox"][2]) for t in run),
            max(float(t["bbox"][3]) for t in run),
        ]
        return {
            "id": run[0]["id"],
            "ids": [t["id"] for t in run],
            "text": "".join(t["text"] for t in run),
            "bbox": bbox,
            "rotation": float(run[0].get("rotation") or 0.0),
            "font_size": float(run[0].get("font_size") or 0.0),
            "category": run[0].get("category"),
        }

    def _build_scale(self) -> None:
        heights = sorted(u["bbox"][3] - u["bbox"][1] for u in self.units)
        if heights:
            self.u = max(heights[len(heights) // 2], 1.0)
        else:
            self.u = 8.0

    def _build_index(self) -> None:
        self.grid = _Grid(cell=max(3.0 * self.u, 4.0))
        for seg in self.segments:
            self.grid.insert_segment(seg["i"], seg["p1"], seg["p2"])

    # -- chains (polylines inside one primitive) ---------------------------
    def _build_chains(self) -> None:
        tol = max(0.25, 0.03 * self.u)
        chains: list[dict[str, Any]] = []
        seg_chain: dict[int, int] = {}
        by_prim: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        for seg in self.segments:
            by_prim[seg["prim"]].append(seg)
        for prim_id, segs in by_prim.items():
            segs.sort(key=lambda s: s["local"])
            run: list[dict[str, Any]] = []
            for seg in segs:
                if run and _hypot(run[-1]["p2"][0], run[-1]["p2"][1], seg["p1"][0], seg["p1"][1]) <= tol:
                    run.append(seg)
                else:
                    if run:
                        chains.append(self._make_chain(len(chains), prim_id, run, tol))
                    run = [seg]
            if run:
                chains.append(self._make_chain(len(chains), prim_id, run, tol))
        for ch in chains:
            for i in ch["seg_indexes"]:
                seg_chain[i] = ch["id"]
        self.chains = chains
        self.seg_chain = seg_chain
        self.closed_polygons = [
            {"chain": ch["id"], "points": ch["points"], "area": _polygon_area(ch["points"]), "bbox": ch["bbox"]}
            for ch in chains
            if ch["closed"] and len(ch["points"]) >= 3
        ]

    def _make_chain(self, cid: int, prim_id: str, run: list[dict[str, Any]], tol: float) -> dict[str, Any]:
        points = [run[0]["p1"]] + [s["p2"] for s in run]
        closed = len(run) >= 3 and _hypot(points[0][0], points[0][1], points[-1][0], points[-1][1]) <= tol
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        return {
            "id": cid,
            "prim": prim_id,
            "seg_indexes": [s["i"] for s in run],
            "points": points[:-1] if closed else points,
            "ends": (points[0], points[-1]),
            "closed": closed,
            "length": sum(s["len"] for s in run),
            "bbox": [min(xs), min(ys), max(xs), max(ys)],
        }

    # -- node graph / connected components ---------------------------------
    def _build_nodes_and_components(self) -> None:
        tol = 0.35
        quant = max(tol, 0.2)
        node_of: dict[tuple[int, int], int] = {}
        seg_nodes: list[tuple[int, int]] = []

        def node_id(p: Sequence[float]) -> int:
            base = (int(round(p[0] / quant)), int(round(p[1] / quant)))
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    key = (base[0] + dx, base[1] + dy)
                    if key in node_of:
                        return node_of[key]
            node_of[base] = len(node_of)
            return node_of[base]

        degree: collections.Counter[int] = collections.Counter()
        for seg in self.segments:
            a, b = node_id(seg["p1"]), node_id(seg["p2"])
            seg_nodes.append((a, b))
            degree[a] += 1
            degree[b] += 1
        self.node_degree = degree
        self.seg_nodes = seg_nodes

        uf = _UnionFind(len(node_of))
        for a, b in seg_nodes:
            uf.union(a, b)
        comps: dict[int, dict[str, Any]] = {}
        for idx, seg in enumerate(self.segments):
            root = uf.find(seg_nodes[idx][0])
            comp = comps.get(root)
            if comp is None:
                comp = comps[root] = {
                    "id": root,
                    "segs": [],
                    "bbox": [seg["p1"][0], seg["p1"][1], seg["p1"][0], seg["p1"][1]],
                    "length": 0.0,
                }
            comp["segs"].append(idx)
            comp["length"] += seg["len"]
            bb = comp["bbox"]
            for p in (seg["p1"], seg["p2"]):
                bb[0] = min(bb[0], p[0])
                bb[1] = min(bb[1], p[1])
                bb[2] = max(bb[2], p[0])
                bb[3] = max(bb[3], p[1])
        self.components = list(comps.values())
        self._node_of = node_of
        self._node_quant = quant
        self.comp_grid = _Grid(cell=max(6.0 * self.u, 8.0))
        for ci, comp in enumerate(self.components):
            bb = comp["bbox"]
            self.comp_grid.insert_segment(ci, (bb[0], bb[1]), (bb[2], bb[3]))
            self.comp_grid.insert_segment(ci, (bb[0], bb[3]), (bb[2], bb[1]))


# ---------------------------------------------------------------------------
# relation detectors
# ---------------------------------------------------------------------------


def _model_node_at(self, p: Sequence[float]) -> int:
    base = (int(round(p[0] / self._node_quant)), int(round(p[1] / self._node_quant)))
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            key = (base[0] + dx, base[1] + dy)
            if key in self._node_of:
                return self._node_of[key]
    return -1


BlockModel.node_at = _model_node_at


def _unit_center(u: dict[str, Any]) -> tuple[float, float]:
    b = u["bbox"]
    return ((b[0] + b[2]) / 2.0, (b[1] + b[3]) / 2.0)


def _unit_width(u: dict[str, Any]) -> float:
    b = u["bbox"]
    rot = abs(u["rotation"] % 180.0)
    return (b[2] - b[0]) if (rot < 45 or rot > 135) else (b[3] - b[1])


def rel_nearest_geometry(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    """v0.1 baseline, recomputed in raw space, with an honest uniqueness test."""
    cx, cy = _unit_center(unit)
    max_d = 0.035 * max(
        model.description["bbox"][2] - model.description["bbox"][0],
        model.description["bbox"][3] - model.description["bbox"][1],
    )
    cand = model.grid.query_rect(cx - max_d, cy - max_d, cx + max_d, cy + max_d)
    best: list[tuple[float, str]] = []
    for i in cand:
        seg = model.segments[i]
        d = _point_segment_distance((cx, cy), seg["p1"], seg["p2"])
        if d <= max_d:
            best.append((d, seg["prim"]))
    if not best:
        return {"hit": False, "candidates": 0, "unique": False}
    best.sort()
    d0, prim0 = best[0]
    runner = next((d for d, p in best if p != prim0), None)
    unique = runner is None or runner > 1.5 * max(d0, 1e-6)
    distinct_within = len({p for d, p in best if d <= max(d0 * 1.5, d0 + 0.3 * model.u)})
    return {
        "hit": True,
        "referent": prim0,
        "distance": round(d0, 3),
        "candidates": distinct_within,
        "unique": bool(unique and distinct_within == 1),
        "v01_confidence": "high" if d0 <= 0.012 * max(
            model.description["bbox"][2] - model.description["bbox"][0],
            model.description["bbox"][3] - model.description["bbox"][1],
        ) else "candidate",
    }


def rel_enclosure(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    cx, cy = _unit_center(unit)
    b = unit["bbox"]
    text_area = max((b[2] - b[0]) * (b[3] - b[1]), 1e-6)
    containing = []
    for poly in model.closed_polygons:
        bb = poly["bbox"]
        if not (bb[0] <= cx <= bb[2] and bb[1] <= cy <= bb[3]):
            continue
        if _point_in_polygon((cx, cy), poly["points"]):
            containing.append(poly)
    if not containing:
        return {"hit": False, "candidates": 0, "unique": False}
    containing.sort(key=lambda p: p["area"])
    minimal = containing[0]
    tight = minimal["area"] <= 60.0 * text_area
    return {
        "hit": True,
        "referent": f"chain-{minimal['chain']}",
        "candidates": len(containing),
        "unique": len(containing) == 1,
        "tight": bool(tight),
        "area_ratio": round(minimal["area"] / text_area, 2),
    }


def rel_grid_cell(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    cx, cy = _unit_center(unit)
    u = model.u
    win = 80.0 * u
    cand = model.grid.query_rect(cx - win, cy - win, cx + win, cy + win)
    left = right = above = below = None
    for i in cand:
        seg = model.segments[i]
        p1, p2 = seg["p1"], seg["p2"]
        if seg["ang"] > 87.0 or seg["ang"] < 3.0:
            pass
        if 87.0 <= seg["ang"] <= 93.0:  # vertical
            ylo, yhi = min(p1[1], p2[1]), max(p1[1], p2[1])
            if ylo - 0.2 * u <= cy <= yhi + 0.2 * u:
                x = (p1[0] + p2[0]) / 2.0
                if x < cx and (left is None or x > left[0]):
                    left = (x, seg["i"])
                if x > cx and (right is None or x < right[0]):
                    right = (x, seg["i"])
        elif seg["ang"] <= 3.0 or seg["ang"] >= 177.0:  # horizontal
            xlo, xhi = min(p1[0], p2[0]), max(p1[0], p2[0])
            if xlo - 0.2 * u <= cx <= xhi + 0.2 * u:
                y = (p1[1] + p2[1]) / 2.0
                if y < cy and (above is None or y > above[0]):
                    above = (y, seg["i"])
                if y > cy and (below is None or y < below[0]):
                    below = (y, seg["i"])
    if not (left and right and above and below):
        return {"hit": False, "candidates": 0, "unique": False}
    w = right[0] - left[0]
    h = below[0] - above[0]
    if w > 80.0 * u or h > 30.0 * u or w < 0.4 * u or h < 0.4 * u:
        return {"hit": False, "candidates": 0, "unique": False, "reason": "cell_out_of_range"}
    return {
        "hit": True,
        "referent": f"cell:{round(left[0], 1)},{round(above[0], 1)},{round(right[0], 1)},{round(below[0], 1)}",
        "candidates": 1,
        "unique": True,
        "cell_w_u": round(w / u, 2),
        "cell_h_u": round(h / u, 2),
    }


def _chain_free_end_referent(model: BlockModel, chain: dict[str, Any], tip: Sequence[float], own_prim: str) -> dict[str, Any]:
    u = model.u
    radius = 1.5 * u
    cand = model.grid.query_rect(tip[0] - radius, tip[1] - radius, tip[0] + radius, tip[1] + radius)
    hits: list[tuple[float, str]] = []
    for i in cand:
        seg = model.segments[i]
        if seg["prim"] == own_prim and model.seg_chain.get(i) == chain["id"]:
            continue
        d = _point_segment_distance(tip, seg["p1"], seg["p2"])
        if d <= radius:
            hits.append((d, f"{seg['prim']}#{seg['local']}"))
    if not hits:
        return {"referent": None, "candidates": 0}
    hits.sort()
    tol = max(0.25 * u, 0.5)
    near = [h for h in hits if h[0] <= hits[0][0] + tol]
    return {"referent": hits[0][1], "candidates": len({n[1].split('#')[0] for n in near}), "tip_distance": round(hits[0][0], 3)}


def rel_leader(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    """Выноска: text sitting on a shelf (полочка) or at the free end of a polyline."""
    u = model.u
    b = unit["bbox"]
    cx, cy = _unit_center(unit)
    tw = max(_unit_width(unit), 0.5 * u)
    win = 2.0 * u + tw
    cand = model.grid.query_rect(b[0] - win, b[1] - win, b[2] + win, b[3] + win)

    shelf_chains: list[tuple[int, str]] = []
    for i in cand:
        seg = model.segments[i]
        if not (seg["ang"] <= 6.0 or seg["ang"] >= 174.0):
            continue
        y = (seg["p1"][1] + seg["p2"][1]) / 2.0
        near_bottom = abs(y - b[3]) <= 1.0 * u
        near_top = abs(y - b[1]) <= 1.0 * u
        if not (near_bottom or near_top):
            continue
        xlo, xhi = min(seg["p1"][0], seg["p2"][0]), max(seg["p1"][0], seg["p2"][0])
        overlap = min(xhi, b[2]) - max(xlo, b[0])
        if overlap < 0.5 * (b[2] - b[0]):
            continue
        if (xhi - xlo) > 4.0 * tw + 3.0 * u:
            continue
        shelf_chains.append((i, "shelf"))

    results: list[dict[str, Any]] = []
    seen_chains: set[int] = set()
    for i, _kind in shelf_chains:
        cid = model.seg_chain.get(i)
        if cid is None or cid in seen_chains:
            continue
        seen_chains.add(cid)
        chain = model.chains[cid]
        if chain["closed"] or len(chain["seg_indexes"]) < 2:
            continue
        # a полочка is followed by an inclined leader: the chain must bend
        angles = [model.segments[j]["ang"] for j in chain["seg_indexes"]]
        if max(_angle_delta(a, angles[0]) for a in angles) < 15.0:
            continue
        e0, e1 = chain["ends"]
        if _hypot(e0[0], e0[1], e1[0], e1[1]) < 3.0 * u:
            continue  # near-closed polyline (a box), not a leader
        far = e0 if _hypot(e0[0], e0[1], cx, cy) > _hypot(e1[0], e1[1], cx, cy) else e1
        if _hypot(far[0], far[1], cx, cy) < 2.0 * u:
            continue
        ref = _chain_free_end_referent(model, chain, far, chain["prim"])
        results.append({"form": "shelf", "chain": cid, "tip": [round(far[0], 2), round(far[1], 2)], **ref})

    if not results:
        # direct form: a free chain end that touches the text box
        for i in cand:
            cid = model.seg_chain.get(i)
            if cid is None or cid in seen_chains:
                continue
            chain = model.chains[cid]
            if chain["closed"] or chain["length"] < 3.0 * u:
                continue
            e0, e1 = chain["ends"]
            if _hypot(e0[0], e0[1], e1[0], e1[1]) < 3.0 * u:
                continue
            d0 = _point_box_distance(e0, b)
            d1 = _point_box_distance(e1, b)
            near, far = (e0, e1) if d0 <= d1 else (e1, e0)
            if min(d0, d1) > 1.0 * u:
                continue
            if _hypot(far[0], far[1], cx, cy) < 3.0 * u:
                continue
            seen_chains.add(cid)
            ref = _chain_free_end_referent(model, chain, far, chain["prim"])
            results.append({"form": "direct", "chain": cid, "tip": [round(far[0], 2), round(far[1], 2)], **ref})

    if not results:
        return {"hit": False, "candidates": 0, "unique": False}
    with_ref = [r for r in results if r.get("referent")]
    chosen = with_ref[0] if with_ref else results[0]
    return {
        "hit": True,
        "referent": chosen.get("referent"),
        "form": chosen["form"],
        "leaders": len(results),
        "candidates": chosen.get("candidates", 0),
        "unique": len(results) == 1 and chosen.get("candidates", 0) == 1,
        "resolved": bool(chosen.get("referent")),
    }


def _point_box_distance(p: Sequence[float], b: Sequence[float]) -> float:
    dx = max(b[0] - p[0], 0.0, p[0] - b[2])
    dy = max(b[1] - p[1], 0.0, p[1] - b[3])
    return math.hypot(dx, dy)


def _terminator_count(model: BlockModel, seg: dict[str, Any], end: Sequence[float]) -> int:
    u = model.u
    r = 0.8 * u
    cand = model.grid.query_rect(end[0] - r, end[1] - r, end[0] + r, end[1] + r)
    count = 0
    for i in cand:
        other = model.segments[i]
        if other["i"] == seg["i"]:
            continue
        if other["len"] > 3.0 * u:
            continue
        d = min(
            _hypot(other["p1"][0], other["p1"][1], end[0], end[1]),
            _hypot(other["p2"][0], other["p2"][1], end[0], end[1]),
        )
        if d > r:
            continue
        if _angle_delta(other["ang"], seg["ang"]) < 12.0:
            continue
        count += 1
    return count


def rel_along_line_and_dimension(model: BlockModel, unit: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Both relations share the same candidate scan: a line parallel to the text baseline."""
    u = model.u
    cx, cy = _unit_center(unit)
    tw = max(_unit_width(unit), 0.5 * u)
    rot = unit["rotation"] % 180.0
    win = 2.0 * u + tw
    cand = model.grid.query_rect(cx - win, cy - win, cx + win, cy + win)
    along: list[dict[str, Any]] = []
    dims: list[dict[str, Any]] = []
    for i in cand:
        seg = model.segments[i]
        if seg["len"] < max(1.2 * tw, 2.0 * u):
            continue
        if _angle_delta(seg["ang"], rot) > 6.0:
            continue
        perp = _perp_distance_to_line((cx, cy), seg["p1"], seg["p2"])
        if perp > 1.3 * u:
            continue
        t = _projection_param((cx, cy), seg["p1"], seg["p2"])
        if not (-0.02 <= t <= 1.02):
            continue
        entry = {"seg": f"{seg['prim']}#{seg['local']}", "len": round(seg["len"], 2), "t": round(t, 3), "perp": round(perp, 2)}
        along.append(entry)
        chain_id = model.seg_chain.get(seg["i"])
        in_closed = chain_id is not None and model.chains[chain_id]["closed"]
        if abs(t - 0.5) <= 0.45 and not in_closed:
            t1 = _terminator_count(model, seg, seg["p1"])
            t2 = _terminator_count(model, seg, seg["p2"])
            if t1 >= 1 and t2 >= 1:
                dims.append({**entry, "terminators": [t1, t2]})
    along_res = {"hit": bool(along), "candidates": len(along), "unique": len(along) == 1}
    if along:
        along.sort(key=lambda e: e["perp"])
        along_res["referent"] = along[0]["seg"]
        along_res["line_len"] = along[0]["len"]
    dim_res = {"hit": bool(dims), "candidates": len(dims), "unique": len(dims) == 1}
    if dims:
        dims.sort(key=lambda e: e["perp"])
        dim_res["referent"] = dims[0]["seg"]
        dim_res["measured_len_pt"] = dims[0]["len"]
    return along_res, dim_res


def _bracketing_span(model: BlockModel, unit: dict[str, Any], D: dict[str, Any]) -> list[float]:
    """Interval on line D that brackets the text — used to enumerate alternatives."""
    u = model.u
    cx, cy = _unit_center(unit)
    p1, p2 = D["p1"], D["p2"]
    dlen = D["len"]
    dx, dy = (p2[0] - p1[0]) / dlen, (p2[1] - p1[1]) / dlen
    t_text = _projection_param((cx, cy), p1, p2) * dlen
    reach = 40.0 * u
    lo, hi = max(0.0, t_text - reach), min(dlen, t_text + reach)
    ax, ay = p1[0] + dx * lo, p1[1] + dy * lo
    bx, by = p1[0] + dx * hi, p1[1] + dy * hi
    band = model.grid.query_rect(min(ax, bx) - 2 * u, min(ay, by) - 2 * u, max(ax, bx) + 2 * u, max(ay, by) + 2 * u)
    ticks = []
    for i in band:
        seg = model.segments[i]
        if seg["i"] == D["i"] or _angle_delta(seg["ang"], D["ang"]) < 20.0:
            continue
        best = None
        for q in (seg["p1"], seg["p2"], ((seg["p1"][0] + seg["p2"][0]) / 2.0, (seg["p1"][1] + seg["p2"][1]) / 2.0)):
            d = _point_segment_distance(q, p1, p2)
            if best is None or d < best[0]:
                best = (d, q)
        if best[0] > 0.7 * u:
            continue
        ticks.append((best[1][0] - p1[0]) * dx + (best[1][1] - p1[1]) * dy)
    if len(ticks) < 2:
        return []
    ticks.sort()
    merged = []
    for t in ticks:
        if not merged or t - merged[-1] > 0.5 * u:
            merged.append(t)
    prev = [t for t in merged if t <= t_text + 0.15 * u]
    nxt = [t for t in merged if t >= t_text - 0.15 * u]
    if not prev or not nxt or prev[-1] >= nxt[0] - 1e-6:
        return []
    return [nxt[0] - prev[-1]]


def rel_dimension_interval(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    """The referent of a dimension number is an INTERVAL on a dimension line, not a primitive.

    A Russian dimension chain (размерная цепочка) is one long line carrying many засечки;
    each number labels the gap between two adjacent ticks.  So: find the line the text sits
    on, collect every segment that crosses it at an angle (tick / extension line / arrow),
    and return the tick-to-tick interval that brackets the text.
    """
    if not _DIM_TEXT_RE.match((unit["text"] or "").strip()):
        return {"hit": False, "candidates": 0, "unique": False, "reason": "text_not_a_dimension_value"}
    u = model.u
    cx, cy = _unit_center(unit)
    tw = max(_unit_width(unit), 0.5 * u)
    rot = unit["rotation"] % 180.0
    win = 3.0 * u + tw
    cand = model.grid.query_rect(cx - win, cy - win, cx + win, cy + win)
    lines: list[tuple[float, dict[str, Any]]] = []
    for i in cand:
        seg = model.segments[i]
        if seg["len"] < max(1.0 * u, 0.9 * tw):
            continue
        if _angle_delta(seg["ang"], rot) > 6.0:
            continue
        perp = _perp_distance_to_line((cx, cy), seg["p1"], seg["p2"])
        if perp > 1.6 * u:
            continue
        t = _projection_param((cx, cy), seg["p1"], seg["p2"])
        if not (-0.02 <= t <= 1.02):
            continue
        lines.append((perp, seg))
    if not lines:
        return {"hit": False, "candidates": 0, "unique": False}
    lines.sort(key=lambda r: r[0])
    alt_spans: list[float] = []
    for _perp_alt, D_alt in lines[1:3]:
        span_alt = _bracketing_span(model, unit, D_alt)
        if span_alt:
            alt_spans.extend(span_alt)
    perp, D = lines[0]
    p1, p2 = D["p1"], D["p2"]
    dlen = D["len"]
    dx, dy = (p2[0] - p1[0]) / dlen, (p2[1] - p1[1]) / dlen
    t_text = _projection_param((cx, cy), p1, p2) * dlen  # distance along D, in points

    reach = 40.0 * u
    lo = max(0.0, t_text - reach)
    hi = min(dlen, t_text + reach)
    ax, ay = p1[0] + dx * lo, p1[1] + dy * lo
    bx, by = p1[0] + dx * hi, p1[1] + dy * hi
    band = model.grid.query_rect(min(ax, bx) - 2 * u, min(ay, by) - 2 * u, max(ax, bx) + 2 * u, max(ay, by) + 2 * u)
    ticks: list[float] = []
    for i in band:
        seg = model.segments[i]
        if seg["i"] == D["i"]:
            continue
        if _angle_delta(seg["ang"], D["ang"]) < 20.0:
            continue
        best = None
        for q in (seg["p1"], seg["p2"], ((seg["p1"][0] + seg["p2"][0]) / 2.0, (seg["p1"][1] + seg["p2"][1]) / 2.0)):
            d = _point_segment_distance(q, p1, p2)
            if best is None or d < best[0]:
                best = (d, q)
        if best[0] > 0.7 * u:
            continue
        pos = ((best[1][0] - p1[0]) * dx + (best[1][1] - p1[1]) * dy)
        if lo - 0.5 <= pos <= hi + 0.5:
            ticks.append(pos)
    if len(ticks) < 2:
        return {"hit": False, "candidates": 0, "unique": False, "reason": "no_ticks"}
    ticks.sort()
    merged: list[float] = []
    for t in ticks:
        if not merged or t - merged[-1] > 0.5 * u:
            merged.append(t)
    prev = [t for t in merged if t <= t_text + 0.15 * u]
    nxt = [t for t in merged if t >= t_text - 0.15 * u]
    if not prev or not nxt or prev[-1] >= nxt[0] - 1e-6:
        return {"hit": False, "candidates": 0, "unique": False, "reason": "text_not_between_ticks"}
    a, b = prev[-1], nxt[0]
    span = b - a
    if span < 0.4 * u or span > 40.0 * u:
        return {"hit": False, "candidates": 0, "unique": False, "reason": "span_out_of_range"}
    centred = abs(t_text - (a + b) / 2.0) <= max(0.35 * span, 0.6 * u)
    wider = []
    idx_a = merged.index(a)
    idx_b = merged.index(b)
    if idx_a - 1 >= 0:
        wider.append(round(b - merged[idx_a - 1], 3))
    if idx_b + 1 < len(merged):
        wider.append(round(merged[idx_b + 1] - a, 3))
    return {
        "hit": True,
        "referent": f"interval:{D['prim']}#{D['local']}:{round(a, 2)}-{round(b, 2)}",
        "measured_len_pt": round(span, 3),
        "alt_spans_pt": [round(v, 3) for v in alt_spans][:4] + wider,
        "candidates": len(lines),
        "unique": len(lines) == 1,
        "centred_on_interval": bool(centred),
        "ticks_in_reach": len(merged),
        "perp": round(perp, 2),
    }


def rel_band_association(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    """Text and a graphic sitting side by side in the same horizontal band.

    This is the legend row (swatch ... description), the specification row and the
    «схема сечения» cell.  No line connects them; the association is pure layout.
    """
    u = model.u
    b = unit["bbox"]
    cy = (b[1] + b[3]) / 2.0
    h = max(b[3] - b[1], 1e-6)
    reach = 30.0 * u
    cand = model.comp_grid.query_rect(b[0] - reach, b[1] - 0.5 * u, b[2] + reach, b[3] + 0.5 * u)
    left: list[tuple[float, int]] = []
    right: list[tuple[float, int]] = []
    for ci in cand:
        comp = model.components[ci]
        bb = comp["bbox"]
        overlap = min(bb[3], b[3]) - max(bb[1], b[1])
        if overlap < 0.5 * min(h, bb[3] - bb[1] + 1e-6):
            continue
        if bb[3] - bb[1] > 4.0 * h:
            continue
        if bb[2] < b[0]:
            gap = b[0] - bb[2]
            if gap <= reach:
                left.append((gap, ci))
        elif bb[0] > b[2]:
            gap = bb[0] - b[2]
            if gap <= reach:
                right.append((gap, ci))
    if not left and not right:
        return {"hit": False, "candidates": 0, "unique": False}
    left.sort()
    right.sort()
    side = "left" if (left and (not right or left[0][0] <= right[0][0])) else "right"
    chosen = (left if side == "left" else right)[0]
    return {
        "hit": True,
        "referent": f"component-{chosen[1]}",
        "side": side,
        "gap_u": round(chosen[0] / u, 2),
        "candidates": len(left) + len(right),
        "unique": (len(left) + len(right)) == 1,
    }


def rel_symbol_cluster(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    u = model.u
    b = unit["bbox"]
    cx, cy = _unit_center(unit)
    diag = math.hypot(b[2] - b[0], b[3] - b[1])
    cand = model.comp_grid.query_rect(cx - 2 * u, cy - 2 * u, cx + 2 * u, cy + 2 * u)
    hits = []
    for ci in cand:
        comp = model.components[ci]
        bb = comp["bbox"]
        if not (bb[0] - 0.2 * u <= cx <= bb[2] + 0.2 * u and bb[1] - 0.2 * u <= cy <= bb[3] + 0.2 * u):
            continue
        cdiag = math.hypot(bb[2] - bb[0], bb[3] - bb[1])
        if cdiag > 6.0 * diag or cdiag < 0.6 * diag:
            continue
        ccx, ccy = (bb[0] + bb[2]) / 2.0, (bb[1] + bb[3]) / 2.0
        off = math.hypot(ccx - cx, ccy - cy)
        if off > 0.7 * u:
            continue
        hits.append((off, ci))
    if not hits:
        return {"hit": False, "candidates": 0, "unique": False}
    hits.sort()
    return {"hit": True, "referent": f"component-{hits[0][1]}", "candidates": len(hits), "unique": len(hits) == 1}


def compute_repeated_labels(model: BlockModel) -> dict[str, dict[str, Any]]:
    """Text that sits at a stable offset from every instance of a repeated motif."""
    u = model.u
    groups: dict[tuple, list[int]] = collections.defaultdict(list)
    for ci, comp in enumerate(model.components):
        bb = comp["bbox"]
        w, h = bb[2] - bb[0], bb[3] - bb[1]
        if len(comp["segs"]) < 2 or max(w, h) > 20.0 * u or max(w, h) < 0.3 * u:
            continue
        key = (
            len(comp["segs"]),
            round(w / u, 1),
            round(h / u, 1),
            round(comp["length"] / u, 0),
        )
        groups[key].append(ci)
    result: dict[str, dict[str, Any]] = {}
    unit_centers = [(u_["id"], *_unit_center(u_)) for u_ in model.units]
    ucx = sorted(unit_centers, key=lambda r: r[1])
    for key, members in groups.items():
        if len(members) < 3:
            continue
        offsets: dict[tuple[int, int], list[tuple[int, str]]] = collections.defaultdict(list)
        for ci in members:
            bb = model.components[ci]["bbox"]
            ccx, ccy = (bb[0] + bb[2]) / 2.0, (bb[1] + bb[3]) / 2.0
            for tid, tx, ty in unit_centers:
                dx, dy = tx - ccx, ty - ccy
                if abs(dx) > 8 * u or abs(dy) > 8 * u:
                    continue
                offsets[(int(round(dx / (0.75 * u))), int(round(dy / (0.75 * u))))].append((ci, tid))
        if not offsets:
            continue
        best_off, best_list = max(offsets.items(), key=lambda kv: len({c for c, _ in kv[1]}))
        instances = len({c for c, _ in best_list})
        if instances < 3 or instances < 0.5 * len(members):
            continue
        motif = f"motif-{key[0]}x{key[1]}x{key[2]}"
        for _ci, tid in best_list:
            prev = result.get(tid)
            if prev is None or prev["instances"] < instances:
                result[tid] = {
                    "hit": True,
                    "referent": motif,
                    "motif_instances": len(members),
                    "instances": instances,
                    "offset_cells": list(best_off),
                    "candidates": 1,
                    "unique": True,
                }
    return result


def compute_text_alignment(model: BlockModel) -> dict[str, dict[str, Any]]:
    u = model.u
    rows: dict[int, list[str]] = collections.defaultdict(list)
    cols: dict[int, list[str]] = collections.defaultdict(list)
    for un in model.units:
        b = un["bbox"]
        rows[int(round(b[3] / (0.4 * u)))].append(un["id"])
        cols[int(round(b[0] / (0.4 * u)))].append(un["id"])
    out: dict[str, dict[str, Any]] = {}
    for un in model.units:
        b = un["bbox"]
        r = len(rows[int(round(b[3] / (0.4 * u)))])
        c = len(cols[int(round(b[0] / (0.4 * u)))])
        out[un["id"]] = {
            "hit": r >= 2 or c >= 3,
            "row_size": r,
            "column_size": c,
            "candidates": 1 if (r >= 2 or c >= 3) else 0,
            "unique": bool(r >= 2 or c >= 3),
        }
    return out


def rel_contour_caption(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    """Text placed just outside a closed contour, overlapping one of its sides.

    Covers the very common CAD case of a box designation written above/beside the box
    (КР, ВК, БГЗ in ss_simple_node; equipment tags on plans).
    """
    u = model.u
    b = unit["bbox"]
    cx, cy = _unit_center(unit)
    hits = []
    for poly in model.closed_polygons:
        if len(poly["points"]) > 12:
            continue
        bb = poly["bbox"]
        if _point_in_polygon((cx, cy), poly["points"]):
            continue
        w, h = bb[2] - bb[0], bb[3] - bb[1]
        if w < 1.5 * u or h < 1.0 * u:
            continue
        ox = min(bb[2], b[2]) - max(bb[0], b[0])
        oy = min(bb[3], b[3]) - max(bb[1], b[1])
        gap_y = min(abs(b[3] - bb[1]), abs(b[1] - bb[3]))
        gap_x = min(abs(b[2] - bb[0]), abs(b[0] - bb[2]))
        touch_top_bottom = ox >= 0.4 * (b[2] - b[0]) and gap_y <= 1.2 * u
        touch_left_right = oy >= 0.4 * (b[3] - b[1]) and gap_x <= 1.2 * u
        if touch_top_bottom or touch_left_right:
            hits.append((min(gap_y, gap_x), poly))
    if not hits:
        return {"hit": False, "candidates": 0, "unique": False}
    hits.sort(key=lambda r: r[0])
    return {
        "hit": True,
        "referent": f"chain-{hits[0][1]['chain']}",
        "candidates": len(hits),
        "unique": len(hits) == 1,
    }


def rel_between_extension_lines(model: BlockModel, unit: dict[str, Any]) -> dict[str, Any]:
    """Text flanked by two roughly parallel lines perpendicular to its baseline.

    This is the выносные линии case: the dimension line itself may be absent or broken,
    but the two extension lines that bracket the measured distance are present.
    """
    u = model.u
    b = unit["bbox"]
    cx, cy = _unit_center(unit)
    rot = unit["rotation"] % 180.0
    perp_angle = (rot + 90.0) % 180.0
    reach = 10.0 * u
    cand = model.grid.query_rect(cx - reach, cy - reach, cx + reach, cy + reach)
    ux, uy = math.cos(math.radians(rot)), math.sin(math.radians(rot))
    left: list[tuple[float, dict[str, Any]]] = []
    right: list[tuple[float, dict[str, Any]]] = []
    for i in cand:
        seg = model.segments[i]
        if seg["len"] < 1.5 * u:
            continue
        if _angle_delta(seg["ang"], perp_angle) > 8.0:
            continue
        mid = ((seg["p1"][0] + seg["p2"][0]) / 2.0, (seg["p1"][1] + seg["p2"][1]) / 2.0)
        along = (mid[0] - cx) * ux + (mid[1] - cy) * uy
        across = abs(-(mid[0] - cx) * uy + (mid[1] - cy) * ux)
        if across > 3.0 * u:
            continue
        if abs(along) > reach or abs(along) < 0.3 * (b[2] - b[0]) * 0.5:
            continue
        (left if along < 0 else right).append((abs(along), seg))
    if not left or not right:
        return {"hit": False, "candidates": 0, "unique": False}
    left.sort(key=lambda r: r[0])
    right.sort(key=lambda r: r[0])
    span = left[0][0] + right[0][0]
    return {
        "hit": True,
        "referent": f"ext:{left[0][1]['prim']}#{left[0][1]['local']}|{right[0][1]['prim']}#{right[0][1]['local']}",
        "candidates": len(left) * len(right),
        "unique": len(left) == 1 and len(right) == 1,
        "measured_len_pt": round(span, 2),
    }


PRIORITY = [
    "dimension_interval",
    "dimension_line_only",
    "leader",
    "symbol_cluster",
    "enclosure_tight",
    "contour_caption",
    "repeated_label",
    "grid_cell",
    "between_extension_lines",
    "band_association",
    "along_line",
    "enclosure_loose",
]


def analyse(description: dict[str, Any], unit_mode: str = "span") -> dict[str, Any]:
    model = BlockModel(description, unit_mode=unit_mode)
    repeated = compute_repeated_labels(model)
    alignment = compute_text_alignment(model)
    rows: list[dict[str, Any]] = []
    for unit in model.units:
        along, dim = rel_along_line_and_dimension(model, unit)
        enc = rel_enclosure(model, unit)
        rels: dict[str, Any] = {
            "nearest_geometry": rel_nearest_geometry(model, unit),
            "enclosure": enc,
            "grid_cell": rel_grid_cell(model, unit),
            "leader": rel_leader(model, unit),
            "along_line": along,
            "dimension_line_only": dim,
            "dimension_interval": rel_dimension_interval(model, unit),
            "symbol_cluster": rel_symbol_cluster(model, unit),
            "contour_caption": rel_contour_caption(model, unit),
            "band_association": rel_band_association(model, unit),
            "between_extension_lines": rel_between_extension_lines(model, unit),
            "repeated_label": repeated.get(unit["id"], {"hit": False, "candidates": 0, "unique": False}),
            "text_alignment": alignment.get(unit["id"], {"hit": False, "candidates": 0, "unique": False}),
        }
        rels["enclosure_tight"] = {**enc, "hit": bool(enc.get("hit") and enc.get("tight"))}
        rels["enclosure_loose"] = {**enc, "hit": bool(enc.get("hit") and not enc.get("tight"))}
        primary = "unbound"
        for name in PRIORITY:
            if rels.get(name, {}).get("hit"):
                primary = name
                break
        rows.append(
            {
                "id": unit["id"],
                "span_ids": unit["ids"],
                "text": unit["text"],
                "bbox": [round(v, 2) for v in unit["bbox"]],
                "rotation": unit["rotation"],
                "category": unit["category"],
                "primary": primary,
                "relations": {k: v for k, v in rels.items() if v.get("hit") or k == "nearest_geometry"},
            }
        )
    return {
        "probe_version": PROBE_VERSION,
        "unit_mode": unit_mode,
        "text_unit_scale_pt": round(model.u, 3),
        "counts": {
            "units": len(model.units),
            "spans": len(description["texts"]),
            "segments": len(model.segments),
            "chains": len(model.chains),
            "closed_polygons": len(model.closed_polygons),
            "components": len(model.components),
        },
        "units": rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--description", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--unit-mode", default="span", choices=["span", "line"])
    args = ap.parse_args()
    description = json.loads(Path(args.description).read_text(encoding="utf-8"))
    result = analyse(description, unit_mode=args.unit_mode)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(result["counts"], ensure_ascii=False))


if __name__ == "__main__":
    main()
