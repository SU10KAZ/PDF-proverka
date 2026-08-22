"""ptn_ probe library: motif units + competing pattern signatures S0..S5.

Research only. Reads Track A descriptions; writes nothing outside this experiment dir.

Signatures under test
--------------------
S0  current `repeated_elements` (per PDF primitive, taken verbatim from the Track A description)
S1  geometry only, per motif unit  (uniform-scale local frame, quantized segment multiset)
S2  S1 + local topology (sorted degree profile of the motif's own endpoint graph)
S3  S1 + nearby text (shape class of the nearest span + polar offset bucket)
S4  S1 + neighbouring relations (# of network segments touching it, enclosed-by-rect flag)
S5  S1 canonicalised over the dihedral group D4 (rotation k*90 + mirror)
S5c S1 canonicalised over continuous rotation (longest segment aligned to +x) + mirror
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

EPS = 1e-9


# ---------------------------------------------------------------- loading

def load_description(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def block_diag(desc: dict[str, Any]) -> float:
    x0, y0, x1, y1 = desc["bbox"]
    return math.hypot(x1 - x0, y1 - y0)


def load_segments(desc: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten raw (PDF-point) segments of every primitive."""
    out: list[dict[str, Any]] = []
    for index, prim in enumerate(desc["geometry"]["primitives"]):
        style = prim.get("style") or {}
        for start, end in prim["raw"]["segments"]:
            length = math.hypot(end[0] - start[0], end[1] - start[1])
            out.append(
                {
                    "p0": (float(start[0]), float(start[1])),
                    "p1": (float(end[0]), float(end[1])),
                    "len": length,
                    "prim": index,
                    "ptype": prim["type"],
                    "stroke": tuple(style.get("stroke") or ()),
                    "fill": tuple(style.get("fill") or ()),
                }
            )
    return out


# ---------------------------------------------------------------- node snapping

class NodeIndex:
    """Snap points to shared nodes using a tolerance grid (3x3 neighbourhood lookup)."""

    def __init__(self, tol: float) -> None:
        self.tol = tol
        self.cells: dict[tuple[int, int], list[int]] = defaultdict(list)
        self.points: list[tuple[float, float]] = []

    def _cell(self, point: Sequence[float]) -> tuple[int, int]:
        return (int(math.floor(point[0] / self.tol)), int(math.floor(point[1] / self.tol)))

    def node(self, point: Sequence[float]) -> int:
        cx, cy = self._cell(point)
        best, best_d = None, self.tol
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for idx in self.cells.get((cx + dx, cy + dy), ()):
                    q = self.points[idx]
                    d = math.hypot(q[0] - point[0], q[1] - point[1])
                    if d <= best_d:
                        best, best_d = idx, d
        if best is not None:
            return best
        idx = len(self.points)
        self.points.append((float(point[0]), float(point[1])))
        self.cells[(cx, cy)].append(idx)
        return idx


class DisjointSet:
    def __init__(self) -> None:
        self.parent: dict[int, int] = {}

    def find(self, item: int) -> int:
        parent = self.parent.setdefault(item, item)
        while parent != item:
            item, parent = parent, self.parent.setdefault(parent, parent)
            self.parent[item] = parent
        return item

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


# ---------------------------------------------------------------- motif units

def build_motifs(
    desc: dict[str, Any],
    *,
    unit: str = "cc_split",
    tol_pt: float = 0.6,
    long_frac: float = 0.03,
    max_seg: int = 400,
    max_diag_frac: float = 0.12,
) -> dict[str, Any]:
    """Return motif candidates plus the network segments left over.

    unit='prim'      : one motif per PDF primitive (what Track A fingerprints today)
    unit='cc'        : connected components of the whole segment graph
    unit='cc_split'  : long ('network') segments removed first, then connected components
    """
    segments = load_segments(desc)
    diag = block_diag(desc)
    long_threshold = long_frac * diag

    if unit == "prim":
        groups: dict[int, list[int]] = defaultdict(list)
        for i, seg in enumerate(segments):
            groups[seg["prim"]].append(i)
        components = list(groups.values())
        network: list[int] = []
        nodes = NodeIndex(tol_pt)
        seg_nodes = [(nodes.node(s["p0"]), nodes.node(s["p1"])) for s in segments]
    else:
        nodes = NodeIndex(tol_pt)
        seg_nodes = [(nodes.node(s["p0"]), nodes.node(s["p1"])) for s in segments]
        if unit == "cc_split":
            network = [i for i, s in enumerate(segments) if s["len"] > long_threshold]
            network_set = set(network)
        else:
            network, network_set = [], set()
        dsu = DisjointSet()
        for i, (a, b) in enumerate(seg_nodes):
            if i in network_set:
                continue
            dsu.union(a, b)
        buckets: dict[int, list[int]] = defaultdict(list)
        for i, (a, _b) in enumerate(seg_nodes):
            if i in network_set:
                continue
            buckets[dsu.find(a)].append(i)
        components = list(buckets.values())

    motifs, oversized = [], 0
    for member_indexes in components:
        if len(member_indexes) < 2:
            continue
        pts = []
        for i in member_indexes:
            pts.append(segments[i]["p0"])
            pts.append(segments[i]["p1"])
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        bbox = (min(xs), min(ys), max(xs), max(ys))
        mdiag = math.hypot(bbox[2] - bbox[0], bbox[3] - bbox[1])
        if len(member_indexes) > max_seg or mdiag > max_diag_frac * diag:
            oversized += 1
            continue
        motifs.append(
            {
                "seg_indexes": member_indexes,
                "bbox": bbox,
                "center": ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2),
                "diag": mdiag,
                "nseg": len(member_indexes),
                "nodes": [seg_nodes[i] for i in member_indexes],
            }
        )
    return {
        "segments": segments,
        "seg_nodes": seg_nodes,
        "node_points": nodes.points,
        "motifs": motifs,
        "network": network,
        "block_diag": diag,
        "long_threshold": long_threshold,
        "oversized_components": oversized,
        "unit": unit,
    }


# ---------------------------------------------------------------- signatures

def _hash(payload: Any) -> str:
    return hashlib.sha1(repr(payload).encode("utf-8")).hexdigest()[:12]


def _local_points(segs: Sequence[tuple[tuple[float, float], tuple[float, float]]]):
    pts = [p for seg in segs for p in seg]
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return min(xs), min(ys), max(xs), max(ys)


def _quantized(segs, q: float) -> tuple:
    x0, y0, x1, y1 = _local_points(segs)
    scale = max(x1 - x0, y1 - y0, EPS)
    out = []
    for a, b in segs:
        pa = (round((a[0] - x0) / scale / q), round((a[1] - y0) / scale / q))
        pb = (round((b[0] - x0) / scale / q), round((b[1] - y0) / scale / q))
        out.append(tuple(sorted((pa, pb))))
    return tuple(sorted(out))


def _aspect(segs) -> float:
    x0, y0, x1, y1 = _local_points(segs)
    w, h = max(x1 - x0, EPS), max(y1 - y0, EPS)
    return round(min(w, h) / max(w, h), 1)


def geom_core(motif, segments, q: float = 0.05) -> tuple:
    segs = [(segments[i]["p0"], segments[i]["p1"]) for i in motif["seg_indexes"]]
    return (motif["nseg"], _aspect(segs), _quantized(segs, q))


def degree_profile(motif) -> tuple:
    degree = Counter()
    for a, b in motif["nodes"]:
        degree[a] += 1
        degree[b] += 1
    return tuple(sorted(Counter(degree.values()).items()))


def _d4_variants(segs):
    for swap in (False, True):
        for sx in (1, -1):
            for sy in (1, -1):
                out = []
                for a, b in segs:
                    pa = (a[1], a[0]) if swap else (a[0], a[1])
                    pb = (b[1], b[0]) if swap else (b[0], b[1])
                    out.append(((pa[0] * sx, pa[1] * sy), (pb[0] * sx, pb[1] * sy)))
                yield out


def _rotate(segs, theta):
    c, s = math.cos(theta), math.sin(theta)
    return [
        (((a[0] * c - a[1] * s), (a[0] * s + a[1] * c)), ((b[0] * c - b[1] * s), (b[0] * s + b[1] * c)))
        for a, b in segs
    ]


def geom_core_d4(motif, segments, q: float = 0.05) -> tuple:
    segs = [(segments[i]["p0"], segments[i]["p1"]) for i in motif["seg_indexes"]]
    best = None
    for variant in _d4_variants(segs):
        key = _quantized(variant, q)
        if best is None or key < best:
            best = key
    x0, y0, x1, y1 = _local_points(segs)
    w, h = max(x1 - x0, EPS), max(y1 - y0, EPS)
    return (motif["nseg"], round(min(w, h) / max(w, h), 1), best)


def geom_core_rot(motif, segments, q: float = 0.05) -> tuple:
    """Continuous-rotation canonicalisation: align the longest segment with +x."""
    segs = [(segments[i]["p0"], segments[i]["p1"]) for i in motif["seg_indexes"]]
    lengths = [(math.hypot(b[0] - a[0], b[1] - a[1]), i) for i, (a, b) in enumerate(segs)]
    lengths.sort(reverse=True)
    top = lengths[: min(3, len(lengths))]
    best = None
    for _length, idx in top:
        a, b = segs[idx]
        theta = math.atan2(b[1] - a[1], b[0] - a[0])
        for extra in (0.0, math.pi / 2, math.pi, 3 * math.pi / 2):
            rotated = _rotate(segs, -theta + extra)
            for mirror in (1, -1):
                variant = [((p[0], p[1] * mirror), (r[0], r[1] * mirror)) for p, r in rotated]
                key = _quantized(variant, q)
                if best is None or key < best:
                    best = key
    x0, y0, x1, y1 = _local_points(segs)
    w, h = max(x1 - x0, EPS), max(y1 - y0, EPS)
    return (motif["nseg"], round(min(w, h) / max(w, h), 1), best)


_DIGITS = set("0123456789")


def text_shape_class(text: str) -> str:
    out = []
    prev = None
    for ch in text:
        if ch in _DIGITS:
            kind = "#"
        elif ch.isalpha():
            kind = "A"
        elif ch.isspace():
            kind = "_"
        else:
            kind = ch
        if kind in ("#", "A", "_") and kind == prev:
            continue
        out.append(kind)
        prev = kind
    return "".join(out)[:16]


def text_context(motif, texts, *, max_ratio: float = 2.0) -> tuple:
    cx, cy = motif["center"]
    radius = max(motif["diag"], EPS) * max_ratio
    best = None
    for t in texts:
        bx0, by0, bx1, by1 = t["bbox"]
        tx, ty = (bx0 + bx1) / 2, (by0 + by1) / 2
        d = math.hypot(tx - cx, ty - cy)
        if d <= radius and (best is None or d < best[0]):
            best = (d, t, tx, ty)
    if best is None:
        return (False, "", -1, -1)
    d, t, tx, ty = best
    sector = int((math.degrees(math.atan2(ty - cy, tx - cx)) % 360) // 45)
    dist_bucket = min(int(d / max(motif["diag"], EPS) * 2), 4)
    return (True, text_shape_class(t["text"]), sector, dist_bucket)


def relation_context(motif, bundle, *, tol: float = 1.2) -> tuple:
    """How many network (long) segments touch the motif, and is it inside a big rect."""
    segments = bundle["segments"]
    node_pts = bundle["node_points"]
    motif_nodes = {n for pair in motif["nodes"] for n in pair}
    motif_pts = [node_pts[n] for n in motif_nodes]
    x0, y0, x1, y1 = motif["bbox"]
    pad = tol
    touch = 0
    for i in bundle["network_index"].query(motif["bbox"], pad):
        seg = segments[i]
        a, b = seg["p0"], seg["p1"]
        hit = False
        for p in motif_pts:
            if _point_seg_dist(p, a, b) <= tol:
                hit = True
                break
        if hit:
            touch += 1
    enclosed = bundle["rect_index"].contains(motif["bbox"])
    return (min(touch, 4), enclosed)


def _point_seg_dist(p, a, b) -> float:
    ax, ay = a
    bx, by = b
    dx, dy = bx - ax, by - ay
    denom = dx * dx + dy * dy
    if denom < EPS:
        return math.hypot(p[0] - ax, p[1] - ay)
    t = max(0.0, min(1.0, ((p[0] - ax) * dx + (p[1] - ay) * dy) / denom))
    return math.hypot(p[0] - (ax + t * dx), p[1] - (ay + t * dy))


class SegGrid:
    def __init__(self, segments, indexes, cell: float) -> None:
        self.cell = max(cell, EPS)
        self.grid: dict[tuple[int, int], list[int]] = defaultdict(list)
        for i in indexes:
            s = segments[i]
            x0, x1 = sorted((s["p0"][0], s["p1"][0]))
            y0, y1 = sorted((s["p0"][1], s["p1"][1]))
            for gx in range(int(x0 // self.cell), int(x1 // self.cell) + 1):
                for gy in range(int(y0 // self.cell), int(y1 // self.cell) + 1):
                    self.grid[(gx, gy)].append(i)

    def query(self, bbox, pad: float) -> set[int]:
        x0, y0, x1, y1 = bbox[0] - pad, bbox[1] - pad, bbox[2] + pad, bbox[3] + pad
        out: set[int] = set()
        for gx in range(int(x0 // self.cell), int(x1 // self.cell) + 1):
            for gy in range(int(y0 // self.cell), int(y1 // self.cell) + 1):
                out.update(self.grid.get((gx, gy), ()))
        return out


class RectIndex:
    def __init__(self, rects) -> None:
        self.rects = rects

    def contains(self, bbox) -> bool:
        for r in self.rects:
            if r[0] <= bbox[0] and r[1] <= bbox[1] and r[2] >= bbox[2] and r[3] >= bbox[3]:
                area_r = (r[2] - r[0]) * (r[3] - r[1])
                area_b = max((bbox[2] - bbox[0]) * (bbox[3] - bbox[1]), EPS)
                if area_r > 2.0 * area_b:
                    return True
        return False


def enrich_bundle(bundle: dict[str, Any], desc: dict[str, Any]) -> None:
    segments = bundle["segments"]
    net = bundle["network"] or list(range(len(segments)))
    bundle["network_index"] = SegGrid(segments, net, max(bundle["block_diag"] * 0.01, 1.0))
    rects = []
    for prim in desc["geometry"]["primitives"]:
        if prim["segment_count"] in (4, 5) and prim.get("closed"):
            b = prim["raw"]["bbox"]
            rects.append(tuple(b))
    bundle["rect_index"] = RectIndex(rects[:4000])


SIGNATURES = ("S1", "S2", "S3", "S4", "S5", "S5c")


def signatures_for(motif, bundle, texts, q: float = 0.05) -> dict[str, str]:
    segments = bundle["segments"]
    core = geom_core(motif, segments, q)
    out = {"S1": _hash(("S1", core))}
    out["S2"] = _hash(("S2", core, degree_profile(motif)))
    out["S3"] = _hash(("S3", core, text_context(motif, texts)))
    out["S4"] = _hash(("S4", core, relation_context(motif, bundle)))
    out["S5"] = _hash(("S5", geom_core_d4(motif, segments, q)))
    out["S5c"] = _hash(("S5c", geom_core_rot(motif, segments, q)))
    return out
