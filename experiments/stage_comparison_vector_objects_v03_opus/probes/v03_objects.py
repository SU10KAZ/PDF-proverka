# -*- coding: utf-8 -*-
"""VECTOR 0.3 · graphical OBJECT LAYER built on top of ``v03_foundation``.

Research code.  Nothing outside
``experiments/stage_comparison_vector_objects_v03_opus/`` is written or modified.

Contract
--------
    layer = build_objects(extract, **params)      # extract == v03_foundation.BlockExtract

``extract`` is read through ``v03_foundation.extract_block`` ONLY.  This file never
opens a PDF for geometry.  (It does read a per-path *style sidecar* — width / colour /
dash / optional-content layer — joined by the ``path`` index that the foundation already
stamps on every segment; the join is verified in ``grp_g4_style_layer.py``.)

Design constraints inherited from measurement, not taste
-------------------------------------------------------
* everything in **PDF points** (v0.2: tolerances in fractions of the block lose 11.7 %);
* ink filter is upstream (foundation), never re-implemented here;
* dashed runs are consolidated before clustering (v0.2: without it 25 -> 92 objects);
* a hard cap on symbol diagonal (v0.2: without it 25 -> 5 objects);
* **arc/circle closure BEFORE classification** — new in v0.3.  v0.2's R12 failure
  ("34 circles on the left, 147 curves on the right, 16 circles vanished") was a pure
  export artefact: the same circle written as 4 cubic Beziers, as 24 chords or as 5
  chords must produce ONE primitive with ONE descriptor;
* object_id derived from content (shape + position), never from enumeration order.

Classes emitted: ``symbol`` | ``linear`` | ``area`` | ``composite`` | ``stray``.
No discipline vocabulary anywhere in this file.
"""

from __future__ import annotations

import hashlib
import math
import statistics
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

# ------------------------------------------------------------------ parameters

DEFAULTS: dict[str, Any] = {
    # characteristic scale
    "scale_mode": "auto",     # auto | text | geom
    "S_override": None,       # force the characteristic scale (shared between two sides)
    "s_text_min_lines": 5,
    # arc closure
    "arc_enable": True,
    "arc_min_pts": 4,          # a chain needs this many points to be fitted
    "arc_resid_rel": 0.06,     # max residual / radius
    "arc_min_span_deg": 45.0,
    "arc_canon_step_deg": 10.0,
    # dashed runs
    "dash_merge": True,
    "dash_max": 2.5,           # dash candidate: length <= dash_max * S
    "dash_gap": 2.0,
    "dash_min": 4,
    "dash_ang_q": 2.0,
    "dash_rho_q": 0.15,
    "dash_len_cv": 0.25,
    "dash_gap_cv": 0.40,
    # connectivity / grouping
    "weld_order": "sorted",   # "sorted" | "input" (input = order-dependent, measured)
    "node_tol": 0.05,          # endpoint welding tolerance, in units of S
    "k_long": 6.0,             # segment longer than k_long*S is a "long run"
    "alpha": 0.60,             # symbol-core merge radius, in units of S
    "diag_max": 8.0,           # HARD symbol-scale cap, in units of S
    "min_seg": 2,
    "max_members": 4000,
    "max_neighbors": 64,       # per-core candidate cap (keeps merging near-linear)
    # style as a grouping signal (G4); off by default so it can be measured against
    "style_split": False,      # refuse to merge cores of different width/colour
    "style_w_tol": 0.15,       # points
    # labels
    "label_r": 1.6,
}

SCHEMA = "v03-object-layer-1"


# ------------------------------------------------------------------ small helpers

class _UF:
    __slots__ = ("p",)

    def __init__(self, n: int) -> None:
        self.p = list(range(n))

    def find(self, a: int) -> int:
        p = self.p
        while p[a] != a:
            p[a] = p[p[a]]
            a = p[a]
        return a

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _bbox_of_pts(pts: Sequence[Sequence[float]]) -> tuple[float, float, float, float]:
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return (min(xs), min(ys), max(xs), max(ys))


def _bbox_gap(a, b) -> float:
    gx = max(0.0, max(a[0] - b[2], b[0] - a[2]))
    gy = max(0.0, max(a[1] - b[3], b[1] - a[3]))
    return math.hypot(gx, gy)


def _bbox_union(a, b):
    return (min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3]))


# ------------------------------------------------------------------ arc closure

def _fit_circle(pts):
    """Kasa algebraic circle fit.  Returns (cx, cy, r, max_resid) or None."""
    n = len(pts)
    if n < 3:
        return None
    sx = sy = sxx = syy = sxy = sxz = syz = sz = 0.0
    for x, y in pts:
        z = x * x + y * y
        sx += x; sy += y; sz += z
        sxx += x * x; syy += y * y; sxy += x * y
        sxz += x * z; syz += y * z
    a11 = 2 * (sxx - sx * sx / n)
    a12 = 2 * (sxy - sx * sy / n)
    a22 = 2 * (syy - sy * sy / n)
    b1 = sxz - sx * sz / n
    b2 = syz - sy * sz / n
    det = a11 * a22 - a12 * a12
    if abs(det) < 1e-12:
        return None
    cx = (b1 * a22 - b2 * a12) / det
    cy = (a11 * b2 - a12 * b1) / det
    r2 = sz / n - 2 * cx * sx / n - 2 * cy * sy / n + cx * cx + cy * cy
    if r2 <= 1e-12:
        return None
    r = math.sqrt(r2)
    resid = max(abs(math.hypot(x - cx, y - cy) - r) for x, y in pts)
    return (cx, cy, r, resid)


def _seg_chains(segs, tol, weld_order="sorted"):
    """Maximal polyline chains through clean degree-2 endpoint junctions.

    Deliberately independent of the ``path`` index: an exporter that writes one path
    per segment (A1) and one that writes one path per polyline (A2) must produce the
    SAME chains, otherwise every descriptor moves when only the packaging changed.
    """
    n = len(segs)
    cell = max(tol, 1e-9)
    nodes: list[tuple[float, float]] = []
    buckets: dict[tuple[int, int], list[int]] = {}

    def nid(pt):
        gx, gy = int(math.floor(pt[0] / cell)), int(math.floor(pt[1] / cell))
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for c in buckets.get((gx + dx, gy + dy), ()):
                    if math.hypot(nodes[c][0] - pt[0], nodes[c][1] - pt[1]) <= tol:
                        return c
        nodes.append((float(pt[0]), float(pt[1])))
        buckets.setdefault((gx, gy), []).append(len(nodes) - 1)
        return len(nodes) - 1

    # Endpoints are welded in COORDINATE order, not input order.  Greedy welding in
    # input order is what made the layer sensitive to A5_order_shuffle (measured: the
    # worst block kept only 0.353 of its ink in 1:1 objects); sorting removes the leak.
    ends_pt: list[list[int]] = [[-1, -1] for _ in range(n)]
    entries = []
    for i, s in enumerate(segs):
        entries.append((s["p0"][0], s["p0"][1], i, 0))
        entries.append((s["p1"][0], s["p1"][1], i, 1))
    if weld_order == "sorted":
        entries.sort(key=lambda e: (e[0], e[1], e[2], e[3]))
    for (x, y, i, side) in entries:
        ends_pt[i][side] = nid((x, y))
    ends: list[tuple[int, int]] = [(a, b) for a, b in ends_pt]
    incident: dict[int, list[int]] = {}
    for i in range(n):
        incident.setdefault(ends[i][0], []).append(i)
        incident.setdefault(ends[i][1], []).append(i)

    visited = [False] * n
    chains: list[list[int]] = []
    order = sorted(range(n), key=lambda i: (segs[i]["p0"][1], segs[i]["p0"][0],
                                            segs[i]["p1"][1], segs[i]["p1"][0]))
    for i in order:
        if visited[i]:
            continue
        visited[i] = True
        chain = [i]
        head, tail = ends[i]
        for side in (0, 1):
            cur = head if side == 0 else tail
            while True:
                inc = incident.get(cur, ())
                if len(inc) != 2:
                    break
                nxt = [j for j in inc if not visited[j]]
                if len(nxt) != 1:
                    break
                j = nxt[0]
                visited[j] = True
                if side == 0:
                    chain.insert(0, j)
                else:
                    chain.append(j)
                cur = ends[j][1] if ends[j][0] == cur else ends[j][0]
        chains.append(chain)
    return chains, ends


def _ordered_points(segs, chain, ends):
    """Point sequence along a chain, orienting each segment to follow the previous one."""
    if len(chain) == 1:
        s = segs[chain[0]]
        return [tuple(s["p0"]), tuple(s["p1"])]
    a0, b0 = ends[chain[0]]
    a1, b1 = ends[chain[1]]
    start_node = a0 if a0 not in (a1, b1) else b0
    pts = []
    cur_node = start_node
    for k, gi in enumerate(chain):
        s = segs[gi]
        a, b = ends[gi]
        if a == cur_node:
            p, q = tuple(s["p0"]), tuple(s["p1"])
            cur_node = b
        else:
            p, q = tuple(s["p1"]), tuple(s["p0"])
            cur_node = a
        if k == 0:
            pts.append(p)
        pts.append(q)
    return pts


def _build_primitives(segs, p, S):
    """Group segments into primitives; close near-circular chains into canonical arcs.

    A primitive is either a polyline chain or an ARC.  The arc carries a canonical
    resampling of the *fitted* circle, which is what makes the descriptor survive
    ``circle -> 4 Bezier -> 24 chords -> 5 chords`` rewrites (v0.2 R12).
    """
    n = len(segs)
    prims: list[dict] = []
    seg2prim = [-1] * n
    if n == 0:
        return prims, seg2prim, {"arc_chains": 0, "arc_segments": 0, "chains": 0}

    tol = max(p["node_tol"] * S, 0.02)
    chains, ends = _seg_chains(segs, tol, p.get("weld_order", "sorted"))

    n_arc_chains = 0
    n_arc_segments = 0
    for ch in chains:
        pts = _ordered_points(segs, ch, ends)
        seg_len = sum(segs[g]["len"] for g in ch)
        made_arc = False
        if p["arc_enable"] and len(pts) >= p["arc_min_pts"]:
            fit = _fit_circle(pts)
            if fit is not None:
                cx, cy, r, resid = fit
                if r > 1e-6 and resid / r <= p["arc_resid_rel"]:
                    angs = [math.atan2(y - cy, x - cx) for x, y in pts]
                    span = 0.0
                    for k in range(1, len(angs)):
                        d = angs[k] - angs[k - 1]
                        while d > math.pi:
                            d -= 2 * math.pi
                        while d < -math.pi:
                            d += 2 * math.pi
                        span += d
                    span_deg = abs(math.degrees(span))
                    if span_deg >= p["arc_min_span_deg"]:
                        a0 = angs[0]
                        step = math.radians(p["arc_canon_step_deg"])
                        m = max(2, int(round(math.radians(span_deg) / step)))
                        sgn = 1.0 if span >= 0 else -1.0
                        canon = [(cx + r * math.cos(a0 + sgn * math.radians(span_deg) * k / m),
                                  cy + r * math.sin(a0 + sgn * math.radians(span_deg) * k / m))
                                 for k in range(m + 1)]
                        prims.append({
                            "kind": "arc", "members": ch, "pts": canon,
                            "cx": cx, "cy": cy, "r": r, "span_deg": round(span_deg, 2),
                            "closed": span_deg >= 350.0,
                            "len": seg_len, "canon_len": 2 * math.pi * r * span_deg / 360.0,
                            "style": (segs[ch[0]]["w"], segs[ch[0]]["color"]),
                        })
                        made_arc = True
                        n_arc_chains += 1
                        n_arc_segments += len(ch)
        if not made_arc:
            closed = (len(pts) > 3 and math.hypot(pts[0][0] - pts[-1][0],
                                                  pts[0][1] - pts[-1][1]) <= tol)
            prims.append({
                "kind": "poly", "members": ch, "pts": pts,
                "len": seg_len, "canon_len": seg_len, "closed": closed,
                "style": (segs[ch[0]]["w"], segs[ch[0]]["color"]),
            })
        pi = len(prims) - 1
        for g in ch:
            seg2prim[g] = pi
    for pr in prims:
        pr["bbox"] = _bbox_of_pts(pr["pts"])
    return prims, seg2prim, {"arc_chains": n_arc_chains, "arc_segments": n_arc_segments,
                             "chains": len(chains)}


# ------------------------------------------------------------------ dashed runs

def _dash_runs(segs, S, p):
    dash_max = p["dash_max"] * S
    gap_max = p["dash_gap"] * S
    ang_q = p["dash_ang_q"]
    rho_q = max(p["dash_rho_q"] * S, 1e-6)
    buckets: dict[tuple[int, int], list[int]] = {}
    for s in segs:
        if s["len"] > dash_max:
            continue
        dx = s["p1"][0] - s["p0"][0]
        dy = s["p1"][1] - s["p0"][1]
        L = s["len"]
        ux, uy = dx / L, dy / L
        if ux < 0 or (abs(ux) < 1e-9 and uy < 0):
            ux, uy = -ux, -uy
        nx, ny = -uy, ux
        rho = s["p0"][0] * nx + s["p0"][1] * ny
        ang = math.degrees(math.atan2(uy, ux)) % 180.0
        buckets.setdefault((int(round(ang / ang_q)), int(round(rho / rho_q))), []).append(s["i"])
    runs: list[list[int]] = []
    # deterministic bucket order: dashed-run assembly must not depend on input order
    for _key in sorted(buckets):
        members = buckets[_key]
        if len(members) < p["dash_min"]:
            continue
        proj = []
        for gi in members:
            s = segs[gi]
            dx = s["p1"][0] - s["p0"][0]
            dy = s["p1"][1] - s["p0"][1]
            L = s["len"]
            ux, uy = dx / L, dy / L
            if ux < 0 or (abs(ux) < 1e-9 and uy < 0):
                ux, uy = -ux, -uy
            t0 = s["p0"][0] * ux + s["p0"][1] * uy
            t1 = s["p1"][0] * ux + s["p1"][1] * uy
            proj.append((min(t0, t1), max(t0, t1),
                         s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1], gi))
        proj.sort()

        def flush(ch, gs):
            if len(ch) < p["dash_min"]:
                return
            lens = [t[1] - t[0] for t in ch]
            m = statistics.mean(lens)
            if m <= 0:
                return
            if len(lens) > 1 and statistics.pstdev(lens) / m > p["dash_len_cv"]:
                return
            if gs and len(gs) > 1 and statistics.mean(gs) > 0 and \
                    statistics.pstdev(gs) / statistics.mean(gs) > p["dash_gap_cv"]:
                return
            runs.append([t[-1] for t in ch])

        chain = [proj[0]]
        gaps: list[float] = []
        for cur in proj[1:]:
            gap = cur[0] - chain[-1][1]
            if gap <= gap_max:
                gaps.append(max(gap, 0.0))
                chain.append(cur)
            else:
                flush(chain, gaps)
                chain, gaps = [cur], []
        flush(chain, gaps)
    used = set()
    for r in runs:
        used.update(r)
    return runs, used


# ------------------------------------------------------------------ connectivity

def _node_ids(prim_ids, prims, tol):
    """Weld primitive endpoints into shared nodes (grid hash, O(n))."""
    cell = max(tol, 1e-9)
    buckets: dict[tuple[int, int], list[int]] = {}
    nodes: list[tuple[float, float]] = []
    ids: dict[tuple[int, int], int] = {}

    def nid(pt):
        gx, gy = int(math.floor(pt[0] / cell)), int(math.floor(pt[1] / cell))
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for c in buckets.get((gx + dx, gy + dy), ()):
                    if math.hypot(nodes[c][0] - pt[0], nodes[c][1] - pt[1]) <= tol:
                        return c
        nodes.append((float(pt[0]), float(pt[1])))
        buckets.setdefault((gx, gy), []).append(len(nodes) - 1)
        return len(nodes) - 1

    entries = []
    for pidx in prim_ids:
        pr = prims[pidx]
        entries.append((pr["pts"][0][0], pr["pts"][0][1], pidx, 0))
        entries.append((pr["pts"][-1][0], pr["pts"][-1][1], pidx, 1))
    entries.sort(key=lambda e: (e[0], e[1], e[2], e[3]))
    for (x, y, pidx, side) in entries:
        ids[(pidx, side)] = nid((x, y))
    return ids, len(nodes)


def _components(prim_ids, prims, tol):
    """Connected components of the primitive endpoint graph."""
    if not prim_ids:
        return []
    ids, n_nodes = _node_ids(prim_ids, prims, tol)
    idx = {p: k for k, p in enumerate(prim_ids)}
    uf = _UF(len(prim_ids))
    by_node: dict[int, int] = {}
    for pidx in prim_ids:
        for side in (0, 1):
            nd = ids[(pidx, side)]
            if nd in by_node:
                uf.union(idx[by_node[nd]], idx[pidx])
            else:
                by_node[nd] = pidx
    groups: dict[int, list[int]] = {}
    for pidx in prim_ids:
        groups.setdefault(uf.find(idx[pidx]), []).append(pidx)
    out = []
    for members in groups.values():
        nodes = set()
        for pidx in members:
            nodes.add(ids[(pidx, 0)])
            nodes.add(ids[(pidx, 1)])
        out.append((members, len(nodes), len(members)))
    return out


def _merge_cores(cores, radius, diag_cap, max_neighbors, style_split, style_w_tol):
    """Single-linkage merge of symbol cores with a HARD cap on the merged diagonal."""
    n = len(cores)
    if n == 0:
        return []
    cell = max(radius, 1e-6)
    grid: dict[tuple[int, int], list[int]] = {}
    for i, c in enumerate(cores):
        b = c["bbox"]
        cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
        grid.setdefault((int(math.floor(cx / cell)), int(math.floor(cy / cell))), []).append(i)
    uf = _UF(n)
    cur = {i: cores[i]["bbox"] for i in range(n)}
    order = sorted(range(n), key=lambda i: (cores[i]["bbox"][1], cores[i]["bbox"][0]))
    for a in order:
        b = cores[a]["bbox"]
        cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
        gx, gy = int(math.floor(cx / cell)), int(math.floor(cy / cell))
        cands: list[int] = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                cands.extend(grid.get((gx + dx, gy + dy), ()))
        if len(cands) > max_neighbors:
            cands.sort(key=lambda i: (cores[i]["bbox"][1], cores[i]["bbox"][0]))
            cands = cands[:max_neighbors]
        for c in cands:
            if c == a:
                continue
            ra, rb = uf.find(a), uf.find(c)
            if ra == rb:
                continue
            if _bbox_gap(cur[ra], cur[rb]) > radius:
                continue
            if style_split:
                wa, ca = cores[a]["style"]
                wb, cb = cores[c]["style"]
                if ca != cb or abs((wa or 0.0) - (wb or 0.0)) > style_w_tol:
                    continue
            m = _bbox_union(cur[ra], cur[rb])
            if math.hypot(m[2] - m[0], m[3] - m[1]) > diag_cap:
                continue
            uf.union(a, c)
            cur[uf.find(a)] = m
    out: dict[int, list[int]] = {}
    for i in range(n):
        out.setdefault(uf.find(i), []).append(i)
    return list(out.values())


# ------------------------------------------------------------------ descriptor

DESC_LEN = 25


def shape_descriptor(prim_ids, prims):
    """25 numbers, insensitive to how the CAD exporter decomposed the object.

    [0]     aspect      w/(w+h)
    [1]     total stroke length / bbox diagonal, capped at 8 and rescaled
    [2:8]   length-weighted histogram of direction, 6 bins of 30 deg (mod 180)
    [8:24]  4x4 occupancy grid of stroke length inside the object's own bbox
    [24]    share of stroke length carried by closed/near-closed ARCS  (v0.3 addition)

    Arcs contribute their CANONICAL resampling of the fitted circle, so the same
    circle written as 4 Beziers, 24 chords or 5 chords lands in the same place.
    """
    pts_all = [q for pidx in prim_ids for q in prims[pidx]["pts"]]
    if not pts_all:
        return {"vec": [0.0] * DESC_LEN, "n_prim": 0, "n_seg": 0, "diag": 0.0,
                "bbox": (0, 0, 0, 0), "total_len": 0.0}
    bb = _bbox_of_pts(pts_all)
    w = max(bb[2] - bb[0], 1e-6)
    h = max(bb[3] - bb[1], 1e-6)
    diag = math.hypot(w, h)
    ang = [0.0] * 6
    grid = [0.0] * 16
    total = 0.0
    arc_len = 0.0
    n_seg = 0
    for pidx in prim_ids:
        pr = prims[pidx]
        n_seg += len(pr["members"])
        pts = pr["pts"]
        if pr["kind"] == "arc":
            arc_len += pr["len"]
        for k in range(len(pts) - 1):
            x0, y0 = pts[k]
            x1, y1 = pts[k + 1]
            L = math.hypot(x1 - x0, y1 - y0)
            if L <= 0:
                continue
            total += L
            a = math.degrees(math.atan2(y1 - y0, x1 - x0)) % 180.0
            ang[min(int(a / 30.0), 5)] += L
            mx, my = (x0 + x1) / 2, (y0 + y1) / 2
            gx = min(int((mx - bb[0]) / w * 4), 3)
            gy = min(int((my - bb[1]) / h * 4), 3)
            grid[gy * 4 + gx] += L
    if total <= 0:
        total = 1.0
    vec = [w / (w + h), min(total / max(diag, 1e-9), 8.0) / 8.0]
    vec += [v / total for v in ang]
    vec += [v / total for v in grid]
    vec += [arc_len / total]
    return {"vec": vec, "n_prim": len(prim_ids), "n_seg": n_seg,
            "diag": diag, "bbox": bb, "total_len": total}


def descriptor_distance(a, b) -> float:
    return sum(abs(x - y) for x, y in zip(a["vec"], b["vec"]))


# ------------------------------------------------------------------ object layer

@dataclass
class ObjectLayer:
    objects: list[dict]
    S: float
    scale_source: str
    params: dict
    stats: dict
    prims: list[dict] = field(default_factory=list)
    seg2obj: list[int] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        c: dict[str, int] = {}
        for o in self.objects:
            c[o["cls"]] = c.get(o["cls"], 0) + 1
        return c

    def to_json(self) -> dict:
        return {
            "schema": SCHEMA, "S": self.S, "scale_source": self.scale_source,
            "params": self.params, "stats": self.stats, "counts": self.counts(),
            "objects": [{k: v for k, v in o.items() if k not in ("prims", "desc")}
                        | {"desc": [round(v, 4) for v in o["desc"]["vec"]]}
                        for o in self.objects],
        }


def characteristic_scale(segments, texts, p) -> tuple[float, str, dict]:
    lens = sorted(s["len"] for s in segments)
    s_geom = statistics.median(lens) if lens else 0.0
    sizes = [t["size"] for t in texts if t.get("size", 0) > 0]
    s_text = statistics.median(sizes) if len(sizes) >= p["s_text_min_lines"] else None
    mode = p["scale_mode"]
    if mode == "text":
        S, src = (s_text or s_geom or 1.0), ("text" if s_text else "geom_fallback")
    elif mode == "geom":
        S, src = (s_geom or 1.0), "geom"
    else:
        if s_text:
            S, src = s_text, "text"
        elif s_geom > 0:
            S, src = s_geom, "geom"
        else:
            S, src = 1.0, "none"
    return float(S), src, {"s_text": float(s_text or 0.0), "s_geom": float(s_geom),
                           "n_text": len(texts), "n_seg": len(segments)}


def _object_id(cls: str, desc, cx: float, cy: float) -> str:
    payload = (cls,
               tuple(round(v, 2) for v in desc["vec"]),
               int(round(cx * 2)), int(round(cy * 2)))
    return hashlib.sha1(repr(payload).encode()).hexdigest()[:12]


def build_objects(extract, **params) -> ObjectLayer:
    """Group the inked segments of ONE prepared block into generic graphical objects."""
    p = dict(DEFAULTS)
    p.update(params or {})

    segs = extract.segments if hasattr(extract, "segments") else extract["segments"]
    texts = extract.texts if hasattr(extract, "texts") else extract.get("texts", [])
    S, scale_src, scale_info = characteristic_scale(segs, texts, p)
    if p.get("S_override"):
        S, scale_src = float(p["S_override"]), "override"

    n = len(segs)
    if n == 0:
        return ObjectLayer([], S, scale_src, p,
                           {"n_seg": 0, "note": "no vector geometry", **scale_info}, [], [])

    prims, seg2prim, prim_stats = _build_primitives(segs, p, S)

    # -- dashed runs first: they must not become phantom symbol clusters ----------
    dash_prims: set[int] = set()
    dash_objects: list[list[int]] = []
    if p["dash_merge"]:
        runs, dash_used = _dash_runs(segs, S, p)
        for run in runs:
            rset = set(run)
            pset = set()
            for g in run:
                pi = seg2prim[g]
                if pi in dash_prims:
                    continue
                if all(g2 in rset for g2 in prims[pi]["members"]):
                    pset.add(pi)
            if len(pset) >= p["dash_min"]:
                dash_objects.append(sorted(pset))
                dash_prims |= pset

    tol = max(p["node_tol"] * S, 0.02)
    long_thr = p["k_long"] * S
    radius = p["alpha"] * S
    diag_max = p["diag_max"] * S

    free = [i for i in range(len(prims)) if i not in dash_prims]
    comps = _components(free, prims, tol)

    cores: list[dict] = []
    raw_objects: list[dict] = []
    stats = {"components": len(comps), "cores": 0, "areas": 0, "linear": 0,
             "composite": 0, "decomposed": 0, "dash_runs": len(dash_objects)}
    stats.update(prim_stats)

    for run in dash_objects:
        raw_objects.append({"cls": "linear", "prims": run, "dashed": True, "cycle": False})

    for members, n_nodes, n_edges in comps:
        bb = _bbox_of_pts([q for pidx in members for q in prims[pidx]["pts"]])
        diag = math.hypot(bb[2] - bb[0], bb[3] - bb[1])
        cycle = n_edges >= n_nodes or any(prims[pidx].get("closed") for pidx in members)
        if diag <= diag_max:
            cores.append({"prims": members, "bbox": bb, "cycle": cycle,
                          "style": prims[members[0]]["style"]})
            stats["cores"] += 1
            continue
        if cycle and len(members) <= 4:
            raw_objects.append({"cls": "area", "prims": members, "cycle": True})
            stats["areas"] += 1
            continue
        # big component: peel off the long runs, re-componentise the rest
        stats["decomposed"] += 1
        long_m = [pidx for pidx in members if prims[pidx]["len"] > long_thr]
        short_m = [pidx for pidx in members if prims[pidx]["len"] <= long_thr]
        for sub, _n, _e in _components(long_m, prims, tol):
            raw_objects.append({"cls": "linear", "prims": sub, "cycle": False})
            stats["linear"] += 1
        if short_m and len(short_m) <= p["max_members"]:
            for sub, sn, se in _components(short_m, prims, tol):
                sbb = _bbox_of_pts([q for pidx in sub for q in prims[pidx]["pts"]])
                sdiag = math.hypot(sbb[2] - sbb[0], sbb[3] - sbb[1])
                if sdiag <= diag_max:
                    cores.append({"prims": sub, "bbox": sbb,
                                  "cycle": se >= sn, "style": prims[sub[0]]["style"]})
                    stats["cores"] += 1
                else:
                    raw_objects.append({"cls": "composite", "prims": sub, "cycle": se >= sn})
                    stats["composite"] += 1
        elif short_m:
            raw_objects.append({"cls": "composite", "prims": short_m, "cycle": False})
            stats["composite"] += 1

    for group in _merge_cores(cores, radius, diag_max, p["max_neighbors"],
                              p["style_split"], p["style_w_tol"]):
        members = [pidx for ci in group for pidx in cores[ci]["prims"]]
        n_seg_here = sum(len(prims[pidx]["members"]) for pidx in members)
        cycle = any(cores[ci]["cycle"] for ci in group)
        cls = "symbol" if n_seg_here >= p["min_seg"] else "stray"
        raw_objects.append({"cls": cls, "prims": members, "cycle": cycle, "cores": len(group)})

    # -- finalise: geometry, class refinement, descriptor, stable id --------------
    objects: list[dict] = []
    for o in raw_objects:
        desc = shape_descriptor(o["prims"], prims)
        bb = desc["bbox"]
        cx, cy = (bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2
        diag = desc["diag"]
        total = desc["total_len"]
        cls = o["cls"]
        # a "symbol"-scale group that is one long straight run is linear, not a symbol
        if cls == "symbol" and diag > 0 and total / diag < 1.15 and diag > 2.0 * S:
            cls = "linear"
        # a closed contour bigger than symbol scale is an area
        if cls in ("composite",) and o.get("cycle") and total / max(diag, 1e-9) < 4.0:
            cls = "area"
        seg_members = [g for pidx in o["prims"] for g in prims[pidx]["members"]]
        seg_len = sum(segs[g]["len"] for g in seg_members)
        objects.append({
            "seg_len": round(seg_len, 3),
            "cls": cls,
            "prims": o["prims"],
            "segments": seg_members,
            "n_seg": len(seg_members),
            "n_prim": len(o["prims"]),
            "bbox": [round(v, 3) for v in bb],
            "cx": round(cx, 3), "cy": round(cy, 3),
            "diag": round(diag, 3),
            "len": round(total, 3),
            "cycle": bool(o.get("cycle")),
            "dashed": bool(o.get("dashed")),
            "arc_share": round(desc["vec"][24], 4),
            "desc": desc,
        })

    objects.sort(key=lambda o: (o["cy"], o["cx"], -o["n_seg"]))
    seen: dict[str, int] = {}
    for o in objects:
        oid = _object_id(o["cls"], o["desc"], o["cx"], o["cy"])
        k = seen.get(oid, 0)
        seen[oid] = k + 1
        o["object_id"] = oid if k == 0 else f"{oid}#{k}"

    # labels (text anchor only; a label never creates or splits an object)
    lr = p["label_r"] * S
    if texts:
        for o in objects:
            best = None
            for t in texts:
                g = _bbox_gap(o["bbox"], t["bbox"])
                if g <= lr and (best is None or g < best[0]):
                    best = (g, t["text"])
            o["label"] = best[1] if best else None

    seg2obj = [-1] * n
    for oi, o in enumerate(objects):
        for g in o["segments"]:
            seg2obj[g] = oi

    total_len = sum(s["len"] for s in segs)
    assigned = sum(o["seg_len"] for o in objects)
    stray_len = sum(o["seg_len"] for o in objects if o["cls"] == "stray")
    stats.update({
        "n_seg": n, "n_prim": len(prims), "n_obj": len(objects),
        "unassigned_segments": sum(1 for v in seg2obj if v < 0),
        "ink_len_total": round(total_len, 2),
        "ink_len_in_objects": round(assigned, 2),
        "ink_coverage": round(assigned / total_len, 6) if total_len > 0 else 0.0,
        "stray_len_share": round(stray_len / total_len, 6) if total_len > 0 else 0.0,
        "S": S, "scale_source": scale_src, **scale_info,
        "tol": tol, "radius": radius, "diag_max": diag_max, "long_thr": long_thr,
    })
    return ObjectLayer(objects, S, scale_src, p, stats, prims, seg2obj)
