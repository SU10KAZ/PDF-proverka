# -*- coding: utf-8 -*-
"""VECTOR 0.3 · probe `rel` — RELATIONS between graphical objects.

    rels = build_relations(layer, extract, **params)   # -> list[dict]

Built strictly on top of ``v03_objects.build_objects`` (which is built strictly on top
of ``v03_foundation.extract_block``).  This file never opens a PDF.

Design constraints inherited from measurement, not taste
-------------------------------------------------------
* every tolerance is in **PDF points** — either absolute, or k * S where S is the
  block's characteristic scale (also points).  Nothing is expressed as a fraction of
  the block bbox (v0.2: at 10 % crop the cluster count moved by -11.7 %);
* object TYPING by normalised size / proportion is never part of a relation key
  (it is not crop-invariant).  The object *class* from the layer is used only as a
  filter where the geometry itself demands it (a closed contour for INSIDE);
* a relation is not a score.  It is an ADDRESS: "this object, the one connected to X".
  Therefore every relation carries the evidence needed to print that phrase.
* no all-pairs graph: every type is either endpoint-local (grid index) or star-shaped
  (nearest neighbour only).  Cost stays near-linear in the number of objects.

Types emitted (the 9 candidates of BRIEF §8):
    CONNECTED_TO, PART_OF, INSIDE, CONTAINS, ADJACENT, ALIGNED,
    LEADER_TO, LABEL_ANCHOR, REPEATED_WITH
"""
from __future__ import annotations

import math
from typing import Any, Optional, Sequence

REL_TYPES = ["CONNECTED_TO", "PART_OF", "INSIDE", "CONTAINS", "ADJACENT",
             "ALIGNED", "LEADER_TO", "LABEL_ANCHOR", "REPEATED_WITH"]

DEFAULTS: dict[str, Any] = {
    # CONNECTED_TO — CAD endpoints coincide or are a rounding apart: ABSOLUTE points
    "conn_abs_pt": 0.35,
    "conn_k_S": 0.05,           # + a small share of the characteristic scale
    # ADJACENT — "near" must scale with the drawing, hence k * S
    "adj_k_S": 1.0,
    "adj_max_pt": 40.0,         # but never more than this many points
    # INSIDE / CONTAINS
    "inside_mode": "polygon",   # "polygon" (point-in-ring) | "bbox"
    "inside_ratio": 2.0,        # container diagonal must exceed this * inner diagonal
    "inside_margin_pt": 0.0,
    # ALIGNED
    "align_k_S": 0.10,
    "align_min_gap_k_S": 1.0,
    "align_max_gap_k_S": 60.0,
    # LEADER_TO
    "leader_min_k_S": 1.5,
    "leader_max_k_S": 60.0,
    "leader_max_prim": 3,
    "leader_tip_k_S": 0.6,      # free end must land this close to the target ink
    "leader_text_k_S": 1.6,
    "leader_require_shelf": False,
    # LABEL_ANCHOR
    "label_k_S": 1.6,
    "label_mutual": False,
    # REPEATED_WITH
    "rep_desc_eps": 0.05,
    "rep_diag_ratio": 1.15,
    "rep_min_group": 2,
    # cost guards
    "max_objects": 20000,
    "grid_cell_k_S": 2.0,
    "max_neighbors": 48,
}


# ------------------------------------------------------------------ small geometry

def _bbox_gap(a, b) -> float:
    dx = max(a[0] - b[2], b[0] - a[2], 0.0)
    dy = max(a[1] - b[3], b[1] - a[3], 0.0)
    return math.hypot(dx, dy)


def _bbox_inside(inner, outer, margin=0.0) -> bool:
    return (inner[0] >= outer[0] - margin and inner[1] >= outer[1] - margin
            and inner[2] <= outer[2] + margin and inner[3] <= outer[3] + margin)


def _pt_seg_d2(px, py, x0, y0, x1, y1) -> float:
    dx, dy = x1 - x0, y1 - y0
    L2 = dx * dx + dy * dy
    if L2 <= 1e-12:
        return (px - x0) ** 2 + (py - y0) ** 2
    t = ((px - x0) * dx + (py - y0) * dy) / L2
    t = 0.0 if t < 0 else (1.0 if t > 1 else t)
    return (px - (x0 + t * dx)) ** 2 + (py - (y0 + t * dy)) ** 2


def _point_in_ring(px, py, ring) -> bool:
    inside = False
    n = len(ring)
    for k in range(n):
        x0, y0 = ring[k]
        x1, y1 = ring[(k + 1) % n]
        if (y0 > py) != (y1 > py):
            xin = x0 + (py - y0) * (x1 - x0) / (y1 - y0 + 1e-30)
            if px < xin:
                inside = not inside
    return inside


class _Grid:
    """Uniform grid over polyline points, tagged with the owning object index."""

    def __init__(self, cell: float):
        self.cell = max(cell, 1e-3)
        self.g: dict[tuple[int, int], list[int]] = {}
        self.owners: dict[tuple[int, int], set] = {}
        self.seg: list[tuple[float, float, float, float, int]] = []

    def add_polyline(self, pts, owner: int):
        c = self.cell
        for k in range(len(pts) - 1):
            x0, y0 = pts[k]
            x1, y1 = pts[k + 1]
            idx = len(self.seg)
            self.seg.append((x0, y0, x1, y1, owner))
            n = max(1, int(math.hypot(x1 - x0, y1 - y0) / c) + 1)
            seen = set()
            for j in range(n + 1):
                t = j / n
                key = (int(math.floor((x0 + t * (x1 - x0)) / c)),
                       int(math.floor((y0 + t * (y1 - y0)) / c)))
                if key in seen:
                    continue
                seen.add(key)
                self.g.setdefault(key, []).append(idx)
                self.owners.setdefault(key, set()).add(owner)

    def near(self, px, py, tol) -> list[tuple[float, int]]:
        """All (distance, owner) with distance <= tol."""
        c = self.cell
        r = int(math.ceil(tol / c))
        gx, gy = int(math.floor(px / c)), int(math.floor(py / c))
        best: dict[int, float] = {}
        t2 = tol * tol
        for dx in range(-r, r + 1):
            for dy in range(-r, r + 1):
                for si in self.g.get((gx + dx, gy + dy), ()):
                    x0, y0, x1, y1, ow = self.seg[si]
                    d2 = _pt_seg_d2(px, py, x0, y0, x1, y1)
                    if d2 <= t2 and (ow not in best or d2 < best[ow]):
                        best[ow] = d2
        return sorted((math.sqrt(v), k) for k, v in best.items())


# ------------------------------------------------------------------ helpers on the layer

def object_rings(layer) -> dict[int, list]:
    """Closed point rings of each object (for polygon-INSIDE)."""
    out: dict[int, list] = {}
    for oi, o in enumerate(layer.objects):
        rings = []
        for pi in o["prims"]:
            pr = layer.prims[pi]
            if pr.get("closed") and len(pr["pts"]) >= 4:
                rings.append(pr["pts"])
        if rings:
            out[oi] = max(rings, key=lambda r: _ring_area(r))
    return out


def _ring_area(ring) -> float:
    a = 0.0
    n = len(ring)
    for k in range(n):
        x0, y0 = ring[k]
        x1, y1 = ring[(k + 1) % n]
        a += x0 * y1 - x1 * y0
    return abs(a) / 2.0


def object_endpoints(layer, oi) -> list[tuple[float, float]]:
    """Free endpoints (degree-1 nodes) of the object's primitive graph."""
    o = layer.objects[oi]
    cnt: dict[tuple[int, int], int] = {}
    pts: dict[tuple[int, int], tuple[float, float]] = {}
    q = 100.0
    for pi in o["prims"]:
        pr = layer.prims[pi]
        if pr.get("closed"):
            continue
        for p in (pr["pts"][0], pr["pts"][-1]):
            k = (int(round(p[0] * q)), int(round(p[1] * q)))
            cnt[k] = cnt.get(k, 0) + 1
            pts[k] = p
    return [pts[k] for k, v in cnt.items() if v == 1]


def _desc_key(o, eps) -> tuple:
    return tuple(int(round(v / eps)) for v in o["desc"]["vec"])


# ------------------------------------------------------------------ the builder

def build_relations(layer, extract=None, **params) -> list[dict]:
    p = dict(DEFAULTS)
    p.update(params or {})
    S = float(layer.S) if layer.S else 1.0
    objs = layer.objects
    n = len(objs)
    if n == 0 or n > p["max_objects"]:
        return []
    texts = []
    if extract is not None:
        texts = extract.texts if hasattr(extract, "texts") else extract.get("texts", [])

    conn_tol = p["conn_abs_pt"] + p["conn_k_S"] * S
    adj_tol = min(p["adj_k_S"] * S, p["adj_max_pt"])

    # fine grid: point queries (connectivity, leader tips).  Cell tied to the TOLERANCE,
    # not to S, so a dense cell never holds hundreds of segments.
    grid = _Grid(max(2.0 * conn_tol, 0.5))
    # coarse grid: adjacency CANDIDATES only (cell -> set of objects present)
    cgrid = _Grid(max(adj_tol, 1.0))
    for oi, o in enumerate(objs):
        for pi in o["prims"]:
            pts = layer.prims[pi]["pts"]
            grid.add_polyline(pts, oi)
            cgrid.add_polyline(pts, oi)

    rels: list[dict] = []
    add = rels.append

    # ---------------- CONNECTED_TO : an endpoint of A lands on the ink of B ---------
    connected: set[tuple[int, int]] = set()
    for oi in range(n):
        for (ex, ey) in object_endpoints(layer, oi):
            for d, ow in grid.near(ex, ey, conn_tol):
                if ow == oi:
                    continue
                key = (min(oi, ow), max(oi, ow))
                if key in connected:
                    continue
                connected.add(key)
                add({"type": "CONNECTED_TO", "a": key[0], "b": key[1],
                     "sym": True, "d_pt": round(d, 4),
                     "at": [round(ex, 3), round(ey, 3)], "tol_pt": round(conn_tol, 4)})

    # ---------------- INSIDE / CONTAINS -------------------------------------------
    rings = object_rings(layer)
    # candidate containers, biggest first; a small object is tested against the
    # smallest container that holds it (nearest enclosing), never against all.
    containers = sorted((oi for oi in range(n)
                         if (oi in rings or objs[oi]["cycle"]) and objs[oi]["diag"] > 0),
                        key=lambda oi: -objs[oi]["diag"])
    cbb = [(oi, objs[oi]["bbox"]) for oi in containers]
    inside_pairs: list[tuple[int, int]] = []
    for oi, o in enumerate(objs):
        bb = o["bbox"]
        best = None
        for ci, cb in cbb:
            if ci == oi:
                continue
            if objs[ci]["diag"] < p["inside_ratio"] * max(o["diag"], 1e-6):
                continue
            if not _bbox_inside(bb, cb, p["inside_margin_pt"]):
                continue
            if p["inside_mode"] == "polygon" and ci in rings:
                ring = rings[ci]
                cx, cy = o["cx"], o["cy"]
                probes = [(cx, cy), (bb[0], bb[1]), (bb[2], bb[1]),
                          (bb[0], bb[3]), (bb[2], bb[3])]
                hits = sum(1 for q in probes if _point_in_ring(q[0], q[1], ring))
                if hits < 5:
                    continue
            elif p["inside_mode"] == "polygon":
                continue
            area = max(cb[2] - cb[0], 1e-9) * max(cb[3] - cb[1], 1e-9)
            if best is None or area < best[1]:
                best = (ci, area)
        if best is not None:
            inside_pairs.append((oi, best[0]))
    for a, b in inside_pairs:
        add({"type": "INSIDE", "a": a, "b": b, "sym": False,
             "mode": p["inside_mode"]})
        add({"type": "CONTAINS", "a": b, "b": a, "sym": False,
             "mode": p["inside_mode"]})

    # ---------------- PART_OF : contained AND touching -----------------------------
    inside_map = {a: b for a, b in inside_pairs}
    for a, b in inside_pairs:
        if (min(a, b), max(a, b)) in connected:
            add({"type": "PART_OF", "a": a, "b": b, "sym": False})

    # ---------------- ADJACENT : close but not connected ---------------------------
    # candidates come from shared coarse cells; the test itself is the bbox gap in points
    adj: set[tuple[int, int]] = set()
    cand: dict[int, set] = {}
    for key, owners in cgrid.owners.items():
        near_owners = set()
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                near_owners |= cgrid.owners.get((key[0] + dx, key[1] + dy), set())
        if len(near_owners) < 2:
            continue
        for oi in owners:
            c = cand.setdefault(oi, set())
            if len(c) <= p["max_neighbors"] * 4:
                c |= near_owners
    for oi, others in cand.items():
        bb = objs[oi]["bbox"]
        hits = []
        for ow in others:
            if ow == oi:
                continue
            key = (min(oi, ow), max(oi, ow))
            if key in connected or key in adj:
                continue
            g = _bbox_gap(bb, objs[ow]["bbox"])
            if g <= adj_tol:
                hits.append((g, ow))
        hits.sort()
        for g, ow in hits[:p["max_neighbors"]]:
            key = (min(oi, ow), max(oi, ow))
            if key in adj:
                continue
            adj.add(key)
            add({"type": "ADJACENT", "a": key[0], "b": key[1], "sym": True,
                 "gap_pt": round(g, 4), "tol_pt": round(adj_tol, 4)})

    # ---------------- ALIGNED : nearest aligned neighbour in each direction ---------
    at = max(p["align_k_S"] * S, 0.05)
    gmin, gmax = p["align_min_gap_k_S"] * S, p["align_max_gap_k_S"] * S
    for axis in (0, 1):
        key_f = (lambda o: o["cy"]) if axis == 0 else (lambda o: o["cx"])   # aligned along x
        pos_f = (lambda o: o["cx"]) if axis == 0 else (lambda o: o["cy"])
        lanes: dict[int, list[int]] = {}
        for oi, o in enumerate(objs):
            lanes.setdefault(int(math.floor(key_f(o) / at)), []).append(oi)
        for lane, members in lanes.items():
            cand = members + lanes.get(lane + 1, [])
            if len(cand) < 2:
                continue
            cand = sorted(set(cand), key=lambda oi: pos_f(objs[oi]))
            for k in range(len(cand) - 1):
                a, b = cand[k], cand[k + 1]
                if abs(key_f(objs[a]) - key_f(objs[b])) > at:
                    continue
                gap = abs(pos_f(objs[b]) - pos_f(objs[a]))
                if gap < gmin or gap > gmax:
                    continue
                add({"type": "ALIGNED", "a": min(a, b), "b": max(a, b), "sym": True,
                     "axis": "x" if axis == 0 else "y",
                     "delta_pt": round(abs(key_f(objs[a]) - key_f(objs[b])), 4),
                     "gap_pt": round(gap, 3), "tol_pt": round(at, 4)})

    # ---------------- LEADER_TO -----------------------------------------------------
    tip_tol = p["leader_tip_k_S"] * S
    txt_tol = p["leader_text_k_S"] * S
    lmin, lmax = p["leader_min_k_S"] * S, p["leader_max_k_S"] * S
    for oi, o in enumerate(objs):
        if o["cls"] != "linear" or o["dashed"] or o["cycle"]:
            continue
        if o["n_prim"] > p["leader_max_prim"] or not (lmin <= o["len"] <= lmax):
            continue
        ends = object_endpoints(layer, oi)
        if len(ends) != 2:
            continue
        # shelf: the object is a polyline with a bend and a short final leg
        shelf = _has_shelf(layer, oi, S)
        if p["leader_require_shelf"] and not shelf:
            continue
        # each end resolves either to a text or to another object's ink
        res = []
        for e in ends:
            near_obj = [(d, ow) for d, ow in grid.near(e[0], e[1], tip_tol) if ow != oi]
            near_txt = _nearest_text(texts, e, txt_tol)
            res.append((near_obj[0] if near_obj else None, near_txt))
        (o0, t0), (o1, t1) = res
        # the useful leader: one end at a text, the other at an object
        pairs = []
        if t0 is not None and o1 is not None:
            pairs.append((o1, t0))
        if t1 is not None and o0 is not None:
            pairs.append((o0, t1))
        for (d, tgt), (td, ttxt) in pairs:
            add({"type": "LEADER_TO", "a": oi, "b": tgt, "sym": False,
                 "text": ttxt, "d_pt": round(d, 4), "text_d_pt": round(td, 4),
                 "shelf": bool(shelf), "resolved": "object+text"})
        if not pairs:
            # a leader that resolves only at one end is still a leader, but its address
            # is half-empty; recorded so the hit-rate can be measured honestly
            tgt = o0 or o1
            txt = t0 or t1
            if tgt is not None and txt is None:
                add({"type": "LEADER_TO", "a": oi, "b": tgt[1], "sym": False,
                     "text": None, "d_pt": round(tgt[0], 4), "shelf": bool(shelf),
                     "resolved": "object_only"})

    # ---------------- LABEL_ANCHOR --------------------------------------------------
    ltol = p["label_k_S"] * S
    if texts:
        # nearest text per object; optionally require mutual nearest
        best_for_obj: dict[int, tuple[float, int]] = {}
        best_for_txt: dict[int, tuple[float, int]] = {}
        for oi, o in enumerate(objs):
            for ti, t in enumerate(texts):
                g = _bbox_gap(o["bbox"], t["bbox"])
                if g > ltol:
                    continue
                if oi not in best_for_obj or g < best_for_obj[oi][0]:
                    best_for_obj[oi] = (g, ti)
                if ti not in best_for_txt or g < best_for_txt[ti][0]:
                    best_for_txt[ti] = (g, oi)
        for oi, (g, ti) in best_for_obj.items():
            if p["label_mutual"] and best_for_txt.get(ti, (None, None))[1] != oi:
                continue
            add({"type": "LABEL_ANCHOR", "a": oi, "b": None, "sym": False,
                 "text_ix": ti, "text": texts[ti].get("text"),
                 "gap_pt": round(g, 4), "tol_pt": round(ltol, 4)})

    # ---------------- REPEATED_WITH : motif groups, star-shaped ---------------------
    groups: dict[tuple, list[int]] = {}
    for oi, o in enumerate(objs):
        if o["cls"] not in ("symbol", "area"):
            continue
        k = (o["cls"], _desc_key(o, p["rep_desc_eps"]),
             int(round(math.log(max(o["diag"], 1e-6)) / math.log(p["rep_diag_ratio"]))))
        groups.setdefault(k, []).append(oi)
    for k, members in groups.items():
        if len(members) < p["rep_min_group"]:
            continue
        rep = members[0]
        for oi in members[1:]:
            add({"type": "REPEATED_WITH", "a": rep, "b": oi, "sym": True,
                 "group_size": len(members)})

    for r in rels:
        r["S_pt"] = round(S, 4)
    return rels


def _has_shelf(layer, oi, S) -> bool:
    o = layer.objects[oi]
    pts: list = []
    for pi in o["prims"]:
        pr = layer.prims[pi]
        if len(pr["pts"]) > len(pts):
            pts = pr["pts"]
    if len(pts) < 3:
        return False
    def ang(i, j):
        return math.degrees(math.atan2(pts[j][1] - pts[i][1], pts[j][0] - pts[i][0])) % 180.0
    a_last = ang(len(pts) - 2, len(pts) - 1)
    a_prev = ang(0, 1)
    d = abs(a_last - a_prev) % 180.0
    d = min(d, 180.0 - d)
    leg = math.hypot(pts[-1][0] - pts[-2][0], pts[-1][1] - pts[-2][1])
    return d > 20.0 and leg <= 6.0 * S


def _nearest_text(texts, pt, tol) -> Optional[tuple[float, str]]:
    best = None
    for t in texts:
        bb = t["bbox"]
        dx = max(bb[0] - pt[0], pt[0] - bb[2], 0.0)
        dy = max(bb[1] - pt[1], pt[1] - bb[3], 0.0)
        d = math.hypot(dx, dy)
        if d <= tol and (best is None or d < best[0]):
            best = (d, t.get("text"))
    return best


def relation_counts(rels) -> dict[str, int]:
    c: dict[str, int] = {t: 0 for t in REL_TYPES}
    for r in rels:
        c[r["type"]] = c.get(r["type"], 0) + 1
    return c


def rel_key(r, obj_ids_a, obj_ids_b=None):
    """A comparable key for a relation, expressed in stable object identifiers."""
    a = obj_ids_a[r["a"]] if r["a"] is not None else None
    if r["type"] == "LABEL_ANCHOR":
        return (r["type"], a, (r.get("text") or "").strip())
    b = obj_ids_a[r["b"]] if r["b"] is not None else None
    if r.get("sym"):
        a, b = (a, b) if str(a) <= str(b) else (b, a)
    return (r["type"], a, b)
