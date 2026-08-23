# -*- coding: utf-8 -*-
"""advB — ADVERSARIAL rewrites of the representation that were NOT in set A.

Independent code (adversary's own), reusing only the sanctioned readers
(`v03_foundation` via `grp_common`) and the sanctioned grouper (`v03_objects`).

Every rewrite here keeps the DRAWN INK identical up to a rigid/■-preserving map,
so the object layer is required to produce the SAME partition (up to that map).
`src` is preserved on every output segment, so `grp_common.churn_exact` measures
the boundary movement exactly.
"""
from __future__ import annotations
import math, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))


def _base(segs):
    out = []
    for k, s in enumerate(segs):
        t = dict(s)
        t["i"] = k
        t["src"] = [s["i"]]
        out.append(t)
    return out


def _renum(out):
    for k, s in enumerate(out):
        s["i"] = k
    return out


# --------------------------------------------------------------- X1: split elsewhere
def _split_at(frac_list, min_len):
    """Split every segment at fractions `frac_list` (NOT the midpoint used nowhere in A).

    The ink is byte-identical: the union of the pieces is exactly the original
    segment.  Only the number of recorded vertices changes — precisely what a
    different exporter does when it flushes a polyline at a different vertex.
    """
    def rw(segs, rng):
        out = []
        for s in _base(segs):
            if s["len"] < min_len:
                out.append(s)
                continue
            (x0, y0), (x1, y1) = s["p0"], s["p1"]
            ts = [0.0] + list(frac_list) + [1.0]
            for a, b in zip(ts[:-1], ts[1:]):
                t = dict(s)
                ax, ay = x0 + a * (x1 - x0), y0 + a * (y1 - y0)
                bx, by = x0 + b * (x1 - x0), y0 + b * (y1 - y0)
                t["p0"], t["p1"] = (ax, ay), (bx, by)
                t["len"] = math.hypot(bx - ax, by - ay)
                t["src"] = list(s["src"])
                out.append(t)
        return _renum(out)
    return rw


# --------------------------------------------------------------- X2: vertex order
def rw_reverse_vertices(segs, rng):
    """Reverse the direction of every segment and the order of segments inside a path.

    Same ink, same vertices, opposite traversal — what happens when a CAD writes a
    polyline the other way round.
    """
    out = _base(segs)
    for s in out:
        s["p0"], s["p1"] = s["p1"], s["p0"]
    by_path = {}
    for s in out:
        by_path.setdefault(s["path"], []).append(s)
    res = []
    for path in sorted(by_path):
        res.extend(reversed(by_path[path]))
    return _renum(res)


# --------------------------------------------------------------- X3/X4: rigid maps
def _affine(fn):
    def rw(segs, rng):
        out = _base(segs)
        for s in out:
            s["p0"] = fn(*s["p0"])
            s["p1"] = fn(*s["p1"])
            s["len"] = math.hypot(s["p1"][0] - s["p0"][0], s["p1"][1] - s["p0"][1])
        return out
    return rw


def mirror_x(cx):
    return _affine(lambda x, y: (2 * cx - x, y))


def rotate_about(cx, cy, deg):
    a = math.radians(deg)
    ca, sa = math.cos(a), math.sin(a)

    def f(x, y):
        dx, dy = x - cx, y - cy
        return (cx + ca * dx - sa * dy, cy + sa * dx + ca * dy)
    return _affine(f)


def map_texts(texts, fn):
    out = []
    for t in texts:
        u = dict(t)
        x0, y0, x1, y1 = t["bbox"]
        pts = [fn(x0, y0), fn(x1, y0), fn(x1, y1), fn(x0, y1)]
        xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
        u["bbox"] = [min(xs), min(ys), max(xs), max(ys)]
        u["cx"], u["cy"] = (min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2
        out.append(u)
    return out


# --------------------------------------------------------------- X7/X8: rect <-> lines
def rw_rect_to_lines(segs, rng):
    """Every segment that came from a `re`/`qu` op becomes an ordinary `l` in its own
    path with closed=False.  Same four strokes on the page."""
    out = _base(segs)
    nxt = max((s["path"] for s in out), default=0) + 1
    for s in out:
        if s.get("op") in ("re", "qu"):
            s["op"] = "l"
            s["closed"] = False
            s["path"] = nxt
            nxt += 1
    return out


def _axis_rect_groups(segs, tol=0.01):
    """Find sets of 4 segments forming an axis-aligned rectangle (any path ids)."""
    horiz, vert = [], []
    for k, s in enumerate(segs):
        (x0, y0), (x1, y1) = s["p0"], s["p1"]
        if abs(y1 - y0) <= tol and abs(x1 - x0) > tol:
            horiz.append((k, min(x0, x1), max(x0, x1), (y0 + y1) / 2))
        elif abs(x1 - x0) <= tol and abs(y1 - y0) > tol:
            vert.append((k, min(y0, y1), max(y0, y1), (x0 + x1) / 2))
    groups = []
    used = set()
    from collections import defaultdict
    hb = defaultdict(list)
    for h in horiz:
        hb[(round(h[1], 2), round(h[2], 2))].append(h)
    vb = defaultdict(list)
    for v in vert:
        vb[(round(v[1], 2), round(v[2], 2))].append(v)
    for key, hs in hb.items():
        if len(hs) < 2:
            continue
        x0, x1 = key
        hs = sorted(hs, key=lambda t: t[3])
        for i in range(len(hs) - 1):
            ytop, ybot = hs[i][3], hs[i + 1][3]
            vk = (round(min(ytop, ybot), 2), round(max(ytop, ybot), 2))
            vs = [v for v in vb.get(vk, []) if abs(v[3] - x0) < 0.02 or abs(v[3] - x1) < 0.02]
            if len(vs) >= 2 and not ({hs[i][0], hs[i + 1][0]} & used):
                ids = [hs[i][0], hs[i + 1][0], vs[0][0], vs[1][0]]
                if used & set(ids):
                    continue
                used |= set(ids)
                groups.append(ids)
    return groups


def rw_lines_to_rect(segs, rng):
    """Four independent lines that form an axis-aligned rectangle are re-packed into
    one closed `re` path — the inverse of X7."""
    out = _base(segs)
    groups = _axis_rect_groups(out)
    nxt = max((s["path"] for s in out), default=0) + 1
    for ids in groups:
        for k in ids:
            out[k]["op"] = "re"
            out[k]["closed"] = True
            out[k]["path"] = nxt
        nxt += 1
    return out
