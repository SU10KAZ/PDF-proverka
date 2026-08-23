# -*- coding: utf-8 -*-
"""Dimension chains: a deterministic re-tiling test (N4, §17).

v0.2 already established that what identifies a dimension chain across versions is
the TILING (which intervals the run is cut into), not the mid-span position and not
the printed number.  This module extracts that tiling from geometry alone:

  a long straight run  +  the short marks that sit ON it  ->  a sorted list of
  interval lengths along the run.

Comparing two sides = matching runs by their end points and comparing the interval
lists.  The printed number is never read, so 1500 -> 1600 CANNOT fire here, while
1500 -> 300+1000+200 must.
"""
from __future__ import annotations
import math

DEFAULTS = {
    "run_min_S": 6.0,      # a dimension line is long
    "tick_min_S": 0.35,    # tick marks are short
    "tick_max_S": 3.0,
    "tick_dist_S": 0.40,   # tick centre must sit on the run
    "tick_ang_min": 20.0,  # ... and cross it
    "quant_pt": 0.5,       # interval quantisation, PDF points
    "match_tol_pt": 2.0,   # run endpoints of the two sides
}


def _unit(s):
    dx = s["p1"][0] - s["p0"][0]
    dy = s["p1"][1] - s["p0"][1]
    L = max(s["len"], 1e-9)
    return dx / L, dy / L


def chains(ex, S, **params):
    p = dict(DEFAULTS)
    p.update(params or {})
    segs = ex.segments
    runs = [(i, s) for i, s in enumerate(segs) if s["len"] >= p["run_min_S"] * S]
    ticks = [(i, s) for i, s in enumerate(segs)
             if p["tick_min_S"] * S <= s["len"] <= p["tick_max_S"] * S]
    # spatial bucket of tick midpoints
    cell = max(2.0 * S, 2.0)
    grid: dict[tuple, list[int]] = {}
    tmid = {}
    for i, s in ticks:
        mx = (s["p0"][0] + s["p1"][0]) / 2
        my = (s["p0"][1] + s["p1"][1]) / 2
        tmid[i] = (mx, my)
        grid.setdefault((int(mx // cell), int(my // cell)), []).append(i)
    out = []
    for ri, r in runs:
        ux, uy = _unit(r)
        ang_r = math.degrees(math.atan2(uy, ux)) % 180.0
        x0, y0 = r["p0"]
        L = r["len"]
        offs = []
        n_cells = int(L // cell) + 2
        seen = set()
        for k in range(n_cells + 1):
            t = min(1.0, k * cell / max(L, 1e-9))
            px, py = x0 + ux * L * t, y0 + uy * L * t
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    for ti in grid.get((int(px // cell) + dx, int(py // cell) + dy), ()):
                        if ti in seen or ti == ri:
                            continue
                        seen.add(ti)
                        mx, my = tmid[ti]
                        # projection on the run
                        proj = (mx - x0) * ux + (my - y0) * uy
                        if proj < -0.5 or proj > L + 0.5:
                            continue
                        perp = abs(-(mx - x0) * uy + (my - y0) * ux)
                        if perp > p["tick_dist_S"] * S:
                            continue
                        ts = segs[ti]
                        au, av = _unit(ts)
                        a = math.degrees(math.atan2(av, au)) % 180.0
                        da = abs(a - ang_r) % 180.0
                        da = min(da, 180.0 - da)
                        if da < p["tick_ang_min"]:
                            continue
                        offs.append(proj)
        if len(offs) < 2:
            continue
        offs = sorted(offs)
        # collapse ticks that coincide
        merged = [offs[0]]
        for v in offs[1:]:
            if v - merged[-1] > max(0.4 * S, 0.5):
                merged.append(v)
        if len(merged) < 2:
            continue
        q = p["quant_pt"]
        cuts = [0.0] + merged + [L]
        iv = [round((cuts[i + 1] - cuts[i]) / q) for i in range(len(cuts) - 1)]
        iv = [v for v in iv if v > 0]
        out.append({"seg": ri, "p0": (round(r["p0"][0], 2), round(r["p0"][1], 2)),
                    "p1": (round(r["p1"][0], 2), round(r["p1"][1], 2)),
                    "len": round(L, 3), "n_ticks": len(merged),
                    "intervals_q": iv,
                    "sig": tuple(iv)})
    return out


def compare(ch_a, ch_b, off=(0.0, 0.0), tol=None, S=1.0):
    """-> records of chains whose tiling changed (plus appeared / disappeared runs)."""
    tol = tol if tol is not None else DEFAULTS["match_tol_pt"]
    used = set()
    recs = []
    for a in ch_a:
        ax0 = (a["p0"][0] - off[0], a["p0"][1] - off[1])
        ax1 = (a["p1"][0] - off[0], a["p1"][1] - off[1])
        best, bi = None, -1
        for j, b in enumerate(ch_b):
            if j in used:
                continue
            d1 = math.hypot(ax0[0] - b["p0"][0], ax0[1] - b["p0"][1]) + \
                math.hypot(ax1[0] - b["p1"][0], ax1[1] - b["p1"][1])
            d2 = math.hypot(ax0[0] - b["p1"][0], ax0[1] - b["p1"][1]) + \
                math.hypot(ax1[0] - b["p0"][0], ax1[1] - b["p0"][1])
            d = min(d1, d2)
            if d <= 2 * tol and (best is None or d < best):
                best, bi = d, j
        if bi < 0:
            recs.append({"type": "DIM_RUN_ONLY_A", "chain": a})
            continue
        used.add(bi)
        b = ch_b[bi]
        sa, sb = a["sig"], b["sig"]
        if sa != sb and tuple(reversed(sa)) != sb:
            recs.append({"type": "DIM_CHAIN_RETILED", "a": a["sig"], "b": b["sig"],
                         "n_ticks_a": a["n_ticks"], "n_ticks_b": b["n_ticks"],
                         "bbox": [a["p0"][0], a["p0"][1], a["p1"][0], a["p1"][1]]})
    for j, b in enumerate(ch_b):
        if j not in used:
            recs.append({"type": "DIM_RUN_ONLY_B", "chain": b})
    return recs
