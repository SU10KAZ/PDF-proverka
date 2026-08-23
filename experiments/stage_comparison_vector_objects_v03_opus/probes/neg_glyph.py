# -*- coding: utf-8 -*-
"""A deterministic 'this is a letter contour, not an object' test (N3).

No font tables, no OCR, no discipline dictionary: the only thing used is that glyphs
of one printed string form a RUN — three or more small marks of similar height whose
centres share a baseline and whose gaps are of the order of that height.  Runs are
looked for along both page axes, because CAD labels are often set vertically.

Every parameter is dimensionless (expressed in units of the candidate's own height),
so the test has no PDF-point constant to tune per discipline.
"""
from __future__ import annotations

DEFAULTS = {
    "base_tol": 0.35,      # baseline offset, in units of glyph height
    "h_ratio": 2.5,        # allowed height ratio inside a run
    "gap_max": 1.2,        # max horizontal gap, in units of height
    "gap_min": -0.35,      # glyphs may slightly overlap (kerning, italics)
    "run_min": 3,          # marks needed to call it a run
    "h_max_frac": 0.06,    # a glyph is small relative to the block diagonal
    "closed_only": False,  # require a closed contour
    "absorb": False,       # second pass: swallow small marks lying inside a run's band
    "absorb_pad": 0.30,    # band padding, in units of the run's glyph height
}


def _runs(items, axis, p):
    """items: list of (idx, x0, y0, x1, y1).  axis 0 = horizontal text."""
    if axis == 0:
        key = lambda it: it[1]
        along = lambda it: (it[1], it[3])          # x0, x1
        across = lambda it: (it[2] + it[4]) / 2     # cy
        size = lambda it: it[4] - it[2]             # height
    else:
        key = lambda it: it[2]
        along = lambda it: (it[2], it[4])
        across = lambda it: (it[1] + it[3]) / 2
        size = lambda it: it[3] - it[1]
    order = sorted(items, key=key)
    parent = {it[0]: it[0] for it in order}

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    n = len(order)
    for i in range(n):
        a = order[i]
        ha = max(size(a), 1e-6)
        for j in range(i + 1, min(i + 12, n)):
            b = order[j]
            hb = max(size(b), 1e-6)
            h = max(ha, hb)
            if not (1.0 / p["h_ratio"] <= ha / hb <= p["h_ratio"]):
                continue
            if abs(across(a) - across(b)) > p["base_tol"] * h:
                continue
            gap = along(b)[0] - along(a)[1]
            if gap > p["gap_max"] * h:
                continue
            if gap < p["gap_min"] * h:
                continue
            union(a[0], b[0])
    groups: dict[int, list[int]] = {}
    for it in order:
        groups.setdefault(find(it[0]), []).append(it[0])
    return [g for g in groups.values() if len(g) >= p["run_min"]]


def _bands(items, groups, axis, pad):
    """Bounding band of each run, padded across the baseline and along the line."""
    by = {it[0]: it for it in items}
    out = []
    for g in groups:
        bb = [min(by[i][1] for i in g), min(by[i][2] for i in g),
              max(by[i][3] for i in g), max(by[i][4] for i in g)]
        h = (bb[3] - bb[1]) if axis == 0 else (bb[2] - bb[0])
        h = max(h, 1e-6)
        out.append([bb[0] - pad * h, bb[1] - pad * h, bb[2] + pad * h, bb[3] + pad * h])
    return out


def _inside(bb, band):
    return (bb[0] >= band[0] and bb[1] >= band[1]
            and bb[2] <= band[2] and bb[3] <= band[3])


def glyph_flags(layer, frame, **params):
    """-> (set of object indices that look like letter contours, diagnostics)."""
    p = dict(DEFAULTS)
    p.update(params or {})
    x0, y0, x1, y1 = frame
    bdiag = max(((x1 - x0) ** 2 + (y1 - y0) ** 2) ** 0.5, 1e-6)
    items = []
    for i, o in enumerate(layer.objects):
        bb = o["bbox"]
        h, w = bb[3] - bb[1], bb[2] - bb[0]
        if max(h, w) > p["h_max_frac"] * bdiag:
            continue
        if p["closed_only"] and not o.get("cycle"):
            continue
        items.append((i, bb[0], bb[1], bb[2], bb[3]))
    flags: set[int] = set()
    runs_h = _runs(items, 0, p) if items else []
    runs_v = _runs(items, 1, p) if items else []
    for g in runs_h + runs_v:
        flags.update(g)
    n_run = len(flags)
    n_absorbed = 0
    if p["absorb"] and (runs_h or runs_v):
        bands = (_bands(items, runs_h, 0, p["absorb_pad"])
                 + _bands(items, runs_v, 1, p["absorb_pad"]))
        for it in items:
            if it[0] in flags:
                continue
            bb = (it[1], it[2], it[3], it[4])
            for band in bands:
                if _inside(bb, band):
                    flags.add(it[0])
                    n_absorbed += 1
                    break
    return flags, {"n_candidates": len(items), "n_runs_h": len(runs_h),
                   "n_runs_v": len(runs_v), "n_flagged": len(flags),
                   "n_by_run": n_run, "n_absorbed": n_absorbed,
                   "n_objects": len(layer.objects), "params": p}
