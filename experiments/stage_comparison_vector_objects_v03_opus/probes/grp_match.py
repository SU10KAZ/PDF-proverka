# -*- coding: utf-8 -*-
"""Geometric ink correspondence between two sides of a pair, in PDF POINTS.

Two prepared blocks of the same sheet live in the same page coordinate system, so the
correspondence is a translation (measured, not assumed).  A left segment is matched to
the right segment that is closest in perpendicular distance AND nearly parallel — a
plain nearest-neighbour on midpoints picks the wrong parallel line in dense CAD.
"""
from __future__ import annotations
import math

CELL = 4.0


def build_index(segs, off=(0.0, 0.0), cell=CELL):
    grid: dict[tuple[int, int], list[int]] = {}
    data = []
    for s in segs:
        x0, y0 = s["p0"][0] + off[0], s["p0"][1] + off[1]
        x1, y1 = s["p1"][0] + off[0], s["p1"][1] + off[1]
        ang = math.degrees(math.atan2(y1 - y0, x1 - x0)) % 180.0
        data.append((x0, y0, x1, y1, ang, s["i"]))
        n = max(1, int(math.hypot(x1 - x0, y1 - y0) / cell) + 1)
        seen = set()
        for k in range(n + 1):
            t = k / n
            gx = int(math.floor((x0 + t * (x1 - x0)) / cell))
            gy = int(math.floor((y0 + t * (y1 - y0)) / cell))
            if (gx, gy) in seen:
                continue
            seen.add((gx, gy))
            grid.setdefault((gx, gy), []).append(len(data) - 1)
    return grid, data, cell


def _pt_seg_dist2(px, py, x0, y0, x1, y1):
    dx, dy = x1 - x0, y1 - y0
    L2 = dx * dx + dy * dy
    if L2 <= 1e-12:
        return (px - x0) ** 2 + (py - y0) ** 2
    t = ((px - x0) * dx + (py - y0) * dy) / L2
    t = 0.0 if t < 0 else (1.0 if t > 1 else t)
    return (px - (x0 + t * dx)) ** 2 + (py - (y0 + t * dy)) ** 2


def query(index, px, py, ang, tol, ang_tol=15.0):
    grid, data, cell = index
    gx, gy = int(math.floor(px / cell)), int(math.floor(py / cell))
    best, best_i = tol * tol, None
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for di in grid.get((gx + dx, gy + dy), ()):
                x0, y0, x1, y1, a, si = data[di]
                da = abs(a - ang) % 180.0
                if min(da, 180.0 - da) > ang_tol:
                    continue
                d = _pt_seg_dist2(px, py, x0, y0, x1, y1)
                if d < best:
                    best, best_i = d, si
    return best_i


# --- exact-decomposition matcher -------------------------------------------------
# Measured necessity: on a pair whose two object layers are byte-identical (2 019 of
# 2 019 objects with the same segment set), a nearest-parallel-segment matcher at
# 0.3 pt still assigned 41 % of the ink to the wrong object, because dense CAD has
# many exactly coincident parallel strokes.  Matching BOTH endpoints removes that.

def build_endpoint_index(segs, cell=CELL):
    grid: dict[tuple[int, int], list[int]] = {}
    data = []
    for s in segs:
        data.append((s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1], s["i"]))
        for (x, y) in (s["p0"], s["p1"]):
            grid.setdefault((int(math.floor(x / cell)), int(math.floor(y / cell))),
                            []).append(len(data) - 1)
    return grid, data, cell


def query_endpoints(index, ax, ay, bx, by, tol):
    grid, data, cell = index
    gx, gy = int(math.floor(ax / cell)), int(math.floor(ay / cell))
    best, best_i = None, None
    t2 = tol * tol
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for di in grid.get((gx + dx, gy + dy), ()):
                x0, y0, x1, y1, si = data[di]
                d1 = (x0 - ax) ** 2 + (y0 - ay) ** 2
                d2 = (x1 - bx) ** 2 + (y1 - by) ** 2
                if d1 <= t2 and d2 <= t2:
                    tot = d1 + d2
                    if best is None or tot < best:
                        best, best_i = tot, si
                    continue
                d1 = (x1 - ax) ** 2 + (y1 - ay) ** 2
                d2 = (x0 - bx) ** 2 + (y0 - by) ** 2
                if d1 <= t2 and d2 <= t2:
                    tot = d1 + d2
                    if best is None or tot < best:
                        best, best_i = tot, si
    return best_i


def matched_share(segs_a, index, off, tol, limit_pts=None):
    """Share of A ink whose midpoint has a parallel B segment within tol."""
    hit = tot = 0.0
    for s in segs_a:
        L = s["len"]
        tot += L
        x = (s["p0"][0] + s["p1"][0]) / 2 - off[0]
        y = (s["p0"][1] + s["p1"][1]) / 2 - off[1]
        ang = math.degrees(math.atan2(s["p1"][1] - s["p0"][1], s["p1"][0] - s["p0"][0])) % 180.0
        if query(index, x, y, ang, tol) is not None:
            hit += L
    return hit / max(tot, 1e-9)


def share_endpoints(segs_a, eidx, off, tol):
    hit = tot = 0.0
    for s in segs_a:
        L = s["len"]
        tot += L
        if query_endpoints(eidx, s["p0"][0] - off[0], s["p0"][1] - off[1],
                           s["p1"][0] - off[0], s["p1"][1] - off[1], tol) is not None:
            hit += L
    return hit / max(tot, 1e-9)


def register(segs_a, segs_b, seeds, tol=0.8):
    """Translation search (PDF points).

    Coarse pass: nearest-parallel point match (tolerant, finds the basin).
    Fine passes: BOTH-endpoint match with a shrinking tolerance, which has a sharp
    maximum at the true offset instead of the flat plateau a tolerant score gives.
    """
    sub_a = segs_a if len(segs_a) <= 800 else segs_a[::max(1, len(segs_a) // 800)]
    sub_a2 = segs_a if len(segs_a) <= 250 else segs_a[::max(1, len(segs_a) // 250)]
    index = build_index(segs_b)
    eidx = build_endpoint_index(segs_b)
    best = None
    ded = []
    for sd in sorted(set(seeds)):
        if all(math.hypot(sd[0] - q[0], sd[1] - q[1]) > 2.0 for q in ded):
            ded.append(sd)
    for sd in ded:
        for i in range(-6, 7):
            for j in range(-6, 7):
                off = (sd[0] + i * 4.0, sd[1] + j * 4.0)
                sc = matched_share(sub_a2, index, off, 1.6)
                if best is None or sc > best[0]:
                    best = (sc, off)
    for step, rad, ftol in ((1.5, 3, 1.5), (0.5, 3, 0.6), (0.15, 3, 0.25), (0.05, 3, 0.1)):
        base = best[1]
        cur = None
        for i in range(-rad, rad + 1):
            for j in range(-rad, rad + 1):
                off = (base[0] + i * step, base[1] + j * step)
                sc = share_endpoints(sub_a, eidx, off, ftol)
                if cur is None or sc > cur[0]:
                    cur = (sc, off)
        best = cur
    final = matched_share(sub_a, index, best[1], tol)
    return best[1][0], best[1][1], final


def churn_rows(layer_a, segs_a, layer_b, segs_b, off, tol=0.8):
    """For each A object: which B objects own its ink, and in what proportion.

    Stage 1 matches whole segments by BOTH endpoints (exact decomposition).
    Stage 2 falls back to nearest-parallel point matching for the rest (different
    decomposition on the two sides).  Which stage matched is recorded.
    """
    eidx = build_endpoint_index([{**s, "p0": (s["p0"][0] + off[0], s["p0"][1] + off[1]),
                                  "p1": (s["p1"][0] + off[0], s["p1"][1] + off[1])}
                                 for s in segs_b])
    pidx = build_index(segs_b, off)
    seg2obj_b = layer_b.seg2obj
    rows = []
    n_stage1 = n_stage2 = 0
    for oi, o in enumerate(layer_a.objects):
        acc: dict[int, float] = {}
        tot = 0.0
        unmatched = 0.0
        for gi in o["segments"]:
            s = segs_a[gi]
            L = s["len"]
            tot += L
            bi = query_endpoints(eidx, s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1], tol)
            if bi is not None:
                n_stage1 += 1
                ob = seg2obj_b[bi]
                if ob >= 0:
                    acc[ob] = acc.get(ob, 0.0) + L
                    continue
            ang = math.degrees(math.atan2(s["p1"][1] - s["p0"][1],
                                          s["p1"][0] - s["p0"][0])) % 180.0
            votes: dict[int, float] = {}
            n = 3
            for k in range(n):
                t = (k + 0.5) / n
                x = s["p0"][0] + t * (s["p1"][0] - s["p0"][0])
                y = s["p0"][1] + t * (s["p1"][1] - s["p0"][1])
                bj = query(pidx, x, y, ang, tol)
                if bj is None:
                    continue
                ob = seg2obj_b[bj]
                if ob >= 0:
                    votes[ob] = votes.get(ob, 0.0) + L / n
            if votes:
                n_stage2 += 1
                for ob, w in votes.items():
                    acc[ob] = acc.get(ob, 0.0) + w
            else:
                unmatched += L
        if tot <= 0:
            continue
        matched = sum(acc.values())
        row = {"o": oi, "cls": o["cls"], "len": tot, "matched": matched,
               "unmatched_share": unmatched / tot, "border": o.get("border", False)}
        if not acc:
            row.update({"n_partners": 0, "best_share": 0.0, "partner_purity": 0.0})
        else:
            bj, bl = max(acc.items(), key=lambda kv: kv[1])
            row.update({"n_partners": len(acc), "partner": bj,
                        "best_share": bl / max(matched, 1e-9),
                        "partner_purity": bl / max(layer_b.objects[bj]["seg_len"], 1e-9)})
        rows.append(row)
    if rows:
        rows[0]["_stage1"] = n_stage1
        rows[0]["_stage2"] = n_stage2
    return rows


def classify(rows, thr=0.95, min_matched=0.5):
    """Ink-length weighted split.  Ink with no partner at all is 'lost' and is NOT
    charged to the grouping: it is a real difference or a crop-boundary artefact."""
    out = {"one_to_one": 0.0, "split": 0.0, "merge": 0.0, "mixed": 0.0, "lost": 0.0}
    tot = sum(r["len"] for r in rows) or 1.0
    for r in rows:
        if r["n_partners"] == 0 or (r["matched"] / max(r["len"], 1e-9)) < min_matched:
            k = "lost"
        elif r["best_share"] >= thr and r["partner_purity"] >= thr:
            k = "one_to_one"
        elif r["best_share"] < thr and r["partner_purity"] >= thr:
            k = "split"
        elif r["best_share"] >= thr and r["partner_purity"] < thr:
            k = "merge"
        else:
            k = "mixed"
        out[k] += r["len"]
    return {k: v / tot for k, v in out.items()}
