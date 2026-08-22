#!/usr/bin/env python3
"""dim_* probe: generic, discipline-free dimension detector over a cached vector block.

A DIMENSION is modelled as a GRAPHICAL OBJECT:
    value_text  --on-->  dimension_line  --bounded-by-->  (terminator_a, terminator_b)
    terminator  --at-->  extension_line_foot
    measured_span = |t_b - t_a|   (PDF points)

No discipline knowledge is used: no unit tables, no symbol libraries, no layer names.
Only: segment geometry, path fill, text span geometry + baseline direction.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_detect \
        --cache <cache.json> --out <result.json>
"""
from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Sequence

# ---- tunables (all geometric, none discipline-specific) --------------------
PARALLEL_TOL_DEG = 3.0        # text baseline vs dimension line
PERP_TOL_DEG = 3.0            # extension line vs dimension line
OFFSET_MAX_FACTOR = 3.0       # |perp offset text->dim line| <= 3.0 * font size
LINE_MERGE_TOL = 0.35         # pt: collinearity tolerance when grouping segments
TICK_MIN_LEN = 0.8            # pt
TICK_MAX_LEN = 9.0            # pt
TICK_ANGLE_MIN = 20.0         # deg from the dimension line
TICK_ANGLE_MAX = 70.0
ARROW_MAX_SIDE = 14.0         # pt: bbox of a filled arrowhead
ARROW_MAX_SEGS = 10
ON_LINE_TOL = 0.6             # pt: terminator/foot must sit on the dimension line
EXT_MIN_LEN = 1.5             # pt: minimum extension-line stub length
BOUND_SNAP = 1.0              # pt: merge coincident bounds
COVER_MIN = 0.8               # dimension line must be drawn over >=80% of the interval
SLASH_CENTER_MIN = 0.25       # the line must cross a terminator object near its middle

_BARE_NUM = re.compile(r"^\d{1,6}$")
_DEC_NUM = re.compile(r"^\d{1,4}[.,]\d{1,3}$")
_SIGNED_DEC = re.compile(r"^[+\-−]\d{1,3}[.,]\d{1,3}$")


def classify_text(t: dict[str, Any]) -> str:
    s = t["text"].replace(" ", "")
    if _BARE_NUM.match(s):
        return "bare_number"
    if _DEC_NUM.match(s):
        return "decimal_number"
    if _SIGNED_DEC.match(s):
        return "signed_decimal"      # elevation-mark shape (leader value)
    return "other"


def _dedup_segments(paths: Sequence[dict[str, Any]]) -> tuple[list[tuple], list[dict]]:
    seen = set()
    segs: list[tuple] = []
    fills: list[dict] = []
    for p in paths:
        if p["filled"] and len(p["segs"]) <= ARROW_MAX_SEGS:
            r = p["rect"]
            if (r[2] - r[0]) <= ARROW_MAX_SIDE and (r[3] - r[1]) <= ARROW_MAX_SIDE:
                pts = []
                for s in p["segs"]:
                    pts.append((s[0], s[1]))
                    pts.append((s[2], s[3]))
                fills.append({"rect": r, "pts": pts, "n": len(p["segs"])})
        for s in p["segs"]:
            x0, y0, x1, y1 = s
            key = (x0, y0, x1, y1) if (x0, y0) <= (x1, y1) else (x1, y1, x0, y0)
            key = tuple(round(v, 2) for v in key)
            if key in seen:
                continue
            seen.add(key)
            segs.append((key[0], key[1], key[2], key[3]))
    return segs, fills


def build_slashes(segs, seg_ang, seg_len, max_arm: float = 7.0, join_tol: float = 0.4):
    """Assemble TERMINATOR OBJECTS out of raw segments.

    A CAD tick/slash is emitted as two collinear arms meeting on the dimension
    line.  Working at segment level, either arm's free endpoint can fall within
    tolerance of a *neighbouring* dimension line and forge a bound there.  So we
    first merge collinear touching short segments into one object, and later
    demand that the dimension line cross the object near its middle.
    """
    short = [i for i, L in enumerate(seg_len) if 0.5 <= L <= max_arm]
    cell = 0.5
    emap = defaultdict(list)
    for i in short:
        x0, y0, x1, y1 = segs[i]
        for (px, py) in ((x0, y0), (x1, y1)):
            emap[(int(px // cell), int(py // cell))].append((i, px, py))
    parent = {i: i for i in short}

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in short:
        x0, y0, x1, y1 = segs[i]
        for (px, py) in ((x0, y0), (x1, y1)):
            cx0, cy0 = int(px // cell), int(py // cell)
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    for (j, qx, qy) in emap.get((cx0 + dx, cy0 + dy), ()):
                        if j == i or find(j) == find(i):
                            continue
                        if abs(qx - px) > join_tol or abs(qy - py) > join_tol:
                            continue
                        if _adiff(seg_ang[i], seg_ang[j]) <= 4.0:
                            union(i, j)
    comps = defaultdict(list)
    for i in short:
        comps[find(i)].append(i)
    out = []
    for root, members in comps.items():
        pts = []
        for i in members:
            x0, y0, x1, y1 = segs[i]
            pts.append((x0, y0))
            pts.append((x1, y1))
        a = seg_ang[members[0]]
        ux, uy = math.cos(math.radians(a)), math.sin(math.radians(a))
        proj = sorted(pts, key=lambda p: p[0] * ux + p[1] * uy)
        p0, p1 = proj[0], proj[-1]
        L = math.hypot(p1[0] - p0[0], p1[1] - p0[1])
        if L < 0.5:
            continue
        out.append({"p0": p0, "p1": p1, "len": L, "ang": _ang(p1[0] - p0[0], p1[1] - p0[1]),
                    "n_arms": len(members)})
    return out


class Grid:
    """Uniform grid over segment bboxes; long segments kept in a separate list."""

    def __init__(self, segs: Sequence[tuple], cell: float = 40.0, long_cut: float = 200.0):
        self.cell = cell
        self.buckets: dict[tuple[int, int], list[int]] = defaultdict(list)
        self.long: list[int] = []
        self.segs = segs
        for i, (x0, y0, x1, y1) in enumerate(segs):
            if max(abs(x1 - x0), abs(y1 - y0)) > long_cut:
                self.long.append(i)
                continue
            cx0, cx1 = int(min(x0, x1) // cell), int(max(x0, x1) // cell)
            cy0, cy1 = int(min(y0, y1) // cell), int(max(y0, y1) // cell)
            for cx in range(cx0, cx1 + 1):
                for cy in range(cy0, cy1 + 1):
                    self.buckets[(cx, cy)].append(i)

    def query(self, x0: float, y0: float, x1: float, y1: float) -> list[int]:
        out = list(self.long)
        c = self.cell
        for cx in range(int(x0 // c), int(x1 // c) + 1):
            for cy in range(int(y0 // c), int(y1 // c) + 1):
                b = self.buckets.get((cx, cy))
                if b:
                    out.extend(b)
        return list(set(out))


def _ang(dx: float, dy: float) -> float:
    return math.degrees(math.atan2(dy, dx)) % 180.0


def _adiff(a: float, b: float) -> float:
    d = abs(a - b) % 180.0
    return min(d, 180.0 - d)


def detect(cache: dict[str, Any], *, require_terminator: bool = True,
           variant: str = "B_side_convention",
           terminator_model: str = "slash_object",
           require_extension: bool = False,
           debug_text: str | None = None) -> dict[str, Any]:
    segs, fills = _dedup_segments(cache["paths"])
    grid = Grid(segs)
    seg_ang = [_ang(s[2] - s[0], s[3] - s[1]) for s in segs]
    seg_len = [math.hypot(s[2] - s[0], s[3] - s[1]) for s in segs]
    slashes = build_slashes(segs, seg_ang, seg_len) if terminator_model == "slash_object" else []
    slash_grid = defaultdict(list)
    for k, sl in enumerate(slashes):
        mx = (sl["p0"][0] + sl["p1"][0]) / 2
        my = (sl["p0"][1] + sl["p1"][1]) / 2
        slash_grid[(int(mx // 40), int(my // 40))].append(k)

    results = []
    for t in cache["texts"]:
        kind = classify_text(t)
        if kind not in ("bare_number", "decimal_number"):
            continue
        bx = t["bbox"]
        cx, cy = (bx[0] + bx[2]) / 2, (bx[1] + bx[3]) / 2
        ux, uy = t["dir"]
        n = math.hypot(ux, uy) or 1.0
        ux, uy = ux / n, uy / n
        vx, vy = -uy, ux                      # perpendicular
        size = t["size"] or 7.0
        w_max = OFFSET_MAX_FACTOR * size
        text_half = max(bx[2] - bx[0], bx[3] - bx[1]) / 2
        tang = _ang(ux, uy)

        def to_tw(px: float, py: float) -> tuple[float, float]:
            dx, dy = px - cx, py - cy
            return dx * ux + dy * uy, dx * vx + dy * vy

        # window around the text: generous along the baseline, tight across it
        reach = max(60.0, 14 * size)
        qx0 = cx - reach; qx1 = cx + reach; qy0 = cy - reach; qy1 = cy + reach
        cand = grid.query(qx0, qy0, qx1, qy1)
        slash_cand = []
        for gx in range(int(qx0 // 40), int(qx1 // 40) + 1):
            for gy in range(int(qy0 // 40), int(qy1 // 40) + 1):
                slash_cand.extend(slash_grid.get((gx, gy), ()))

        par: list[int] = []       # parallel to the text baseline
        perp: list[int] = []      # perpendicular -> extension-line candidates
        tick: list[int] = []      # short oblique -> terminator candidates
        for i in cand:
            a = seg_ang[i]
            d = _adiff(a, tang)
            if d <= PARALLEL_TOL_DEG:
                par.append(i)
            elif abs(d - 90.0) <= PERP_TOL_DEG:
                perp.append(i)
            if TICK_MIN_LEN <= seg_len[i] <= TICK_MAX_LEN and TICK_ANGLE_MIN <= d <= TICK_ANGLE_MAX:
                tick.append(i)

        # ---- group parallel segments into collinear "dimension line" hypotheses
        groups: dict[int, dict[str, Any]] = {}
        for i in par:
            x0, y0, x1, y1 = segs[i]
            t0, w0 = to_tw(x0, y0)
            t1, w1 = to_tw(x1, y1)
            w = (w0 + w1) / 2
            if abs(w) > w_max:
                continue
            lo, hi = (t0, t1) if t0 <= t1 else (t1, t0)
            key = int(round(w / LINE_MERGE_TOL))
            g = groups.setdefault(key, {"w": w, "ivals": [], "n": 0})
            g["ivals"].append((lo, hi))
            g["n"] += 1
            g["w"] = (g["w"] * (g["n"] - 1) + w) / g["n"]

        def union_len(ivals: list[tuple[float, float]], a: float, b: float) -> float:
            cut = []
            for lo, hi in ivals:
                lo2, hi2 = max(lo, a), min(hi, b)
                if hi2 > lo2:
                    cut.append((lo2, hi2))
            if not cut:
                return 0.0
            cut.sort()
            total, cur_lo, cur_hi = 0.0, cut[0][0], cut[0][1]
            for lo, hi in cut[1:]:
                if lo > cur_hi:
                    total += cur_hi - cur_lo
                    cur_lo, cur_hi = lo, hi
                else:
                    cur_hi = max(cur_hi, hi)
            return total + cur_hi - cur_lo

        hyps = []
        for key, g in groups.items():
            w = g["w"]
            bounds: list[dict[str, Any]] = []
            if terminator_model == "raw_endpoint":
                for i in tick:
                    x0, y0, x1, y1 = segs[i]
                    for (px, py) in ((x0, y0), (x1, y1)):
                        tt, ww = to_tw(px, py)
                        if abs(ww - w) <= ON_LINE_TOL and abs(tt) <= reach:
                            bounds.append({"t": tt, "kind": "tick"})
                            break
            else:
                for k in slash_cand:
                    sl = slashes[k]
                    if not (TICK_MIN_LEN <= sl["len"] <= TICK_MAX_LEN):
                        continue
                    d = _adiff(sl["ang"], tang)
                    if not (TICK_ANGLE_MIN <= d <= TICK_ANGLE_MAX):
                        continue
                    t0, w0 = to_tw(*sl["p0"])
                    t1, w1 = to_tw(*sl["p1"])
                    if abs(w1 - w0) < 1e-6:
                        continue
                    s_par = (w - w0) / (w1 - w0)
                    if not (SLASH_CENTER_MIN <= s_par <= 1.0 - SLASH_CENTER_MIN):
                        continue
                    tt = t0 + (t1 - t0) * s_par
                    if abs(tt) <= reach:
                        bounds.append({"t": tt, "kind": "tick"})
            for f in fills:
                pts = [to_tw(px, py) for px, py in f["pts"]]
                on = [p for p in pts if abs(p[1] - w) <= ON_LINE_TOL * 2]
                if not on:
                    continue
                tip = min(on, key=lambda p: abs(p[1] - w))
                if abs(tip[0]) <= reach:
                    bounds.append({"t": tip[0], "kind": "arrow"})
            feet = []
            for i in perp:
                x0, y0, x1, y1 = segs[i]
                if seg_len[i] < EXT_MIN_LEN:
                    continue
                t0, w0 = to_tw(x0, y0)
                t1, w1 = to_tw(x1, y1)
                lo, hi = (w0, w1) if w0 <= w1 else (w1, w0)
                if not (lo - ON_LINE_TOL <= w <= hi + ON_LINE_TOL):
                    continue
                denom = (w1 - w0) or 1e-9
                tt = t0 + (t1 - t0) * ((w - w0) / denom)
                if abs(tt) <= reach:
                    feet.append(tt)
            hyps.append({"w": w, "bounds": bounds, "feet": feet,
                         "ivals": g["ivals"], "n": g["n"],
                         "cover": sum(hi - lo for lo, hi in g["ivals"])})

        def snap(vals: list[float]) -> list[float]:
            vals = sorted(vals)
            out: list[float] = []
            for v in vals:
                if out and abs(v - out[-1]) <= BOUND_SNAP:
                    continue
                out.append(v)
            return out

        # ---- LEADER OBJECT test (выноска): text sitting on a shelf whose end
        # continues into an oblique leader line.  Such a value annotates an
        # element; it does not measure a span, so it must never enter a
        # dimension diff.  This is an OBJECT test, not a text-regex test.
        leader = None
        for i in par:
            x0, y0, x1, y1 = segs[i]
            t0, w0 = to_tw(x0, y0)
            t1, w1 = to_tw(x1, y1)
            w = (w0 + w1) / 2
            if abs(w) > 1.3 * size or seg_len[i] < 0.7 * text_half * 2:
                continue
            lo, hi = (t0, t1) if t0 <= t1 else (t1, t0)
            if not (lo - 1.0 <= 0.0 <= hi + 1.0):
                continue
            for (ex, ey) in ((x0, y0), (x1, y1)):
                for j in cand:
                    if j == i:
                        continue
                    d = _adiff(seg_ang[j], tang)
                    if not (12.0 <= d <= 78.0):
                        continue
                    if seg_len[j] < 1.2 * size:
                        continue
                    ax, ay, bx, by = segs[j]
                    if min(math.hypot(ax - ex, ay - ey), math.hypot(bx - ex, by - ey)) <= 0.6:
                        leader = {"shelf_len": round(seg_len[i], 3),
                                  "leader_len": round(seg_len[j], 3),
                                  "leader_angle": round(d, 1)}
                        break
                if leader:
                    break
            if leader:
                break

        # ---- bind the value to an interval -------------------------------
        text_len = 2 * text_half
        hyp_bindings = []
        for h in hyps:
            term_t = snap([b["t"] for b in h["bounds"]])
            foot_t = snap(h["feet"])
            for mode, arr in (("terminator", term_t), ("extension_foot", foot_t)):
                if require_terminator and mode != "terminator":
                    continue
                if len(arr) < 2:
                    continue
                cands = []
                if require_extension and mode == "terminator":
                    arr = [v for v in arr
                           if any(abs(v - fv) <= BOUND_SNAP for fv in foot_t)]
                    if len(arr) < 2:
                        continue
                for i in range(len(arr) - 1):
                    a, b = arr[i], arr[i + 1]
                    width = b - a
                    if width < 0.5:
                        continue
                    # the dimension line must actually be drawn across the interval
                    if union_len(h["ivals"], a, b) < COVER_MIN * width:
                        continue
                    if a <= 0.0 <= b:
                        dist, binding = 0.0, "enclosing"
                    else:
                        dist = min(abs(a), abs(b))
                        binding = "displaced"
                        if width > text_len + 3.0 or dist > 4.0 * size:
                            continue
                    cands.append((dist, width, a, b, binding))
                if not cands:
                    continue
                dist, width, a, b, binding = min(cands, key=lambda z: (z[0], z[1]))
                hyp_bindings.append({
                    "w": round(h["w"], 3),
                    "bound_mode": mode,
                    "binding": binding,
                    "t_left": round(a, 3),
                    "t_right": round(b, 3),
                    "span_pt": round(width, 4),
                    "n_bounds": len(arr),
                    "cover": round(h["cover"], 2),
                    "_rank": (0 if binding == "enclosing" else 1, abs(h["w"])),
                })
                break
        for hb in hyp_bindings:
            hb.pop("_rank", None)
        rec = {
            "text_id": t["id"],
            "text": t["text"],
            "kind": kind,
            "value": float(t["text"].replace(" ", "").replace(",", ".")),
            "size": size,
            "rotation": t["rotation"],
            "center": [round(cx, 3), round(cy, 3)],
            "dir": [round(ux, 4), round(uy, 4)],
            "n_hypotheses": len(hyps),
            "leader_object": leader,
            "candidates": hyp_bindings,
        }
        results.append(rec)
        if debug_text and t["id"] == debug_text:
            rec["_debug_hyps"] = [
                {"w": round(h["w"], 3), "cover": round(h["cover"], 2),
                 "term": sorted(round(b["t"], 2) for b in h["bounds"]),
                 "feet": sorted(round(v, 2) for v in h["feet"])}
                for h in sorted(hyps, key=lambda z: abs(z["w"]))
            ]

    select(results, variant=variant)
    scale = fit_scale(results)
    if variant == "C_scale_arbitration" and scale["scale_mm_per_pt"]:
        # second pass: among the geometric alternatives keep the one whose
        # measured span corroborates the printed value under the fitted scale
        S = scale["scale_mm_per_pt"]
        for r in results:
            if not r["candidates"]:
                continue
            scored = sorted(
                r["candidates"],
                key=lambda c: abs(c["span_pt"] * S - r["value"]) / max(r["value"], 1e-9),
            )
            top = scored[0]
            if abs(top["span_pt"] * S - r["value"]) / max(r["value"], 1e-9) <= 0.02:
                r.update({k: v for k, v in top.items()})
                r["detected"] = True
                r["foot_a"] = [round(r["center"][0] + top["t_left"] * r["dir"][0], 3),
                               round(r["center"][1] + top["t_left"] * r["dir"][1], 3)]
                r["foot_b"] = [round(r["center"][0] + top["t_right"] * r["dir"][0], 3),
                               round(r["center"][1] + top["t_right"] * r["dir"][1], 3)]
        scale = fit_scale(results)
    for r in results:
        if r.get("detected") and scale["scale_mm_per_pt"]:
            pred = r["span_pt"] * scale["scale_mm_per_pt"]
            r["predicted_mm"] = round(pred, 2)
            r["rel_err"] = round(abs(pred - r["value"]) / max(r["value"], 1e-9), 4)
            r["scale_ok"] = r["rel_err"] <= 0.02
    for r in results:
        r["n_candidates"] = len(r["candidates"])
        r["alt_spans"] = [c["span_pt"] for c in r["candidates"][:5]]
        del r["candidates"]
    return {
        "source": {"pdf": cache["pdf"], "page_index": cache["page_index"],
                   "block_rect": cache["block_rect"]},
        "variant": variant,
        "terminator_model": terminator_model,
        "require_extension": require_extension,
        "segments_dedup": len(segs),
        "filled_small_paths": len(fills),
        "texts_total": len(cache["texts"]),
        "scale": scale,
        "dimensions": results,
    }


def select(results: list[dict[str, Any]], variant: str) -> None:
    """Pick one binding per value.  Variants isolate what each rule contributes.

    A_nearest_line   : purely nearest parallel line, enclosing preferred
    B_side_convention: + GOST 2.307 / ISO 129 placement (value above its line)
    C_scale_arbitration: B, then re-picked by scale corroboration (2nd pass)
    """
    for r in results:
        cands = r["candidates"]
        if not cands:
            r["detected"] = False
            continue
        if variant == "A_nearest_line":
            key = lambda c: (0 if c["binding"] == "enclosing" else 1, abs(c["w"]))
        else:
            key = lambda c: (0 if c["w"] > 0 else 1,
                             0 if c["binding"] == "enclosing" else 1,
                             abs(c["w"]))
        cands.sort(key=key)
        top = cands[0]
        r.update({k: v for k, v in top.items()})
        r["detected"] = True
        r["foot_a"] = [round(r["center"][0] + top["t_left"] * r["dir"][0], 3),
                       round(r["center"][1] + top["t_left"] * r["dir"][1], 3)]
        r["foot_b"] = [round(r["center"][0] + top["t_right"] * r["dir"][0], 3),
                       round(r["center"][1] + top["t_right"] * r["dir"][1], 3)]


def fit_scale(results: list[dict[str, Any]]) -> dict[str, Any]:
    ratios = [r["value"] / r["span_pt"] for r in results
              if r.get("detected") and r.get("span_pt", 0) > 0.5]
    if len(ratios) < 3:
        return {"scale_mm_per_pt": None, "n": len(ratios)}
    # dominant cluster in log space (handles several drawing scales in one block)
    buckets: dict[int, list[float]] = defaultdict(list)
    for v in ratios:
        buckets[int(round(math.log(v) / 0.05))].append(v)
    # merge neighbouring buckets
    best_key = max(buckets, key=lambda k: len(buckets[k]) + 0.5 * len(buckets.get(k - 1, []))
                   + 0.5 * len(buckets.get(k + 1, [])))
    pool = buckets[best_key] + buckets.get(best_key - 1, []) + buckets.get(best_key + 1, [])
    med = statistics.median(pool)
    return {
        "scale_mm_per_pt": round(med, 4),
        "implied_drawing_scale_1_to": round(med / (25.4 / 72.0), 2),
        "n_in_cluster": len(pool),
        "n_ratios": len(ratios),
        "clusters": {str(k): len(v) for k, v in sorted(buckets.items(), key=lambda z: -len(z[1]))[:6]},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--no-terminator", action="store_true",
                    help="ablation: accept extension-line feet without terminators")
    ap.add_argument("--variant", default="B_side_convention",
                    choices=["A_nearest_line", "B_side_convention", "C_scale_arbitration"])
    ap.add_argument("--require-extension", action="store_true")
    ap.add_argument("--terminator-model", default="slash_object",
                    choices=["raw_endpoint", "slash_object"])
    ap.add_argument("--debug-text")
    a = ap.parse_args()
    cache = json.loads(Path(a.cache).read_text(encoding="utf-8"))
    res = detect(cache, require_terminator=not a.no_terminator, variant=a.variant,
                 terminator_model=a.terminator_model,
                 require_extension=a.require_extension, debug_text=a.debug_text)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    det = [r for r in res["dimensions"] if r.get("detected")]
    ok = [r for r in det if r.get("scale_ok")]
    print(f"candidates={len(res['dimensions'])} detected={len(det)} scale_ok={len(ok)} "
          f"scale={res['scale']}")


if __name__ == "__main__":
    main()
