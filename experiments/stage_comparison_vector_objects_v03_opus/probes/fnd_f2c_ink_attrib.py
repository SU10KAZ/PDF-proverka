# -*- coding: utf-8 -*-
"""F2c — the decisive ink test, per PATH, not per segment.

F2b's "at_risk" bucket (64.7 %) is over-pessimistic: in dense CAD graphics almost any
point has ink within a 5x5 window, so "there is ink near the outline" proves nothing.

A path that paints only white on white paper cannot ADD a mark.  It can only cover.
So there are exactly three outcomes, and each is directly measurable in the raster:

  no_effect        the area the path covers holds no ink and its neighbourhood holds
                   none either -> the path is invisible, dropping it loses nothing
  knockout_visible the area inside is clean while the ring around it carries ink -> the
                   path erased something and its border IS visible as a white hole.
                   Dropping the path is safe for a LINE layer but loses that boundary
  ink_inside       ink lives inside the covered area too -> the ink was drawn by other
                   paths ON TOP.  Then we ask whether a kept segment reproduces the
                   outline (redundant) and whether the outline itself is a dark ridge.
"""
from __future__ import annotations

import collections, json, math, os, sys, time
from pathlib import Path

import numpy as np
from PIL import Image

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
DPI = 200
RS = DPI / 72.0
N_BLOCKS = 120
RIDGE_DROP = 30          # grey levels the on-line pixel must be darker than both sides
RIDGE_SHARE = 0.5        # share of samples that must show the ridge


def ridge_share(A, seg, clip, samples=7):
    H, W = A.shape
    (x0, y0), (x1, y1) = seg["p0"], seg["p1"]
    dx, dy = x1 - x0, y1 - y0
    L = math.hypot(dx, dy)
    if L <= 1e-9:
        return 0.0
    nx, ny = -dy / L, dx / L
    hit = tot = 0
    for k in range(samples):
        t = (k + 0.5) / samples
        cx, cy = x0 + dx * t, y0 + dy * t
        def val(ox, oy):
            px = int((cx + ox - clip[0]) * RS)
            py = int((cy + oy - clip[1]) * RS)
            if 0 <= py < H and 0 <= px < W:
                return int(A[py, px])
            return None
        c = val(0, 0)
        a = val(nx * 0.9, ny * 0.9)
        b = val(-nx * 0.9, -ny * 0.9)
        if c is None or a is None or b is None:
            continue
        tot += 1
        if a - c >= RIDGE_DROP and b - c >= RIDGE_DROP:
            hit += 1
    return hit / tot if tot else 0.0


def main():
    src = json.loads((ART / "fnd_ink.json").read_text(encoding="utf-8"))
    want = {r["block_id"] for r in src["rows"] if r.get("seg_invisible", 0) > 0}
    blocks = []
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            if b["block_id"] in want:
                blocks.append(b)
                if len(blocks) >= N_BLOCKS:
                    break
    print("blocks:", len(blocks))
    tot = collections.Counter()
    by_rule = collections.defaultdict(collections.Counter)
    rows = []
    t0 = time.time()
    for i, b in enumerate(blocks):
        try:
            fr = F.block_frame(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"])
            ex = F.extract_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"],
                                 frame=fr, keep_dropped_segments=True)
            dropped = ex.quality.pop("dropped_segments", [])
            if not dropped:
                continue
            pix = F.render_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"],
                                 dpi=DPI, frame=fr)
            if pix.width * pix.height > 60_000_000:
                rows.append({"block_id": b["block_id"], "skipped": "raster too large"})
                F.clear_caches(); continue
            A = np.asarray(Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                           .convert("L")).astype(np.int16)
            ink = (A < 250)
            H, W = ink.shape
            integ = np.zeros((H + 1, W + 1), dtype=np.int64)
            integ[1:, 1:] = np.cumsum(np.cumsum(ink, axis=0), axis=1)

            def dens(x0, y0, x1, y1):
                x0, y0 = max(0, int(x0)), max(0, int(y0))
                x1, y1 = min(W, int(x1)), min(H, int(y1))
                if x1 <= x0 or y1 <= y0:
                    return None, 0
                s = integ[y1, x1] - integ[y0, x1] - integ[y1, x0] + integ[y0, x0]
                n = (x1 - x0) * (y1 - y0)
                return s / n, n

            clip = ex.frame["clip_display"]
            paths = collections.defaultdict(list)
            for s in dropped:
                paths[s["path"]].append(s)
            r = {"block_id": b["block_id"], "discipline": b["discipline"],
                 "n_dropped_paths": len(paths), "n_dropped_segments": len(dropped),
                 "n_kept_segments": len(ex.segments)}
            c = collections.Counter()
            for pidx, segs in paths.items():
                xs = [p for s in segs for p in (s["p0"][0], s["p1"][0])]
                ys = [p for s in segs for p in (s["p0"][1], s["p1"][1])]
                bx0 = (min(xs) - clip[0]) * RS
                bx1 = (max(xs) - clip[0]) * RS
                by0 = (min(ys) - clip[1]) * RS
                by1 = (max(ys) - clip[1]) * RS
                pad = 3
                d_in, n_in = dens(bx0 + 1, by0 + 1, bx1 - 1, by1 - 1)
                d_out, n_out = dens(bx0 - pad, by0 - pad, bx1 + pad, by1 + pad)
                if d_in is None or n_in < 9 or d_out is None or n_out <= n_in:
                    bucket = "too_small_to_judge"
                else:
                    # ring density = outer box minus inner box
                    s_out = d_out * n_out
                    s_in_full, n_in_full = dens(bx0, by0, bx1, by1)
                    s_in_full = (s_in_full or 0) * n_in_full
                    ring_n = n_out - n_in_full
                    d_ring = (s_out - s_in_full) / ring_n if ring_n > 0 else 0.0
                    if d_in < 0.005 and d_ring < 0.02:
                        bucket = "no_effect"
                    elif d_in < 0.3 * d_ring and d_ring >= 0.05:
                        bucket = "knockout_visible"
                    else:
                        bucket = "ink_inside"
                if bucket == "ink_inside":
                    rs_ = [ridge_share(A, s, clip) for s in segs[:24]]
                    if rs_ and float(np.mean([1.0 if v >= RIDGE_SHARE else 0.0 for v in rs_])) >= 0.5:
                        bucket = "ink_inside_outline_is_ridge"
                    else:
                        bucket = "ink_inside_outline_not_a_line"
                c[bucket] += 1
                tot[bucket] += 1
                tot["seg_" + bucket] += len(segs)
                by_rule[segs[0]["ink_rule"]][bucket] += 1
            r["buckets"] = dict(c)
            rows.append(r)
        except Exception as exc:
            rows.append({"block_id": b["block_id"], "error": f"{type(exc).__name__}: {exc}"})
        F.clear_caches()
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(blocks)} {time.time()-t0:.0f}s", flush=True)

    keys = ["no_effect", "knockout_visible", "ink_inside_outline_is_ridge",
            "ink_inside_outline_not_a_line", "too_small_to_judge"]
    npaths = sum(tot[k] for k in keys)
    nsegs = sum(tot["seg_" + k] for k in keys)
    summary = {
        "n_blocks": len([r for r in rows if "buckets" in r]),
        "n_dropped_paths": npaths, "n_dropped_segments": nsegs,
        "paths": {k: tot[k] for k in keys},
        "paths_share": {k: tot[k] / max(1, npaths) for k in keys},
        "segments": {k: tot["seg_" + k] for k in keys},
        "segments_share": {k: tot["seg_" + k] / max(1, nsegs) for k in keys},
        "by_rule": {k: dict(v) for k, v in by_rule.items()},
        "params": {"dpi": DPI, "ridge_drop": RIDGE_DROP, "ridge_share": RIDGE_SHARE},
    }
    (ART / "fnd_ink_attrib.json").write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
