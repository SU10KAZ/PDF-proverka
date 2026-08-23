# -*- coding: utf-8 -*-
"""G0 — COMPLETENESS of the object layer against the picture, not against itself.

The gate-fix lesson (fnd_GATEFIX.md): a metric of the form "what we found is correct"
cannot show that half was never found.  So the object layer is checked twice:

  (a) against the extractor:  share of inked segment length that belongs to an object
      (must be exactly 1.0 by construction — an assertion, not a result);
  (b) against the rendered block: share of DARK pixels that no object's ink, no text
      box and no raster insert explains.  This is the number that would catch a
      grouping that silently drops geometry.

Usage:  grp_g0_completeness.py <shard> <nshards>
"""
from __future__ import annotations
import json, math, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import numpy as np
from PIL import Image, ImageDraw

SEED = 20260823
DPI = 100


def run_block(rec):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    ex = G.extract(pb)
    if not ex.segments:
        return None
    L = G.layer_of(ex.segments, ex.texts)
    pix = G.F.render_block(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w,
                           pb.page_px_h, dpi=DPI, min_long_side=0)
    if pix.width * pix.height > 12_000_000:
        return None
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    gray = img[:, :, :3].mean(axis=2)
    dark = gray < 250
    clip = ex.frame["clip_display"]
    sx = pix.width / max(clip[2] - clip[0], 1e-9)
    sy = pix.height / max(clip[3] - clip[1], 1e-9)
    cover = np.zeros_like(dark)
    H, W = dark.shape

    def stamp(x, y, r=1):
        xi, yi = int(x), int(y)
        x0, x1 = max(0, xi - r), min(W, xi + r + 1)
        y0, y1 = max(0, yi - r), min(H, yi + r + 1)
        if x1 > x0 and y1 > y0:
            cover[y0:y1, x0:x1] = True

    for o in L.objects:
        for gi in o["segments"]:
            s = ex.segments[gi]
            ax, ay = (s["p0"][0] - clip[0]) * sx, (s["p0"][1] - clip[1]) * sy
            bx, by = (s["p1"][0] - clip[0]) * sx, (s["p1"][1] - clip[1]) * sy
            n = max(2, int(math.hypot(bx - ax, by - ay)) + 1)
            for k in range(n + 1):
                t = k / n
                stamp(ax + t * (bx - ax), ay + t * (by - ay))
    # filled paths: the INTERIOR is ink too.  Without this the metric blames the
    # grouping for the grey shading of a plan (measured: one block read 63 % unexplained
    # and every unexplained pixel was inside two filled polygons).
    fill_img = Image.new("1", (W, H), 0)
    fd = ImageDraw.Draw(fill_img)
    n_fill_poly = 0
    for pr in L.prims:
        segs_here = pr["members"]
        if not segs_here or not ex.segments[segs_here[0]].get("fill"):
            continue
        pts = [((q[0] - clip[0]) * sx, (q[1] - clip[1]) * sy) for q in pr["pts"]]
        if len(pts) >= 3:
            fd.polygon(pts, fill=1)
            n_fill_poly += 1
    cover |= np.array(fill_img, dtype=bool)

    for t in ex.texts:
        b = t["bbox"]
        x0 = int(max(0, (b[0] - clip[0]) * sx - 1)); x1 = int(min(W, (b[2] - clip[0]) * sx + 2))
        y0 = int(max(0, (b[1] - clip[1]) * sy - 1)); y1 = int(min(H, (b[3] - clip[1]) * sy + 2))
        if x1 > x0 and y1 > y0:
            cover[y0:y1, x0:x1] = True
    for im in ex.images:
        b = im["bbox"]
        x0 = int(max(0, (b[0] - clip[0]) * sx)); x1 = int(min(W, (b[2] - clip[0]) * sx + 1))
        y0 = int(max(0, (b[1] - clip[1]) * sy)); y1 = int(min(H, (b[3] - clip[1]) * sy + 1))
        if x1 > x0 and y1 > y0:
            cover[y0:y1, x0:x1] = True
    n_dark = int(dark.sum())
    unexplained = int((dark & ~cover).sum())
    # what a NON-stray-only layer would explain (are strays carrying real ink?)
    return {"block_id": rec["block_id"], "discipline": rec["discipline"], "cls": rec["cls"],
            "bucket": rec["bucket"], "n_seg": len(ex.segments), "n_obj": len(L.objects),
            "ink_coverage_vs_extractor": L.stats["ink_coverage"],
            "unassigned_segments": L.stats["unassigned_segments"],
            "dark_px": n_dark, "n_fill_polygons": n_fill_poly,
            "unexplained_dark_share": round(unexplained / max(1, n_dark), 5),
            "stray_len_share": L.stats["stray_len_share"]}


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if 20 <= b["n_seg"] <= 60000]
    rng = random.Random(SEED)
    rng.shuffle(blocks)
    blocks = blocks[:100]
    blocks = [b for i, b in enumerate(blocks) if i % nsh == shard]
    outp = G.ART / f"grp_runs/g0_{shard}.jsonl"
    outp.parent.mkdir(exist_ok=True)
    with open(outp, "w", encoding="utf-8") as fh:
        for k, rec in enumerate(blocks):
            try:
                r = run_block(rec)
            except Exception as e:
                r = {"block_id": rec["block_id"], "error": repr(e)}
            if r:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                fh.flush()
            print(f"[{shard}] {k+1}/{len(blocks)}", flush=True)


if __name__ == "__main__":
    main()
