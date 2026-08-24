#!/usr/bin/env python3
"""Ground-truth assistant — raster-side evidence for every difference cluster.

Purely raster: renders, phase correlation, page context.  It shares nothing with
the vector detector under test, so it can serve as independent evidence for the
human label.  For every cluster it answers three objective questions:

* does the cluster survive when the same area is re-rendered from the PAGE with
  a margin around it (i.e. is it a real appearance/disappearance or just the two
  crops ending in different places)?
* does it sit inside text boxes (text is another pipeline's business)?
* does it touch the common area's edge?

The human then confirms or overrides each proposal by looking at the zooms.
"""
from __future__ import annotations

import json
import pathlib
import sys

import cv2
import fitz
import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.m1.core import PAGES, text_spans  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.probes.gt_tool import (  # noqa: E402
    GT_CELL, DARK, block_of, raster_regions)

MARGIN_PT = 30.0


def _page_gray(block, rect_pt, cell):
    page = PAGES.page(block.pdf, block.page_index)["page"]
    pm = page.get_pixmap(clip=fitz.Rect(*rect_pt), matrix=fitz.Matrix(1 / cell, 1 / cell),
                         colorspace=fitz.csGRAY, alpha=False)
    return np.frombuffer(pm.samples, dtype=np.uint8).reshape(pm.height, pm.width).copy()


def region_evidence(pair, cell=GT_CELL, tol_px=2):
    a = block_of(pair["pdf_left"], pair["page_index_left"], pair["bbox_left"])
    b = block_of(pair["pdf_right"], pair["page_index_right"], pair["bbox_right"])
    R = raster_regions(a, b, cell=cell, tol_px=tol_px)
    dx_pt, dy_pt = R["shift"][0] * cell, R["shift"][1] * cell
    H, W = R["shape"]
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * tol_px + 1, 2 * tol_px + 1))

    tboxes = []
    for blk, off in ((a, (a.bbox_vis[0] - dx_pt, a.bbox_vis[1] - dy_pt)), (b, (b.bbox_vis[0], b.bbox_vis[1]))):
        for s in text_spans(blk):
            bb = s["bbox"]
            tboxes.append([(bb[0] - off[0]) / cell, (bb[1] - off[1]) / cell,
                           (bb[2] - off[0]) / cell, (bb[3] - off[1]) / cell])
    tmask = np.zeros((H, W), np.uint8)
    for bb in tboxes:
        x0 = max(0, int(bb[0] - 2)); y0 = max(0, int(bb[1] - 2))
        x1 = min(W, int(bb[2] + 3)); y1 = min(H, int(bb[3] + 3))
        if x1 > x0 and y1 > y0:
            tmask[y0:y1, x0:x1] = 1

    out = []
    for r in R["regions"]:
        x0, y0, x1, y1 = r["px"]
        sub_t = tmask[y0:y1, x0:x1]
        text_share = float(sub_t.mean()) if sub_t.size else 0.0
        edge = min(x0, y0, W - x1, H - y1)
        # re-diff the same physical area taken from the pages with a margin
        m = MARGIN_PT
        lrect = [a.bbox_vis[0] + x0 * cell - dx_pt - m, a.bbox_vis[1] + y0 * cell - dy_pt - m,
                 a.bbox_vis[0] + x1 * cell - dx_pt + m, a.bbox_vis[1] + y1 * cell - dy_pt + m]
        rrect = [b.bbox_vis[0] + x0 * cell - m, b.bbox_vis[1] + y0 * cell - m,
                 b.bbox_vis[0] + x1 * cell + m, b.bbox_vis[1] + y1 * cell + m]
        try:
            gl = _page_gray(a, lrect, cell)
            gr = _page_gray(b, rrect, cell)
            h = min(gl.shape[0], gr.shape[0]); w = min(gl.shape[1], gr.shape[1])
            A = (gl[:h, :w] < DARK).astype(np.uint8); B = (gr[:h, :w] < DARK).astype(np.uint8)
            ao = int((A & ~cv2.dilate(B, ker)).sum()); bo = int((B & ~cv2.dilate(A, ker)).sum())
            with_margin = ao + bo
        except Exception:
            with_margin = None
        base = r["cells"]
        # is the cluster explained by the SAME ink drawn a little way off?
        # correlate the left-only mask against the right-only mask inside the
        # cluster: dominated by unchanged ink a whole-window correlation always
        # peaks at zero, so only the changed ink is correlated here.
        local_shift = None
        overlap = None
        try:
            sub_a = R["ao"][y0:y1, x0:x1].astype(np.uint8)
            sub_b = R["bo"][y0:y1, x0:x1].astype(np.uint8)
            if sub_a.sum() >= 8 and sub_b.sum() >= 8:
                best = (0, 0, -1.0)
                rad = 6
                pad_a = np.pad(sub_a, rad)
                for ddy in range(-rad, rad + 1):
                    for ddx in range(-rad, rad + 1):
                        sh = pad_a[rad + ddy:rad + ddy + sub_a.shape[0], rad + ddx:rad + ddx + sub_a.shape[1]]
                        inter = int((sh & sub_b).sum())
                        sc = inter / max(1, min(int(sub_a.sum()), int(sub_b.sum())))
                        if sc > best[2]:
                            best = (ddx, ddy, sc)
                # a 1-cell dilation makes the score robust to rasterisation
                ddx, ddy, _ = best
                sh = pad_a[rad + ddy:rad + ddy + sub_a.shape[0], rad + ddx:rad + ddx + sub_a.shape[1]]
                inter = int((cv2.dilate(sh, np.ones((3, 3), np.uint8)) & sub_b).sum())
                overlap = round(inter / max(1, min(int(sub_a.sum()), int(sub_b.sum()))), 3)
                local_shift = round(float(np.hypot(ddx, ddy) * cell), 3)
        except Exception:
            pass
        prop = "REAL"
        if text_share >= 0.9:
            prop = "TEXT"
        elif with_margin is not None and with_margin <= 0.3 * base:
            prop = "CROP"
        elif overlap is not None and overlap >= 0.6 and local_shift is not None and local_shift <= 2.5:
            prop = "HAIRLINE"
        elif overlap is not None and overlap >= 0.6 and local_shift is not None:
            prop = "MOVED"
        out.append({
            "px": r["px"], "cells": base, "left_only": r["left_only"], "right_only": r["right_only"],
            "text_share": round(text_share, 3), "edge_px": int(edge),
            "cells_with_margin": with_margin,
            "local_shift_pt": local_shift,
            "changed_ink_overlap_after_shift": overlap,
            "proposal": prop,
        })
    return {"pair_id": pair["pair_id"], "shape": R["shape"], "shift_px": R["shift"],
            "changed_cells": R["changed_cells"], "ink_cells": R["ink_cells"],
            "changed_fraction": round(R["changed_cells"] / max(1, R["ink_cells"]), 5),
            "regions": out}


def main():
    bench = json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]
    only = set(sys.argv[1:]) or None
    rows = []
    for p in bench:
        if only and p["pair_id"] not in only:
            continue
        try:
            r = region_evidence(p)
        except Exception as e:  # noqa: BLE001
            r = {"pair_id": p["pair_id"], "error": f"{type(e).__name__}: {e}"}
        rows.append(r)
        props = [x["proposal"] for x in r.get("regions", [])]
        print(f"{r['pair_id']:28s} cf={r.get('changed_fraction')} regions={len(props)} {props}", flush=True)
    (ART / "gt_assist.json").write_text(json.dumps({"probe": "gt_assist", "items": rows}, ensure_ascii=False),
                                        encoding="utf-8")
    print("wrote", ART / "gt_assist.json")


if __name__ == "__main__":
    main()
