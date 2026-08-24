#!/usr/bin/env python3
"""One contact sheet per pair: up to N difference clusters, each as a row
[left | right | overlay], cut from the PAGE so the crop edge is visible.

Used for the human label; nothing here touches the vector detector.
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

from experiments.local_graphic_diff_mode1_opus.m1.core import PAGES  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.probes.gt_tool import (  # noqa: E402
    GT_CELL, block_of, raster_regions, _fit)


def _page_gray(block, rect_pt, cell):
    page = PAGES.page(block.pdf, block.page_index)["page"]
    pm = page.get_pixmap(clip=fitz.Rect(*rect_pt), matrix=fitz.Matrix(1 / cell, 1 / cell),
                         colorspace=fitz.csGRAY, alpha=False)
    return np.frombuffer(pm.samples, dtype=np.uint8).reshape(pm.height, pm.width).copy()


def sheet(pair, regions_px, outpath, rows=6, cell=GT_CELL):
    a = block_of(pair["pdf_left"], pair["page_index_left"], pair["bbox_left"])
    b = block_of(pair["pdf_right"], pair["page_index_right"], pair["bbox_right"])
    R = raster_regions(a, b, cell=cell)
    rgb = np.dstack([np.minimum(R["gray_left"], R["gray_right"])] * 3).astype(np.uint8)
    rgb[R["ao"] > 0] = [255, 0, 0]
    rgb[R["bo"] > 0] = [0, 180, 0]
    dx_pt, dy_pt = R["shift"][0] * cell, R["shift"][1] * cell
    H, W = R["shape"]
    panels = []
    for i, px in enumerate(regions_px[:rows]):
        x0, y0, x1, y1 = px
        side = max(x1 - x0, y1 - y0)
        half = max(70, side // 2 + max(40, int(0.5 * side)))
        cx, cy = (x0 + x1) // 2, (y0 + y1) // 2
        X0, Y0 = max(0, cx - half), max(0, cy - half)
        X1, Y1 = min(W, cx + half), min(H, cy + half)
        ov = rgb[Y0:Y1, X0:X1].copy()
        lrect = [a.bbox_vis[0] + X0 * cell - dx_pt, a.bbox_vis[1] + Y0 * cell - dy_pt,
                 a.bbox_vis[0] + X1 * cell - dx_pt, a.bbox_vis[1] + Y1 * cell - dy_pt]
        rrect = [b.bbox_vis[0] + X0 * cell, b.bbox_vis[1] + Y0 * cell,
                 b.bbox_vis[0] + X1 * cell, b.bbox_vis[1] + Y1 * cell]
        try:
            cl, cr = _page_gray(a, lrect, cell), _page_gray(b, rrect, cell)
        except Exception:
            continue
        hh = min(cl.shape[0], cr.shape[0], ov.shape[0]); ww = min(cl.shape[1], cr.shape[1], ov.shape[1])
        if hh < 8 or ww < 8:
            continue
        cl, cr, ov = cl[:hh, :ww], cr[:hh, :ww], ov[:hh, :ww]
        k = min(3.5, max(1.0, 230 / max(hh, ww)))
        rs = lambda im: cv2.resize(im, None, fx=k, fy=k, interpolation=cv2.INTER_NEAREST)
        cl3, cr3, ov3 = np.dstack([rs(cl)] * 3), np.dstack([rs(cr)] * 3), rs(ov)
        sep = np.full((cl3.shape[0], 4, 3), 128, np.uint8)
        row = np.hstack([cl3, sep, cr3, sep, ov3])
        cv2.putText(row, f"#{i+1}", (4, 18), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 0, 255), 1, cv2.LINE_AA)
        panels.append(row)
    if not panels:
        return None
    wmax = max(p.shape[1] for p in panels)
    padded = []
    for p in panels:
        if p.shape[1] < wmax:
            p = np.hstack([p, np.full((p.shape[0], wmax - p.shape[1], 3), 255, np.uint8)])
        padded.append(p)
        padded.append(np.full((3, wmax, 3), 60, np.uint8))
    img = np.vstack(padded)
    cv2.imwrite(str(outpath), cv2.cvtColor(_fit(img, 1400, 1000), cv2.COLOR_RGB2BGR))
    return str(outpath)


def main():
    bench = {p["pair_id"]: p for p in json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]}
    assist = {x["pair_id"]: x for x in json.loads((ART / "gt_assist.json").read_text(encoding="utf-8"))["items"]}
    only = sys.argv[1:] or list(bench)
    out = ART / "gt_sheets"
    out.mkdir(parents=True, exist_ok=True)
    for pid in only:
        a = assist.get(pid, {})
        regs = [r["px"] for r in a.get("regions", []) if r["proposal"] == "REAL"][:6]
        if not regs:
            regs = [r["px"] for r in a.get("regions", [])][:6]
        if not regs:
            print(pid, "no regions")
            continue
        p = sheet(bench[pid], regs, out / f"{pid}_sheet.png")
        print(pid, p, flush=True)


if __name__ == "__main__":
    main()
