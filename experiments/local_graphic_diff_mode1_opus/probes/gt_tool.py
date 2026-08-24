#!/usr/bin/env python3
"""Ground-truth tooling: renders the evidence a human needs to label a pair.

For every pair it produces, from the RENDERED pages only (no vector detector
involved, so the label stays independent of the system under test):

  <pair>_side.png     left | right, side by side, downscaled to fit;
  <pair>_ov.png       overlay: red = only on the left, green = only on the right,
                      after a translation-only alignment of the two renders;
  <pair>_z<i>.png     zoom on the i-th largest raster difference cluster.

The label itself is made by a person looking at these images:
NO_CHANGE / LOCAL_CHANGE / CROP_DIFFERENCE / UNSURE (+ bbox of the change).
"""
from __future__ import annotations

import json
import math
import pathlib
import sys

import cv2
import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.m1.core import (  # noqa: E402
    PAGES, Block, block_from_record, render_gray)

GT_CELL = 0.6      # pt/px for the evidence renders (~42 dpi on the sheet)
DARK = 200


def block_of(pdf, page_index, bbox_norm, bid="", label=""):
    rect = PAGES.page(pdf, page_index)["rect"]
    return block_from_record(pdf, {"coords_norm": bbox_norm, "page_index": page_index,
                                   "id": bid, "ocr_label": label}, rect)


def raster_regions(a: Block, b: Block, cell=GT_CELL, min_area=40, merge_px=6, tol_px=2):
    ga, gb = render_gray(a, cell), render_gray(b, cell)
    h = min(ga.shape[0], gb.shape[0]); w = min(ga.shape[1], gb.shape[1])
    ga, gb = ga[:h, :w], gb[:h, :w]
    A = (ga < DARK).astype(np.uint8); B = (gb < DARK).astype(np.uint8)
    try:
        win = cv2.createHanningWindow((w, h), cv2.CV_32F)
        (dx, dy), _ = cv2.phaseCorrelate(A.astype(np.float32) * win, B.astype(np.float32) * win)
    except Exception:
        dx = dy = 0.0
    M = np.float32([[1, 0, dx], [0, 1, dy]])
    Aw = cv2.warpAffine(A, M, (w, h), flags=cv2.INTER_NEAREST)
    gaw = cv2.warpAffine(ga, M, (w, h), flags=cv2.INTER_NEAREST, borderValue=255)
    # tolerance matches the physical tolerance MODE 1 works at (1.2 pt): a line
    # redrawn half a pixel away is not a project change and must not enter GT
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * tol_px + 1, 2 * tol_px + 1))
    ao = (Aw & ~cv2.dilate(B, ker)).astype(np.uint8)
    bo = (B & ~cv2.dilate(Aw, ker)).astype(np.uint8)
    u = ((ao | bo) > 0).astype(np.uint8)
    mk = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * merge_px + 1, 2 * merge_px + 1))
    n, lab, st, _ = cv2.connectedComponentsWithStats(cv2.dilate(u, mk), 8)
    regs = []
    for i in range(1, n):
        sel = lab == i
        area = int((u[sel]).sum())
        if area < min_area:
            continue
        x, y, ww, hh, _ = st[i]
        regs.append({"px": [int(x), int(y), int(x + ww), int(y + hh)], "cells": area,
                     "left_only": int(ao[sel].sum()), "right_only": int(bo[sel].sum())})
    regs.sort(key=lambda r: -r["cells"])
    return {"shape": [h, w], "shift": [round(float(dx), 2), round(float(dy), 2)],
            "gray_left": gaw, "gray_right": gb, "ao": ao, "bo": bo,
            "regions": regs, "changed_cells": int(u.sum()),
            "ink_cells": int(Aw.sum()) + int(B.sum())}


def _fit(img, maxw=1500, maxh=900):
    h, w = img.shape[:2]
    s = min(1.0, maxw / w, maxh / h)
    if s < 1.0:
        img = cv2.resize(img, (max(1, int(w * s)), max(1, int(h * s))), interpolation=cv2.INTER_AREA)
    return img


def _page_crop(block, rect_pt, cell):
    import fitz
    from experiments.local_graphic_diff_mode1_opus.m1.core import PAGES
    page = PAGES.page(block.pdf, block.page_index)["page"]
    pm = page.get_pixmap(clip=fitz.Rect(*rect_pt), matrix=fitz.Matrix(1.0 / cell, 1.0 / cell),
                         colorspace=fitz.csGRAY, alpha=False)
    return np.frombuffer(pm.samples, dtype=np.uint8).reshape(pm.height, pm.width).copy()


def render_pair(pair, outdir: pathlib.Path, zooms=3):
    a = block_of(pair["pdf_left"], pair["page_index_left"], pair["bbox_left"])
    b = block_of(pair["pdf_right"], pair["page_index_right"], pair["bbox_right"])
    R = raster_regions(a, b)
    outdir.mkdir(parents=True, exist_ok=True)
    pid = pair["pair_id"]
    gl, gr = R["gray_left"], R["gray_right"]
    sep = np.full((gl.shape[0], 6), 128, np.uint8)
    side = np.hstack([gl, sep, gr])
    cv2.imwrite(str(outdir / f"{pid}_side.png"), _fit(side))
    rgb = np.dstack([np.minimum(gl, gr)] * 3).astype(np.uint8)
    rgb[R["ao"] > 0] = [255, 0, 0]
    rgb[R["bo"] > 0] = [0, 180, 0]
    for r in R["regions"][:8]:
        x0, y0, x1, y1 = r["px"]
        cv2.rectangle(rgb, (x0 - 3, y0 - 3), (x1 + 3, y1 + 3), (0, 0, 255), 1)
    cv2.imwrite(str(outdir / f"{pid}_ov.png"), cv2.cvtColor(_fit(rgb), cv2.COLOR_RGB2BGR))
    zpaths = []
    for i, r in enumerate(R["regions"][:zooms]):
        x0, y0, x1, y1 = r["px"]
        side_px = max(x1 - x0, y1 - y0)
        pad = max(40, int(0.6 * side_px))
        cx, cy = (x0 + x1) // 2, (y0 + y1) // 2
        half = max(60, side_px // 2 + pad)
        X0, Y0 = max(0, cx - half), max(0, cy - half)
        X1, Y1 = min(gl.shape[1], cx + half), min(gl.shape[0], cy + half)
        ov = rgb[Y0:Y1, X0:X1].copy()
        # the zoom panels are cut from the PAGE, not from the crop, so that a
        # difference sitting on the crop edge can still be judged by eye
        dx_pt, dy_pt = R["shift"][0] * GT_CELL, R["shift"][1] * GT_CELL
        rr = [X0 * GT_CELL, Y0 * GT_CELL, X1 * GT_CELL, Y1 * GT_CELL]
        lrect = [a.bbox_vis[0] + rr[0] - dx_pt, a.bbox_vis[1] + rr[1] - dy_pt,
                 a.bbox_vis[0] + rr[2] - dx_pt, a.bbox_vis[1] + rr[3] - dy_pt]
        rrect = [b.bbox_vis[0] + rr[0], b.bbox_vis[1] + rr[1],
                 b.bbox_vis[0] + rr[2], b.bbox_vis[1] + rr[3]]
        try:
            cl, cr = _page_crop(a, lrect, GT_CELL), _page_crop(b, rrect, GT_CELL)
        except Exception:
            cl, cr = gl[Y0:Y1, X0:X1].copy(), gr[Y0:Y1, X0:X1].copy()
        hh = min(cl.shape[0], cr.shape[0], ov.shape[0]); ww = min(cl.shape[1], cr.shape[1], ov.shape[1])
        cl, cr, ov = cl[:hh, :ww], cr[:hh, :ww], ov[:hh, :ww]
        if cl.size == 0 or cr.size == 0:
            continue
        k = min(5.0, max(1.0, 300 / max(1, max(cl.shape))))
        rs = lambda im: cv2.resize(im, None, fx=k, fy=k, interpolation=cv2.INTER_NEAREST)
        cl3, cr3, ov3 = np.dstack([rs(cl)] * 3), np.dstack([rs(cr)] * 3), rs(ov)
        sep2 = np.full((cl3.shape[0], 6, 3), 128, np.uint8)
        z = np.hstack([cl3, sep2, cr3, sep2, ov3])
        pth = outdir / f"{pid}_z{i}.png"
        cv2.imwrite(str(pth), cv2.cvtColor(_fit(z, 1400, 620), cv2.COLOR_RGB2BGR))
        zpaths.append(str(pth))
    return {
        "pair_id": pid,
        "shape": R["shape"], "shift_px": R["shift"],
        "raster_changed_cells": R["changed_cells"],
        "raster_ink_cells": R["ink_cells"],
        "raster_changed_fraction": round(R["changed_cells"] / max(1, R["ink_cells"]), 5),
        "regions_px": [r["px"] for r in R["regions"][:8]],
        "regions_cells": [r["cells"] for r in R["regions"][:8]],
        "cell_pt": GT_CELL,
        "side": str(outdir / f"{pid}_side.png"),
        "overlay": str(outdir / f"{pid}_ov.png"),
        "zooms": zpaths,
    }


def px_to_pt(pair, px, side="right"):
    b = block_of(pair[f"pdf_{side}"], pair[f"page_index_{side}"], pair[f"bbox_{side}"])
    x0, y0 = b.bbox_vis[0], b.bbox_vis[1]
    return [round(x0 + px[0] * GT_CELL, 2), round(y0 + px[1] * GT_CELL, 2),
            round(x0 + px[2] * GT_CELL, 2), round(y0 + px[3] * GT_CELL, 2)]


def main():
    bench = json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]
    only = set(sys.argv[1:]) or None
    outdir = ART / "gt_evidence"
    recs = []
    for p in bench:
        if only and p["pair_id"] not in only:
            continue
        r = render_pair(p, outdir)
        r["regions_pt"] = [px_to_pt(p, b) for b in r["regions_px"]]
        recs.append(r)
        print(p["pair_id"], r["raster_changed_fraction"], len(r["regions_px"]), flush=True)
    path = ART / "gt_evidence_index.json"
    old = {}
    if path.exists():
        old = {x["pair_id"]: x for x in json.loads(path.read_text(encoding="utf-8"))["items"]}
    for r in recs:
        old[r["pair_id"]] = r
    path.write_text(json.dumps({"probe": "gt_tool", "items": list(old.values())}, ensure_ascii=False), encoding="utf-8")
    print("wrote", path)


if __name__ == "__main__":
    main()
