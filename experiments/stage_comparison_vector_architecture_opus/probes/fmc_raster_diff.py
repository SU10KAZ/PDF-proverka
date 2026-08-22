#!/usr/bin/env python3
"""FMC probe step 4 — localize visual change between two matched pages by raster diff.

Renders both pages to the SAME pixel grid, thresholds the absolute difference and returns
bounding boxes (page-normalized) of connected changed regions.  Used only to FIND candidate
regions for the corpus; no comparator decision depends on it.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_raster_diff \
        --left <pdf> --li 20 --right <pdf> --ri 21 [--px 1400] [--top 8] [--png prefix]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import fitz
import numpy as np
from scipy import ndimage

ROOT = Path(__file__).resolve().parents[3]


def _p(rel: str) -> Path:
    p = Path(rel)
    return p if p.is_absolute() else ROOT / rel


def render(page, px: int) -> np.ndarray:
    scale = px / max(page.rect.width, page.rect.height)
    pm = page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csGRAY, alpha=False)
    return np.frombuffer(pm.samples, dtype=np.uint8).reshape(pm.height, pm.width)


def diff_regions(a: np.ndarray, b: np.ndarray, thresh: int = 60, dilate: int = 6, min_px: int = 40):
    h = min(a.shape[0], b.shape[0])
    w = min(a.shape[1], b.shape[1])
    a2, b2 = a[:h, :w].astype(np.int16), b[:h, :w].astype(np.int16)
    d = np.abs(a2 - b2) > thresh
    if dilate:
        d = ndimage.binary_dilation(d, np.ones((dilate, dilate), bool))
    lab, n = ndimage.label(d)
    out = []
    if n:
        objs = ndimage.find_objects(lab)
        for idx, sl in enumerate(objs, start=1):
            cnt = int((lab[sl] == idx).sum())
            if cnt < min_px:
                continue
            y0, y1 = sl[0].start, sl[0].stop
            x0, x1 = sl[1].start, sl[1].stop
            out.append(
                {
                    "px_count": cnt,
                    "bbox_px": [x0, y0, x1, y1],
                    "bbox_norm": [round(x0 / w, 5), round(y0 / h, 5), round(x1 / w, 5), round(y1 / h, 5)],
                    "area_frac": round((x1 - x0) * (y1 - y0) / (w * h), 6),
                }
            )
    out.sort(key=lambda r: -r["px_count"])
    changed_frac = float(d.mean())
    return out, changed_frac, (w, h)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--left", required=True)
    ap.add_argument("--li", type=int, required=True)
    ap.add_argument("--right", required=True)
    ap.add_argument("--ri", type=int, required=True)
    ap.add_argument("--px", type=int, default=1600)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--thresh", type=int, default=60)
    ap.add_argument("--png")
    a = ap.parse_args()
    dl, dr = fitz.open(_p(a.left)), fitz.open(_p(a.right))
    ia, ib = render(dl[a.li], a.px), render(dr[a.ri], a.px)
    regs, frac, (w, h) = diff_regions(ia, ib, thresh=a.thresh)
    print(json.dumps({"grid": [w, h], "changed_pixel_frac": round(frac, 5), "regions": regs[: a.top]}, ensure_ascii=False, indent=1))
    if a.png:
        out = Path(a.png)
        out.parent.mkdir(parents=True, exist_ok=True)
        from PIL import Image
        Image.fromarray(ia).save(str(out) + "_L.png")
        Image.fromarray(ib).save(str(out) + "_R.png")
        hh = min(ia.shape[0], ib.shape[0]); ww = min(ia.shape[1], ib.shape[1])
        d = (np.abs(ia[:hh, :ww].astype(np.int16) - ib[:hh, :ww].astype(np.int16)) > a.thresh)
        Image.fromarray((255 - d.astype(np.uint8) * 255)).save(str(out) + "_D.png")


if __name__ == "__main__":
    main()
