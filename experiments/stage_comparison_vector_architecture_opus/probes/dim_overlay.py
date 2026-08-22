#!/usr/bin/env python3
"""dim_* probe: draw detected dimension objects over the rendered block.

Green  = bound and the measured span corroborates the printed value (<=2%)
Red    = bound but the measured span contradicts the printed value
Yellow = numeric text the detector refused to bind

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_overlay \
        --result <detector json> --blocks <blocks.json> --pair <id> --side left \
        --out <png> [--dpi 150] [--sub x0,y0,x1,y1]
"""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

import fitz
from PIL import Image, ImageDraw


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--result", required=True)
    ap.add_argument("--blocks", default="experiments/stage_comparison_vector_architecture_opus/probes/dim_blocks.json")
    ap.add_argument("--pair", required=True)
    ap.add_argument("--side", default="left")
    ap.add_argument("--out", required=True)
    ap.add_argument("--dpi", type=int, default=150)
    ap.add_argument("--sub", help="sub-rect of the block in fractions x0,y0,x1,y1")
    a = ap.parse_args()

    pairs = json.loads(Path(a.blocks).read_text(encoding="utf-8"))["pairs"]
    spec = next(p for p in pairs if p["pair_id"] == a.pair)[a.side]
    res = json.loads(Path(a.result).read_text(encoding="utf-8"))

    doc = fitz.open(spec["pdf"])
    page = doc[spec["page"]]
    pr = page.rect
    bn = spec["bbox_norm"]
    rect = fitz.Rect(bn[0] * pr.width, bn[1] * pr.height, bn[2] * pr.width, bn[3] * pr.height)
    if a.sub:
        f = [float(v) for v in a.sub.split(",")]
        w, h = rect.width, rect.height
        rect = fitz.Rect(rect.x0 + f[0] * w, rect.y0 + f[1] * h, rect.x0 + f[2] * w, rect.y0 + f[3] * h)
    zoom = a.dpi / 72.0
    pm = page.get_pixmap(clip=rect, dpi=a.dpi)
    img = Image.open(io.BytesIO(pm.tobytes("png"))).convert("RGB")
    draw = ImageDraw.Draw(img)
    rot = page.rotation_matrix          # data (mediabox) -> visual

    def to_px(pt):
        p = fitz.Point(pt[0], pt[1]) * rot
        return ((p.x - rect.x0) * zoom, (p.y - rect.y0) * zoom)

    n = {"ok": 0, "bad": 0, "none": 0}
    for d in res["dimensions"]:
        cx, cy = to_px(d["center"])
        if not d.get("detected"):
            draw.ellipse([cx - 4, cy - 4, cx + 4, cy + 4], outline=(220, 170, 0), width=2)
            n["none"] += 1
            continue
        ax, ay = to_px(d["foot_a"])
        bx, by = to_px(d["foot_b"])
        col = (0, 160, 0) if d.get("scale_ok") else (220, 0, 0)
        n["ok" if d.get("scale_ok") else "bad"] += 1
        draw.line([ax, ay, bx, by], fill=col, width=3)
        for (px, py) in ((ax, ay), (bx, by)):
            draw.ellipse([px - 3, py - 3, px + 3, py + 3], fill=col)
        draw.line([cx, cy, (ax + bx) / 2, (ay + by) / 2], fill=col, width=1)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    img.save(a.out)
    print(a.out, img.size, n)


if __name__ == "__main__":
    main()
