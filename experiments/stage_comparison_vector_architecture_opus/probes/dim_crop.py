#!/usr/bin/env python3
"""dim_* probe: render a diagnostic crop of a block in the VISUAL page space.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_crop \
        --blocks <blocks.json> --pair <id> --side left --out <png> [--dpi 150] [--sub x0,y0,x1,y1]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import fitz


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--blocks", default="experiments/stage_comparison_vector_architecture_opus/probes/dim_blocks.json")
    ap.add_argument("--pair", required=True)
    ap.add_argument("--side", default="left")
    ap.add_argument("--out", required=True)
    ap.add_argument("--dpi", type=int, default=150)
    ap.add_argument("--sub", help="sub-rect inside the block, fractions x0,y0,x1,y1")
    a = ap.parse_args()
    pairs = json.loads(Path(a.blocks).read_text(encoding="utf-8"))["pairs"]
    spec = next(p for p in pairs if p["pair_id"] == a.pair)[a.side]
    doc = fitz.open(spec["pdf"])
    page = doc[spec["page"]]
    r = page.rect
    bn = spec["bbox_norm"]
    rect = fitz.Rect(bn[0] * r.width, bn[1] * r.height, bn[2] * r.width, bn[3] * r.height)
    if a.sub:
        f = [float(v) for v in a.sub.split(",")]
        w, h = rect.width, rect.height
        rect = fitz.Rect(rect.x0 + f[0] * w, rect.y0 + f[1] * h, rect.x0 + f[2] * w, rect.y0 + f[3] * h)
    pm = page.get_pixmap(clip=rect, dpi=a.dpi)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    pm.save(a.out)
    print(a.out, pm.width, pm.height, "rect", [round(v, 1) for v in rect])


if __name__ == "__main__":
    main()
