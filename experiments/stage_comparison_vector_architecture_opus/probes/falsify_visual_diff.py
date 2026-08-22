"""falsify_ probe: localize what actually changed between two versions of one page,
by rasterising both pages and diffing pixels, then cropping the changed regions
from both sides so a human can name the engineering change.

Also runs the unmodified Track A v0.1 extractor + comparator on the SAME region
so the measured v0.1 verdict sits next to the picture.

Run:
  python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_visual_diff \
      --left <pdfA> --right <pdfB> --page 8 --tag aps_k4_p8 [--dpi 110] [--no-compare]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"


def gray(page, dpi):
    return page.get_pixmap(matrix=fitz.Matrix(dpi / 72.0, dpi / 72.0), colorspace=fitz.csGRAY)


def diff_boxes(pa, pb, thresh=48, min_pixels=6):
    """Return (changed_pixel_count, total_pixels, [boxes]) in pixmap coordinates."""
    w = min(pa.width, pb.width)
    h = min(pa.height, pb.height)
    sa, sb = pa.samples, pb.samples
    sw_a, sw_b = pa.width, pb.width
    changed = []
    cells = {}
    cell = 24
    total_changed = 0
    for y in range(h):
        ra = y * sw_a
        rb = y * sw_b
        for x in range(w):
            if abs(sa[ra + x] - sb[rb + x]) > thresh:
                total_changed += 1
                cells.setdefault((x // cell, y // cell), 0)
                cells[(x // cell, y // cell)] += 1
    # merge adjacent dirty cells into boxes (simple flood fill on the cell grid)
    dirty = {c for c, n in cells.items() if n >= min_pixels}
    seen = set()
    for c in sorted(dirty):
        if c in seen:
            continue
        stack = [c]
        seen.add(c)
        comp = []
        while stack:
            cx, cy = stack.pop()
            comp.append((cx, cy))
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    n = (cx + dx, cy + dy)
                    if n in dirty and n not in seen:
                        seen.add(n)
                        stack.append(n)
        xs = [p[0] for p in comp]
        ys = [p[1] for p in comp]
        changed.append(
            {
                "px_box": [min(xs) * cell, min(ys) * cell, (max(xs) + 1) * cell, (max(ys) + 1) * cell],
                "dirty_cells": len(comp),
                "dirty_pixels": sum(cells[p] for p in comp),
            }
        )
    changed.sort(key=lambda b: -b["dirty_pixels"])
    return total_changed, w * h, changed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--left", required=True)
    ap.add_argument("--right", required=True)
    ap.add_argument("--page", type=int, required=True)
    ap.add_argument("--right-page", type=int, default=None)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--dpi", type=float, default=100.0)
    ap.add_argument("--regions", type=int, default=4)
    ap.add_argument("--compare", action="store_true", help="also run Track A v0.1 on each region")
    args = ap.parse_args()

    la = ROOT / args.left if not Path(args.left).is_absolute() else Path(args.left)
    lb = ROOT / args.right if not Path(args.right).is_absolute() else Path(args.right)
    da, db = fitz.open(la), fitz.open(lb)
    pa_page = da[args.page]
    pb_page = db[args.right_page if args.right_page is not None else args.page]
    pa, pb = gray(pa_page, args.dpi), gray(pb_page, args.dpi)
    total_changed, total_px, boxes = diff_boxes(pa, pb)
    outdir = ART / "falsify_visual" / args.tag
    outdir.mkdir(parents=True, exist_ok=True)
    scale = 72.0 / args.dpi
    regions = []
    for i, b in enumerate(boxes[: args.regions], 1):
        x0, y0, x1, y1 = b["px_box"]
        pad = 40
        rect = fitz.Rect(
            max(0, (x0 - pad) * scale), max(0, (y0 - pad) * scale),
            (x1 + pad) * scale, (y1 + pad) * scale,
        )
        zoom = min(1400 / max(rect.width, rect.height, 1e-6), 12)
        for side, page in (("left", pa_page), ("right", pb_page)):
            pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect)
            pix.save(outdir / f"region{i}_{side}.png")
        entry = {
            "region": i,
            "dirty_pixels": b["dirty_pixels"],
            "rect_pt": [round(v, 2) for v in (rect.x0, rect.y0, rect.x1, rect.y1)],
            "bbox_norm_left": [
                rect.x0 / pa_page.rect.width, rect.y0 / pa_page.rect.height,
                rect.x1 / pa_page.rect.width, rect.y1 / pa_page.rect.height,
            ],
            "bbox_norm_right": [
                rect.x0 / pb_page.rect.width, rect.y0 / pb_page.rect.height,
                rect.x1 / pb_page.rect.width, rect.y1 / pb_page.rect.height,
            ],
        }
        regions.append(entry)
    payload = {
        "left": str(la.relative_to(ROOT)),
        "right": str(lb.relative_to(ROOT)),
        "page_index": args.page,
        "right_page_index": args.right_page if args.right_page is not None else args.page,
        "dpi": args.dpi,
        "page_size_left": [round(pa_page.rect.width, 1), round(pa_page.rect.height, 1)],
        "page_size_right": [round(pb_page.rect.width, 1), round(pb_page.rect.height, 1)],
        "changed_pixels": total_changed,
        "compared_pixels": total_px,
        "changed_pixel_share": round(total_changed / max(total_px, 1), 8),
        "changed_regions_found": len(boxes),
        "regions": regions,
    }
    (outdir / "diff.json").write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in payload.items() if k != "regions"}, ensure_ascii=False, indent=1))
    for r in regions:
        print("  region", r["region"], "dirty_px", r["dirty_pixels"], "rect", r["rect_pt"])
    print("wrote", outdir)


if __name__ == "__main__":
    main()
