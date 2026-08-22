#!/usr/bin/env python3
"""dim_* probe: cache flattened vector geometry + text spans for one block.

Absolute PDF coordinates are kept (unlike Track A's normalized space) because the
scale cross-check needs real page units.  page.get_drawings() is called ONCE per
page and the result is cached on disk.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_cache \
        --pdf <path> --page 13 --bbox-norm 0.41,0.38,0.84,0.67 --out <cache.json>
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Sequence

import fitz

CURVE_STEPS = 6


def _sample_cubic(item, steps: int = CURVE_STEPS):
    p0, p1, p2, p3 = (float(item[1].x), float(item[1].y)), (float(item[2].x), float(item[2].y)), \
        (float(item[3].x), float(item[3].y)), (float(item[4].x), float(item[4].y))
    pts = []
    for i in range(steps + 1):
        t = i / steps
        mt = 1 - t
        x = mt**3 * p0[0] + 3 * mt**2 * t * p1[0] + 3 * mt * t**2 * p2[0] + t**3 * p3[0]
        y = mt**3 * p0[1] + 3 * mt**2 * t * p1[1] + 3 * mt * t**2 * p2[1] + t**3 * p3[1]
        pts.append((x, y))
    return pts


def _rect_intersects(a: Sequence[float], b: Sequence[float]) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def flatten_page(page: fitz.Page, block_rect: Sequence[float]) -> dict[str, Any]:
    """Return per-path records: segments (abs coords), fill flag, bbox, area."""
    drawings = page.get_drawings()
    paths: list[dict[str, Any]] = []
    for di, d in enumerate(drawings):
        r = d.get("rect")
        if r is None:
            continue
        rb = [r.x0, r.y0, r.x1, r.y1]
        if not _rect_intersects(rb, block_rect):
            continue
        segs: list[list[float]] = []
        subpaths: list[list[tuple[float, float]]] = []
        cur: list[tuple[float, float]] = []
        for item in d.get("items") or []:
            op = item[0]
            if op == "l":
                a = (float(item[1].x), float(item[1].y))
                b = (float(item[2].x), float(item[2].y))
                if cur and cur[-1] == a:
                    cur.append(b)
                else:
                    if len(cur) > 1:
                        subpaths.append(cur)
                    cur = [a, b]
            elif op == "c":
                pts = _sample_cubic(item)
                if cur and abs(cur[-1][0] - pts[0][0]) < 1e-9 and abs(cur[-1][1] - pts[0][1]) < 1e-9:
                    cur.extend(pts[1:])
                else:
                    if len(cur) > 1:
                        subpaths.append(cur)
                    cur = list(pts)
            elif op == "re":
                rr = item[1]
                pts = [(rr.x0, rr.y0), (rr.x1, rr.y0), (rr.x1, rr.y1), (rr.x0, rr.y1), (rr.x0, rr.y0)]
                if len(cur) > 1:
                    subpaths.append(cur)
                cur = []
                subpaths.append([(float(x), float(y)) for x, y in pts])
            elif op == "qu":
                q = item[1]
                pts = [(q.ul.x, q.ul.y), (q.ur.x, q.ur.y), (q.lr.x, q.lr.y), (q.ll.x, q.ll.y), (q.ul.x, q.ul.y)]
                if len(cur) > 1:
                    subpaths.append(cur)
                cur = []
                subpaths.append([(float(x), float(y)) for x, y in pts])
        if len(cur) > 1:
            subpaths.append(cur)
        for sp in subpaths:
            for i in range(len(sp) - 1):
                x0, y0 = sp[i]
                x1, y1 = sp[i + 1]
                if math.hypot(x1 - x0, y1 - y0) < 1e-9:
                    continue
                segs.append([round(x0, 4), round(y0, 4), round(x1, 4), round(y1, 4)])
        if not segs:
            continue
        fill = d.get("fill")
        paths.append(
            {
                "i": di,
                "rect": [round(v, 4) for v in rb],
                "filled": bool(d.get("fill") is not None and d.get("type") in ("f", "fs")),
                "fill": list(fill) if fill else None,
                "stroke_color": list(d.get("color")) if d.get("color") else None,
                "width": float(d.get("width") or 0.0),
                "type": d.get("type"),
                "n_subpaths": len(subpaths),
                "segs": segs,
            }
        )
    return {"paths": paths, "page_drawings_total": len(drawings)}


def extract_text(page: fitz.Page, block_rect: Sequence[float]) -> list[dict[str, Any]]:
    out = []
    data = page.get_text("dict", clip=fitz.Rect(*block_rect))
    for block in data.get("blocks") or []:
        if block.get("type") != 0:
            continue
        for line in block.get("lines") or []:
            d = line.get("dir") or (1.0, 0.0)
            rot = math.degrees(math.atan2(float(d[1]), float(d[0])))
            for span in line.get("spans") or []:
                text = re.sub(r"\s+", " ", str(span.get("text") or "")).strip()
                if not text:
                    continue
                bbox = [round(float(v), 4) for v in span.get("bbox")]
                out.append(
                    {
                        "id": f"t{len(out)+1}",
                        "text": text,
                        "bbox": bbox,
                        "dir": [round(float(d[0]), 6), round(float(d[1]), 6)],
                        "rotation": round(rot, 3),
                        "size": round(float(span.get("size") or 0.0), 3),
                        "font": span.get("font") or "",
                    }
                )
    return out


def build(pdf: str, page_index: int, bbox_norm: Sequence[float], out: str) -> dict[str, Any]:
    doc = fitz.open(pdf)
    page = doc[page_index]
    r = page.rect
    # bbox_norm is expressed in the VISUAL (rotation-aware) page space, the same
    # space get_pixmap(clip=...) uses.  get_drawings()/get_text(clip=...) return
    # MEDIABOX coordinates, so the rect must be de-rotated before clipping.
    visual = fitz.Rect(
        r.x0 + bbox_norm[0] * r.width,
        r.y0 + bbox_norm[1] * r.height,
        r.x0 + bbox_norm[2] * r.width,
        r.y0 + bbox_norm[3] * r.height,
    )
    data_rect = visual * page.derotation_matrix
    br = [data_rect.x0, data_rect.y0, data_rect.x1, data_rect.y1]
    geom = flatten_page(page, br)
    texts = extract_text(page, br)
    payload = {
        "pdf": pdf,
        "page_index": page_index,
        "page_rect": [r.x0, r.y0, r.x1, r.y1],
        "bbox_norm": list(bbox_norm),
        "block_rect": [round(v, 4) for v in br],
        "block_rect_visual": [round(v, 4) for v in (visual.x0, visual.y0, visual.x1, visual.y1)],
        "page_rotation": page.rotation,
        "paths": geom["paths"],
        "page_drawings_total": geom["page_drawings_total"],
        "texts": texts,
    }
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(json.dumps(payload), encoding="utf-8")
    doc.close()
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--page", type=int, required=True)
    ap.add_argument("--bbox-norm", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    bn = [float(v) for v in a.bbox_norm.split(",")]
    p = build(a.pdf, a.page, bn, a.out)
    nseg = sum(len(x["segs"]) for x in p["paths"])
    print(f"paths={len(p['paths'])} segments={nseg} texts={len(p['texts'])} -> {a.out}")


if __name__ == "__main__":
    main()
