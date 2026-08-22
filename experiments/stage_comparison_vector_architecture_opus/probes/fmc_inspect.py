#!/usr/bin/env python3
"""FMC helper — inspect one page pair: text diff, drawing stats, optional crop render.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_inspect \
        --left <pdf> --li <page> --right <pdf> --ri <page> [--bbox x0 y0 x1 y1] [--png out_prefix] [--dpi 90] [--drawings]
"""
from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
_NUM = re.compile(r"-?\d+(?:[.,]\d+)?")
_WORD = re.compile(r"[^\W\d_]{2,}", re.UNICODE)


def _p(rel: str) -> Path:
    path = Path(rel)
    return path if path.is_absolute() else ROOT / rel


def spans(page, bbox=None):
    out = []
    clip = fitz.Rect(bbox) if bbox else None
    d = page.get_text("dict", clip=clip)
    for b in d["blocks"]:
        for l in b.get("lines", []):
            for s in l.get("spans", []):
                out.append(s)
    return out


def draw_stats(page, bbox=None):
    drawings = page.get_drawings()
    rect = fitz.Rect(bbox) if bbox else page.rect
    items = collections.Counter()
    prims = 0
    for d in drawings:
        r = d["rect"]
        if not r.intersects(rect):
            continue
        prims += 1
        for it in d["items"]:
            items[it[0]] += 1
    return {"paths_in_bbox": prims, "paths_total": len(drawings), "items": dict(items)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--left", required=True)
    ap.add_argument("--li", type=int, required=True)
    ap.add_argument("--right")
    ap.add_argument("--ri", type=int)
    ap.add_argument("--bbox", nargs=4, type=float, default=None, help="normalized x0 y0 x1 y1")
    ap.add_argument("--rbbox", nargs=4, type=float, default=None)
    ap.add_argument("--png")
    ap.add_argument("--dpi", type=int, default=90)
    ap.add_argument("--drawings", action="store_true")
    ap.add_argument("--dump-text", action="store_true")
    a = ap.parse_args()

    dl = fitz.open(_p(a.left))
    pl = dl[a.li]
    lb = None
    if a.bbox:
        lb = [a.bbox[0] * pl.rect.width, a.bbox[1] * pl.rect.height, a.bbox[2] * pl.rect.width, a.bbox[3] * pl.rect.height]
    ls = spans(pl, lb)
    lt = " ".join(s["text"] for s in ls)
    print(f"LEFT  {a.left} p{a.li} rect={[round(v,1) for v in pl.rect]} spans={len(ls)} chars={len(lt)} images={len(pl.get_images(full=True))}")
    print("  fonts:", sorted({s['font'] for s in ls}))
    if a.drawings:
        print("  draw:", draw_stats(pl, lb))
    if a.dump_text:
        print("  TEXT:", lt[:3000])

    if a.right is not None:
        dr = fitz.open(_p(a.right))
        pr = dr[a.ri]
        rb = None
        bb = a.rbbox or a.bbox
        if bb:
            rb = [bb[0] * pr.rect.width, bb[1] * pr.rect.height, bb[2] * pr.rect.width, bb[3] * pr.rect.height]
        rs = spans(pr, rb)
        rt = " ".join(s["text"] for s in rs)
        print(f"RIGHT {a.right} p{a.ri} rect={[round(v,1) for v in pr.rect]} spans={len(rs)} chars={len(rt)} images={len(pr.get_images(full=True))}")
        print("  fonts:", sorted({s['font'] for s in rs}))
        if a.drawings:
            print("  draw:", draw_stats(pr, rb))
        if a.dump_text:
            print("  TEXT:", rt[:3000])
        cl = collections.Counter(s["text"].strip() for s in ls)
        cr = collections.Counter(s["text"].strip() for s in rs)
        print("  ONLY-LEFT :", sorted((cl - cr).items(), key=lambda x: -x[1])[:40])
        print("  ONLY-RIGHT:", sorted((cr - cl).items(), key=lambda x: -x[1])[:40])
        nl = collections.Counter(_NUM.findall(lt))
        nr = collections.Counter(_NUM.findall(rt))
        print("  NUM only-left :", sorted((nl - nr).items(), key=lambda x: -x[1])[:30])
        print("  NUM only-right:", sorted((nr - nl).items(), key=lambda x: -x[1])[:30])

    if a.png:
        out = Path(a.png)
        out.parent.mkdir(parents=True, exist_ok=True)
        m = fitz.Matrix(a.dpi / 72, a.dpi / 72)
        pl.get_pixmap(matrix=m, clip=fitz.Rect(lb) if lb else None).save(str(out) + "_left.png")
        if a.right is not None:
            pr.get_pixmap(matrix=m, clip=fitz.Rect(rb) if rb else None).save(str(out) + "_right.png")
        print("  wrote", out)


if __name__ == "__main__":
    main()
