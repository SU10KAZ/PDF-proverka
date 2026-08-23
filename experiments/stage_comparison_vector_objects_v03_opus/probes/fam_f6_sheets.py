# -*- coding: utf-8 -*-
"""F6 — render family contact sheets for EYE labelling (hatch vs symbol vs rectangle).

One cell per family: the family's members drawn in red on top of the block's ink
(grey), plus a zoomed representative in the corner.  The cell caption carries only
an index — the features must not be visible to the labeller, otherwise the labels
are not independent of the rule they are used to test (v0.2 P11/P12 has the same
caveat and it is the reason its labels are "medium" confidence).
Usage: fam_f6_sheets.py [n_blocks]
"""
from __future__ import annotations
import json, random, sys, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fitz
import grp_common as G
import fam_family as FAM
import fam_f5_scope as F5

SEED = 20260823
OUT = G.ART / "fam_sheets"
CELL = 300
COLS, ROWS = 6, 4


def render_family(ex, objs, fam, size=CELL):
    """Grey block + this family's members in red; returns a fitz.Pixmap."""
    xs = [p for s in ex.segments for p in (s["p0"][0], s["p1"][0])]
    ys = [p for s in ex.segments for p in (s["p0"][1], s["p1"][1])]
    x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
    w, h = max(x1 - x0, 1e-6), max(y1 - y0, 1e-6)
    rs = size / max(w, h)
    doc = fitz.open()
    page = doc.new_page(width=w * rs, height=h * rs)
    mem = set(fam["members"])
    sel = set()
    for m in mem:
        sel.update(objs[m]["segments"])
    sh = page.new_shape()
    for k, s in enumerate(ex.segments):
        if k in sel:
            continue
        sh.draw_line(fitz.Point((s["p0"][0] - x0) * rs, (s["p0"][1] - y0) * rs),
                     fitz.Point((s["p1"][0] - x0) * rs, (s["p1"][1] - y0) * rs))
    sh.finish(color=(0.72, 0.72, 0.72), width=0.4)
    sh.commit()
    sh = page.new_shape()
    for k in sel:
        s = ex.segments[k]
        sh.draw_line(fitz.Point((s["p0"][0] - x0) * rs, (s["p0"][1] - y0) * rs),
                     fitz.Point((s["p1"][0] - x0) * rs, (s["p1"][1] - y0) * rs))
    sh.finish(color=(0.85, 0.0, 0.0), width=1.1)
    sh.commit()
    return page.get_pixmap(alpha=False)


def render_member(ex, obj, size=110):
    bb = obj["bbox"]
    pad = max(0.12 * max(bb[2] - bb[0], bb[3] - bb[1]), 0.5)
    x0, y0, x1, y1 = bb[0] - pad, bb[1] - pad, bb[2] + pad, bb[3] + pad
    w, h = max(x1 - x0, 1e-6), max(y1 - y0, 1e-6)
    rs = size / max(w, h)
    doc = fitz.open()
    page = doc.new_page(width=max(w * rs, 8), height=max(h * rs, 8))
    sh = page.new_shape()
    for k in obj["segments"]:
        s = ex.segments[k]
        sh.draw_line(fitz.Point((s["p0"][0] - x0) * rs, (s["p0"][1] - y0) * rs),
                     fitz.Point((s["p1"][0] - x0) * rs, (s["p1"][1] - y0) * rs))
    sh.finish(color=(0, 0, 0), width=1.2)
    sh.commit()
    return page.get_pixmap(alpha=False)


def main():
    from PIL import Image, ImageDraw
    n_blocks = int(sys.argv[1]) if len(sys.argv) > 1 else 34
    OUT.mkdir(parents=True, exist_ok=True)
    import glob
    scope = []
    for fn in sorted(glob.glob(str(G.ART / "fam_f5_scope_s*.jsonl"))):
        for line in open(fn, encoding="utf-8"):
            if line.strip():
                scope.append(json.loads(line))
    ok = [r for r in scope if r.get("verdict") in ("usable", "background_dominated")
          and r.get("n_obj", 0) >= 8 and r.get("families")]
    rng = random.Random(SEED)
    rng.shuffle(ok)
    # stratify: at most 3 blocks per discipline
    per: dict[str, int] = {}
    chosen = []
    for r in ok:
        d = r["discipline"]
        if per.get(d, 0) >= 3:
            continue
        per[d] = per.get(d, 0) + 1
        chosen.append(r)
        if len(chosen) >= n_blocks:
            break
    cells = []
    for r in chosen:
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            continue
        try:
            ex = G.extract(pb)
        except Exception:
            print("ERR", r["block_id"], traceback.format_exc().splitlines()[-1]); continue
        L = G.layer_of(ex.segments, ex.texts)
        F = FAM.build_families(L)
        block_ink = sum(o["seg_len"] for o in L.objects) or 1.0
        order = sorted(range(len(F.families)), key=lambda i: -len(F.families[i]["members"]))
        # 3 families per block: the biggest, a middle one, a small repeated one
        picks = [i for i in order if len(F.families[i]["members"]) >= 2]
        if not picks:
            continue
        take = [picks[0]]
        if len(picks) > 3:
            take.append(picks[len(picks) // 3])
            take.append(picks[min(len(picks) - 1, 2 * len(picks) // 3)])
        for fi in take:
            f = F.families[fi]
            feats = F5.fam_features(f, L.objects, L.S, block_ink)
            pix = render_family(ex, L.objects, f)
            rep = f["members"][0]
            pmem = render_member(ex, L.objects[rep])
            cells.append({"block_id": r["block_id"], "discipline": r["discipline"],
                          "cls": r["cls"], "fam_index": fi, "feats": feats,
                          "pix": pix, "mem": pmem})
        print(r["block_id"][:12], r["discipline"], len(take), flush=True)
    # montage
    meta = []
    for page_i in range(0, len(cells), COLS * ROWS):
        chunk = cells[page_i:page_i + COLS * ROWS]
        W, H = COLS * (CELL + 8), ROWS * (CELL + 26)
        img = Image.new("RGB", (W, H), "white")
        dr = ImageDraw.Draw(img)
        for k, c in enumerate(chunk):
            cx = (k % COLS) * (CELL + 8)
            cy = (k // COLS) * (CELL + 26)
            p = c["pix"]
            im = Image.frombytes("RGB", (p.width, p.height), p.samples)
            img.paste(im, (cx + (CELL - p.width) // 2, cy + 22 + (CELL - p.height) // 2))
            m = c["mem"]
            imm = Image.frombytes("RGB", (m.width, m.height), m.samples)
            dr.rectangle([cx + 2, cy + 22, cx + 2 + m.width + 1, cy + 22 + m.height + 1],
                         outline=(0, 0, 200))
            img.paste(imm, (cx + 3, cy + 23))
            gid = page_i + k
            dr.text((cx + 4, cy + 5), f"#{gid}  n={c['feats']['n']}", fill=(0, 0, 0))
            dr.rectangle([cx, cy, cx + CELL + 6, cy + CELL + 24], outline=(180, 180, 180))
            meta.append({"cell": gid, "block_id": c["block_id"],
                         "discipline": c["discipline"], "cls": c["cls"],
                         "fam_index": c["fam_index"], "feats": c["feats"]})
        img.save(OUT / f"sheet_{page_i // (COLS * ROWS):02d}.png")
        print("sheet", page_i // (COLS * ROWS), len(chunk), flush=True)
    json.dump({"seed": SEED, "cells": meta},
              open(G.ART / "fam_f6_cells.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
