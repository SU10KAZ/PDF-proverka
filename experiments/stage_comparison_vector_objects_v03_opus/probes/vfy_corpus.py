# -*- coding: utf-8 -*-
"""VERIFY probe: independent corpus index. Does NOT import v03_foundation.

Reads result.json by hand (own minimal parser) and records PDF page geometry facts
that the fnd probe never recorded: MediaBox, CropBox, page.rect origin, /Rotate.
Read-only over projects_v2/.
"""
from __future__ import annotations
import glob, json, os, sys, time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import fitz

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments/stage_comparison_vector_objects_v03_opus/artifacts"
PAT = str(ROOT / "projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/result.json")


def j4(v):
    if isinstance(v, str):
        try: v = json.loads(v)
        except Exception: return None
    if isinstance(v, (list, tuple)) and len(v) >= 4:
        try: return [float(v[0]), float(v[1]), float(v[2]), float(v[3])]
        except Exception: return None
    return None


def scan(rj: str) -> dict:
    p = Path(rj)
    pdf = p.parent / "document.pdf"
    parts = p.parts
    try:
        i = parts.index("objects")
        obj, disc, doc, ver = parts[i+1], parts[i+3], parts[i+5], parts[i+7]
    except Exception:
        obj = disc = doc = ver = "?"
    rec = {"result_json": rj, "pdf": str(pdf), "pdf_exists": pdf.exists(),
           "obj": obj, "disc": disc, "doc": doc, "ver": ver,
           "n_pages_rj": 0, "n_img_blocks": 0, "n_blocks_all": 0,
           "pages": [], "err": None}
    try:
        data = json.load(open(rj, encoding="utf-8"))
    except Exception as e:
        rec["err"] = f"json:{type(e).__name__}"; return rec
    pageinfo = []
    if rec["pdf_exists"]:
        try:
            d = fitz.open(str(pdf))
            for k in range(d.page_count):
                pg = d[k]
                mb = pg.mediabox; cb = pg.cropbox; r = pg.rect
                pageinfo.append({
                    "rect": [r.x0, r.y0, r.x1, r.y1],
                    "mb": [mb.x0, mb.y0, mb.x1, mb.y1],
                    "cb": [cb.x0, cb.y0, cb.x1, cb.y1],
                    "rot": int(pg.rotation) % 360,
                    "cbpos": [pg.cropbox_position.x, pg.cropbox_position.y],
                })
            d.close()
        except Exception as e:
            rec["err"] = f"pdf:{type(e).__name__}"
    rec["n_pdf_pages"] = len(pageinfo)
    for page in data.get("pages") or []:
        rec["n_pages_rj"] += 1
        try: pn = int(page.get("page_number"))
        except Exception: pn = None
        w = int(page.get("width") or 0); h = int(page.get("height") or 0)
        rr = page.get("rotation")
        rot_rj = None
        if rr not in (None, ""):
            try: rot_rj = int(rr) % 360
            except Exception: rot_rj = None
        blocks = page.get("blocks") or []
        rec["n_blocks_all"] += len(blocks)
        img = [b for b in blocks if str(b.get("block_type") or "") == "image"]
        rec["n_img_blocks"] += len(img)
        pi_vals = []
        for b in img:
            try: pi_vals.append(int(b.get("page_index")))
            except Exception: pi_vals.append(None)
        pe = None
        if pn is not None and 0 <= pn - 1 < len(pageinfo):
            pe = pageinfo[pn - 1]
        prec = {
            "pn": pn, "w": w, "h": h, "rot_rj": rot_rj, "n_img": len(img),
            "pi_field": sorted({v for v in pi_vals if v is not None}),
            "pdf_rot": pe["rot"] if pe else None,
            "rect": pe["rect"] if pe else None,
            "mb": pe["mb"] if pe else None,
            "cb": pe["cb"] if pe else None,
            "cbpos": pe["cbpos"] if pe else None,
            "coords": [j4(b.get("coords_px")) for b in img],
            "shape": [str(b.get("shape_type") or "") for b in img],
            "bid": [str(b.get("id") or "") for b in img],
        }
        rec["pages"].append(prec)
    return rec


def main():
    files = sorted(glob.glob(PAT))
    t0 = time.time()
    out = []
    with ProcessPoolExecutor(max_workers=12) as ex:
        for r in ex.map(scan, files, chunksize=4):
            out.append(r)
    ART.mkdir(parents=True, exist_ok=True)
    with open(ART / "vfy_corpus.jsonl", "w", encoding="utf-8") as fh:
        for r in out:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    n_img = sum(r["n_img_blocks"] for r in out)
    print(f"files={len(files)} img_blocks={n_img} t={time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
