#!/usr/bin/env python3
"""FMC probe step 3 — richer per-page profiles (fonts, ToUnicode, images, glyph coverage).

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_page_profile

Writes artifacts/fmc_page_profile.json for every PDF taking part in a non-identical version step.
"""
from __future__ import annotations

import json
import multiprocessing as mp
import re
import sys
import time
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"
OUT = ART / "fmc_page_profile.json"

_CYR = re.compile(r"[А-Яа-яЁё]")
_LAT = re.compile(r"[A-Za-z]")
_DIG = re.compile(r"[0-9]")
_PRINTABLE_ODD = re.compile(r"[-�]")


def _profile(rel: str) -> dict:
    doc = fitz.open(ROOT / rel)
    pages = []
    for i in range(doc.page_count):
        page = doc[i]
        text = page.get_text("text")
        fonts = page.get_fonts(full=True)
        # xref -> has ToUnicode?
        no_tounicode = 0
        for f in fonts:
            xref = f[0]
            try:
                tu = doc.xref_get_key(xref, "ToUnicode")
            except Exception:
                tu = ("null", "null")
            if not tu or tu[0] == "null":
                no_tounicode += 1
        imgs = page.get_images(full=True)
        img_area = 0.0
        for im in imgs:
            try:
                for r in page.get_image_rects(im[0]):
                    img_area += r.get_area()
            except Exception:
                pass
        pages.append(
            {
                "n_chars": len(text),
                "n_cyr": len(_CYR.findall(text)),
                "n_lat": len(_LAT.findall(text)),
                "n_dig": len(_DIG.findall(text)),
                "n_odd": len(_PRINTABLE_ODD.findall(text)),
                "n_fonts": len(fonts),
                "fonts_no_tounicode": no_tounicode,
                "font_names": sorted({f[3] for f in fonts}),
                "n_images": len(imgs),
                "image_area_frac": round(img_area / max(page.rect.get_area(), 1.0), 4),
                "page_area": round(page.rect.get_area(), 1),
                "rect": [round(v, 1) for v in page.rect],
            }
        )
    doc.close()
    return {"pdf": rel, "pages": pages}


def main() -> None:
    from .fmc_io import read_json
    cand = read_json(ART / "fmc_candidates.json")
    rels = sorted({st[s]["pdf"] for st in cand for s in ("left", "right")})
    print(f"pdfs={len(rels)}", file=sys.stderr)
    t0 = time.perf_counter()
    with mp.Pool(8) as pool:
        res = {r["pdf"]: r["pages"] for r in pool.map(_profile, rels)}
    OUT.write_text(json.dumps(res, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {OUT} in {time.perf_counter()-t0:.1f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
