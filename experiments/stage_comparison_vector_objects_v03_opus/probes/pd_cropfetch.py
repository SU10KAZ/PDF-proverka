# -*- coding: utf-8 -*-
"""pd_cropfetch — доступ к подготовленному блоку через ОБЛАЧНЫЙ вектор-кроп (crop_url → *.pdf).

Нужен для объекта 213, где 02_work/document.pdf отсутствует у ВСЕХ 96 версий,
поэтому v03_foundation.render_block/extract_block неприменимы (нет исходного PDF).
Кроп — одностраничный вектор-PDF ровно того региона, что и подготовленный блок;
проверка соответствия — сверка аспекта crop-PDF с аспектом coords_px (см. pd_crop_fidelity.json).
"""
from __future__ import annotations
import hashlib, json, os, sys, urllib.request
from pathlib import Path
import fitz

BASE = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[3]
CACHE = BASE / "artifacts" / "pd_crops" / "_cache"


def fetch(url: str, timeout: int = 60) -> Path | None:
    CACHE.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha1(url.encode()).hexdigest()[:16]
    ext = ".pdf" if url.lower().endswith(".pdf") else ".png"
    p = CACHE / (h + ext)
    if p.exists() and p.stat().st_size > 0:
        return p
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "curl/8.5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
        if not data:
            return None
        p.write_bytes(data)
        return p
    except Exception as e:
        print("FETCH-ERR", url[-40:], e, file=sys.stderr)
        return None


def crop_doc(url):
    p = fetch(url)
    if p is None or p.suffix != ".pdf":
        return None
    try:
        return fitz.open(p)
    except Exception:
        return None


def render_crop(url, out_png, target_px=1500):
    d = crop_doc(url)
    if d is None:
        return None
    page = d[0]
    rs = target_px / max(page.rect.width, page.rect.height)
    rs = max(0.3, min(8.0, rs))
    pix = page.get_pixmap(matrix=fitz.Matrix(rs, rs), alpha=False)
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    pix.save(str(out_png))
    return out_png


def blocks_of(rj, graphic_only=True):
    d = json.load(open(rj, encoding="utf-8"))
    out = []
    for pg in d.get("pages") or []:
        for b in pg.get("blocks") or []:
            if graphic_only and b.get("block_type") != "image":
                continue
            sd = b.get("stamp_data") or b.get("ocr_json")
            if isinstance(sd, str):
                try:
                    sd = json.loads(sd)
                except Exception:
                    sd = None
            out.append({
                "block_id": b.get("id"), "page_number": pg.get("page_number"),
                "coords_px": b.get("coords_px"), "page_px": [pg.get("width"), pg.get("height")],
                "crop_url": b.get("crop_url"), "category_code": b.get("category_code"),
                "shape_type": b.get("shape_type"),
                "stamp": sd if isinstance(sd, dict) else None,
                "ocr_text": b.get("ocr_text") or "",
            })
    return out
