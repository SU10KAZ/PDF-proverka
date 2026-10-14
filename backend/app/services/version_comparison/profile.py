"""Профиль версии: что лежит на каждой странице PDF (план §5 шаг 1).

Считается один раз на файл и кэшируется по ``sha256 + PROFILE_VERSION``:
``profiles/<sha>/<algo>/pages.json`` + слова по страницам + маски 36 dpi
(признак для сопоставления листов, НЕ подтверждение совпадения).
"""
from __future__ import annotations

import gzip
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import cv2
import fitz
import numpy as np

from backend.app.services.common.atomic_json import atomic_write_json

from . import paths, pdfio, stamp
from .settings import DARK_THRESHOLD, PROFILE_VERSION

MASK_DPI = 36
THUMB_WIDTH = 220
# Скан: картинка не меньше этой доли листа и не больше стольких векторных контуров.
SCAN_IMAGE_SHARE = 0.6
SCAN_MAX_DRAWINGS = 20


def _segments(page: fitz.Page) -> list[tuple[float, float, float, float]]:
    """Длинные отрезки страницы (кандидаты в линии рамки), повёрнутая система."""
    out = []
    try:
        drawings = page.get_drawings()
    except Exception:
        return out
    m = page.rotation_matrix
    min_len = 0.35 * min(page.mediabox.width, page.mediabox.height)
    for d in drawings:
        for item in d.get("items", []):
            if item[0] == "l":
                a, b = item[1], item[2]
                if abs(a.x - b.x) + abs(a.y - b.y) >= min_len:
                    out.append(tuple(pdfio.to_display(page, (a.x, a.y, b.x, b.y), m)))
            elif item[0] == "re":
                r = item[1]
                if max(r.width, r.height) < min_len:
                    continue
                for x0, y0, x1, y1 in ((r.x0, r.y0, r.x1, r.y0), (r.x0, r.y1, r.x1, r.y1),
                                       (r.x0, r.y0, r.x0, r.y1), (r.x1, r.y0, r.x1, r.y1)):
                    out.append(tuple(pdfio.to_display(page, (x0, y0, x1, y1), m)))
    return out


def _ink_mask(page: fitz.Page, dpi: int) -> np.ndarray:
    gray, _, _ = pdfio.render(page, fitz.Matrix(dpi / 72, dpi / 72), gray=True)
    return (gray < DARK_THRESHOLD).astype(np.uint8)


def _qr_like_components(mask: np.ndarray, dpi: int, near: list[float]) -> list[list[float]]:
    """Квадратные пятна графики 8–40 мм рядом с отметкой выпуска (QR)."""
    k = dpi / 72.0
    mm = dpi / 25.4
    dil = cv2.dilate(mask, np.ones((3, 3), np.uint8))
    n, _, stats, _ = cv2.connectedComponentsWithStats(dil, connectivity=8)
    x0, y0, x1, y1 = near
    out = []
    for i in range(1, n):
        x, y, w, h, area = (int(v) for v in stats[i])
        if not (8 * mm <= w <= 40 * mm and 8 * mm <= h <= 40 * mm and 0.7 <= w / max(h, 1) <= 1.4):
            continue
        if area < 0.25 * w * h:
            continue
        bx0, by0, bx1, by1 = x / k, y / k, (x + w) / k, (y + h) / k
        v_overlap = min(by1, y1) - max(by0, y0)
        h_gap = max(bx0 - x1, x0 - bx1)
        if v_overlap > 0.3 * (by1 - by0) and h_gap <= 30 * 72 / 25.4:
            out.append([bx0, by0, bx1, by1])
    return out


def _permit_izm(words: list[list[Any]], page_h: float) -> list[int]:
    """Номера «Изм.» в первой колонке таблицы страницы «Разрешение»."""
    heads = [w for w in words if w[4].strip().lower() in {"изм.", "изм"} and w[1] < page_h * 0.5]
    out = []
    for head in heads:
        cx = (head[0] + head[2]) / 2
        for w in words:
            if (w[1] > head[3] and abs((w[0] + w[2]) / 2 - cx) < 4 * stamp.MM
                    and re.fullmatch(r"\d{1,2}", w[4].strip()) and w[1] < page_h * 0.85):
                out.append(int(w[4]))
    return out


def _page_profile(orig: fitz.Document, stripped: fitz.Document, pno: int, merkle) -> tuple[dict, list, np.ndarray]:
    page = orig[pno]
    spage = stripped[pno]
    W, H = float(page.rect.width), float(page.rect.height)
    annots = pdfio.annotation_records(page)
    words = pdfio.page_words(spage)
    shx_words = [a["rect"] + [a["content"]] for a in annots if a["role"] == "shx_text" and a["content"].strip()]
    frame = stamp.detect_frame(W, H, _segments(page))
    st = stamp.parse(words, W, H, frame=frame)
    kind = stamp.classify_kind(words, st, W, H)
    blocks = [pdfio.to_display(spage, b[:4]) + [b[4]] for b in spage.get_text("blocks") if len(b) > 4]
    releases = stamp.release_blocks(blocks)
    mask = _ink_mask(page, MASK_DPI)
    zones: list[dict[str, Any]] = []
    if st.get("zone"):
        zones.append({"type": "stamp", "bbox": st["zone"], "source": st["form"]})
    for rel in releases:
        bbox = rel["bbox"]
        qr = _qr_like_components(mask, MASK_DPI, bbox)
        zones.append({"type": "release_stamp", "bbox": stamp._union([bbox] + qr), "source": "text+qr" if qr else "text",
                      "date": rel["date"], "text": rel["text"][:200]})
    if frame:
        fx0, fy0, fx1, fy1 = frame
        for rect in ([0, 0, fx0, H], [fx1, 0, W, H], [fx0, 0, fx1, fy0], [fx0, fy1, fx1, H]):
            if rect[2] - rect[0] > 0.5 and rect[3] - rect[1] > 0.5:
                zones.append({"type": "margin", "bbox": rect, "source": "frame"})
    images = pdfio.image_records(orig, page)
    markup = [a for a in annots if a["role"] == "markup"]
    izm_candidates = [v for v in [st.get("izm_max")] if v is not None]
    if kind == "permit":
        izm_candidates += _permit_izm(words, H)
    rec = {
        "page": pno + 1,
        "width": W, "height": H, "rotation": int(page.rotation),
        "kind": kind,
        "frame": frame,
        "stamp": {k: st[k] for k in ("recognized", "form", "zone", "designation", "sheet_no", "title", "izm_max")},
        "izm_rows": st.get("izm_rows", []),
        "izm_candidates": izm_candidates,
        "zones": zones,
        "annots": annots,
        "images": images,
        "words_count": len(words),
        "shx_words_count": len(shx_words),
        "content_hash": pdfio.content_hash(orig, page, merkle),
        "text_hash": pdfio.text_hash(words),
        "annots_hash": pdfio.annots_hash(markup),
        "ink_share": round(float(mask.mean()), 5),
        "scan": _is_scan(page, images),
    }
    all_words = [w + ["text"] for w in words] + [w + ["shx"] for w in shx_words]
    return rec, all_words, mask


def _is_scan(page: fitz.Page, images: list[dict[str, Any]]) -> bool:
    """Страница-скан: картинка на большей части листа, видимого текста нет,
    векторной графики почти нет.

    Полупрозрачное предупреждение во весь лист тоже картинка, но под ним
    обычный чертёж с текстом — это не скан. Невидимый текст распознавания
    (OCR-слой скана) видимым не считается.
    """
    if not any(im["area_share"] >= SCAN_IMAGE_SHARE for im in images):
        return False
    try:
        visible = any(span.get("type") != 3 and span.get("chars") for span in page.get_texttrace())
    except Exception:
        visible = bool(page.get_text("text").strip())
    return not visible and len(page.get_drawings()) <= SCAN_MAX_DRAWINGS


def build(object_id: str, sha: str, *, force: bool = False) -> dict[str, Any]:
    out_dir = paths.profile_dir(object_id, sha)
    pages_json = out_dir / "pages.json"
    if pages_json.exists() and not force:
        return json.loads(pages_json.read_text(encoding="utf-8"))
    data = pdfio.read_bytes(paths.source_pdf(object_id, sha))
    orig = pdfio.open_memory(data)
    stripped = pdfio.open_memory(data)
    pdfio.strip_annotations(stripped)
    merkle = pdfio._Merkle(orig)
    (out_dir / "words").mkdir(parents=True, exist_ok=True)
    (out_dir / "mask36").mkdir(parents=True, exist_ok=True)
    pages = []
    for pno in range(len(orig)):
        rec, words, mask = _page_profile(orig, stripped, pno, merkle)
        pages.append(rec)
        with gzip.open(out_dir / "words" / f"p{pno + 1:04d}.json.gz", "wt", encoding="utf-8") as fh:
            json.dump(words, fh, ensure_ascii=False)
        cv2.imwrite(str(out_dir / "mask36" / f"p{pno + 1:04d}.png"), mask * 255)
    designations = Counter(p["stamp"]["designation"] for p in pages if p["stamp"]["designation"])
    # Обозначение документа без суффикса раздела («….СО», «….ОД»): по нему группируются версии.
    bases = Counter(re.sub(r"\.[A-ZА-ЯЁ]{1,4}$", "", d, flags=re.I) for d in designations.elements())
    izm = [v for p in pages for v in p["izm_candidates"]]
    release_dates = sorted({z["date"] for p in pages for z in p["zones"] if z["type"] == "release_stamp" and z.get("date")},
                           key=lambda d: (d[6:], d[3:5], d[:2]))
    profile = {
        "schema": "version_comparison.profile/v1",
        "algo": PROFILE_VERSION,
        "sha256": sha,
        "page_count": len(pages),
        "summary": {
            "designation": designations.most_common(1)[0][0] if designations else "",
            "designations": dict(designations),
            "base_designation": bases.most_common(1)[0][0] if bases else "",
            "izm_max": max(izm) if izm else None,
            "release_dates": release_dates,
            "kinds": dict(Counter(p["kind"] for p in pages)),
        },
        "pages": pages,
    }
    atomic_write_json(pages_json, profile)
    return profile


def load(object_id: str, sha: str) -> dict[str, Any] | None:
    path = paths.profile_dir(object_id, sha) / "pages.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def page_words(object_id: str, sha: str, page: int) -> list[list[Any]]:
    path = paths.profile_dir(object_id, sha) / "words" / f"p{page:04d}.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def page_mask36(object_id: str, sha: str, page: int) -> np.ndarray | None:
    path = paths.profile_dir(object_id, sha) / "mask36" / f"p{page:04d}.png"
    img = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
    return None if img is None else (img > 0).astype(np.uint8)


def thumbnail(object_id: str, sha: str, page: int, width: int = THUMB_WIDTH) -> Path:
    """PNG-миниатюра страницы (без аннотаций), дисковый кэш."""
    width = max(64, min(800, int(width)))
    out = paths.thumbs_dir(object_id, sha) / f"p{page:04d}_w{width}.png"
    if out.exists():
        return out
    doc = pdfio.open_memory(pdfio.read_bytes(paths.source_pdf(object_id, sha)))
    pg = doc[page - 1]
    scale = width / max(1.0, pg.rect.width)
    rgb, _, _ = pdfio.render(pg, fitz.Matrix(scale, scale))
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp.png")
    cv2.imwrite(str(tmp), cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
    tmp.replace(out)
    return out
