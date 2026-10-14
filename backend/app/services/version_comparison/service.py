"""Служебные зоны на уровне полей (план §5 шаг 6).

Основная область сравнивается наложением; здесь — что именно поменялось в
служебных зонах, списком «было → стало»:

* штамп: обозначение, название, номер листа, последнее «Изм.», строки таблицы
  изменений (добавлена / удалена / изменена);
* отметка выпуска: дата и остальной текст;
* QR: содержимое (``cv2.QRCodeDetector``), если не читается — отпечаток
  растра кода;
* подписи: изображения в зоне штампа (по хэшу).

Ничего не скрывается и не трактуется: исчезновение отметки или QR — тоже
запись. Согласование по этим данным не подтверждается.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any

import cv2
import fitz
import numpy as np

from . import pdfio

DATE = re.compile(r"\b\d{2}\.\d{2}\.\d{2,4}\b")
QR_DPI = 200
STAMP_FIELDS = (("designation", "Обозначение"), ("title", "Название листа"), ("sheet_no", "Лист"),
                ("izm_max", "Последнее «Изм.»"))


def _norm(text: str) -> str:
    return " ".join(str(text or "").split())


def _qr_crops(gray: np.ndarray) -> list[np.ndarray]:
    """Квадратные плотные пятна — кандидаты в QR (когда детектор не нашёл код в большой зоне)."""
    ink = (gray < 128).astype(np.uint8)
    closed = cv2.morphologyEx(ink, cv2.MORPH_CLOSE, np.ones((9, 9), np.uint8))
    n, _, st, _ = cv2.connectedComponentsWithStats(closed)
    out = []
    for i in range(1, n):
        x, y, w, h, _a = (int(v) for v in st[i])
        if 40 <= w <= 800 and 0.8 <= w / max(1, h) <= 1.25 and ink[y:y + h, x:x + w].mean() >= 0.3:
            out.append(cv2.copyMakeBorder(gray[y:y + h, x:x + w], 20, 20, 20, 20, cv2.BORDER_CONSTANT, value=255))
    return out


def read_qr(page: fitz.Page, bbox: list[float]) -> dict[str, Any] | None:
    """QR в области листа (отображаемая система, pt): содержимое или отпечаток. None — кода нет."""
    clip = fitz.Rect(bbox) & page.rect
    if clip.is_empty:
        return None
    pm = page.get_pixmap(matrix=fitz.Matrix(QR_DPI / 72, QR_DPI / 72), clip=clip, colorspace=fitz.csGRAY,
                         annots=False)
    gray = np.frombuffer(pm.samples, np.uint8).reshape(pm.h, pm.w).copy()
    det = cv2.QRCodeDetector()
    try:
        ok, texts, _pts, _ = det.detectAndDecodeMulti(gray)
    except cv2.error:
        ok, texts = False, ()
    found = sorted(t for t in (texts or ()) if t) if ok else []
    crops = _qr_crops(gray)
    if not found:
        # Классический детектор читает не все коды; детектор на ArUco — запасной.
        detectors = [det] + ([cv2.QRCodeDetectorAruco()] if hasattr(cv2, "QRCodeDetectorAruco") else [])
        for crop in crops:
            for d in detectors:
                try:
                    text = d.detectAndDecode(crop)[0]
                except cv2.error:
                    text = ""
                if text:
                    found.append(text)
                    break
    if found:
        return {"decoded": True, "content": sorted(set(found))}
    if not crops:
        return None
    # Код есть, но не читается: отпечаток сетки модулей (уменьшение + порог).
    small = cv2.resize(crops[0], (41, 41), interpolation=cv2.INTER_AREA) < 128
    return {"decoded": False, "content": [], "fingerprint": hashlib.sha256(bytes(np.packbits(small))).hexdigest()[:16]}


def _izm_rows(rec: dict[str, Any]) -> dict[int, str]:
    return {int(r["izm"]): _norm(r.get("text")) for r in rec.get("izm_rows") or [] if r.get("izm") is not None}


def _zone_bbox(rec: dict[str, Any], kind: str) -> list[float] | None:
    boxes = [z["bbox"] for z in rec.get("zones") or [] if z["type"] == kind]
    if not boxes:
        return None
    return [min(b[0] for b in boxes), min(b[1] for b in boxes), max(b[2] for b in boxes), max(b[3] for b in boxes)]


QR_SEARCH_PT = (250.0, 100.0)   # насколько расширить зону отметки при поиске QR (по x, по y)


def _around(box: list[float] | None) -> list[float] | None:
    if not box:
        return None
    dx, dy = QR_SEARCH_PT
    return [box[0] - dx, box[1] - dy, box[2] + dx, box[3] + dy]


def _stamp_images(doc: fitz.Document, page: fitz.Page, stamp: list[float] | None) -> list[str]:
    if not stamp:
        return []
    out = []
    for img in pdfio.image_records(doc, page):
        b = img["bbox"]
        cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
        if stamp[0] <= cx <= stamp[2] and stamp[1] <= cy <= stamp[3] and img["digest"]:
            out.append(img["digest"])
    return sorted(out)


def release_text(rec: dict[str, Any], separated: list[dict[str, Any]], zone_words: list[str]) -> str:
    texts = [_norm(s.get("text")) for s in separated if s.get("kind") == "release_stamp"]
    return " | ".join(t for t in texts if t) or _norm(" ".join(zone_words))


def compare(rec_a: dict[str, Any], rec_b: dict[str, Any], doc_a: fitz.Document, page_a: fitz.Page,
            doc_b: fitz.Document, page_b: fitz.Page, release_a: str, release_b: str) -> list[dict[str, Any]]:
    """Поля служебных зон: только изменившиеся, списком «было → стало»."""
    out: list[dict[str, Any]] = []
    sa, sb = rec_a.get("stamp") or {}, rec_b.get("stamp") or {}
    for field, label in STAMP_FIELDS:
        va, vb = sa.get(field), sb.get(field)
        if _norm(va) != _norm(vb):
            out.append({"zone": "stamp", "field": field, "label": label, "change": "changed",
                        "before": "" if va is None else str(va), "after": "" if vb is None else str(vb)})
    ra, rb = _izm_rows(rec_a), _izm_rows(rec_b)
    for izm in sorted(set(ra) | set(rb), reverse=True):
        if izm not in ra:
            out.append({"zone": "stamp", "field": "izm_row", "label": f"Таблица изменений: Изм. {izm}",
                        "change": "added", "before": "", "after": rb[izm]})
        elif izm not in rb:
            out.append({"zone": "stamp", "field": "izm_row", "label": f"Таблица изменений: Изм. {izm}",
                        "change": "removed", "before": ra[izm], "after": ""})
        elif ra[izm].replace(" ", "") != rb[izm].replace(" ", ""):
            out.append({"zone": "stamp", "field": "izm_row", "label": f"Таблица изменений: Изм. {izm}",
                        "change": "changed", "before": ra[izm], "after": rb[izm]})

    # Отметка выпуска: дата отдельно, остальной текст — если изменился.
    if release_a or release_b:
        da, db = DATE.findall(release_a), DATE.findall(release_b)
        if da != db:
            out.append({"zone": "release_stamp", "field": "date", "label": "Отметка выпуска: дата",
                        "change": "changed" if (da and db) else ("added" if db else "removed"),
                        "before": ", ".join(da), "after": ", ".join(db)})
        rest_a, rest_b = _norm(DATE.sub("", release_a)), _norm(DATE.sub("", release_b))
        if rest_a != rest_b:
            out.append({"zone": "release_stamp", "field": "text", "label": "Отметка выпуска: текст",
                        "change": "changed" if (rest_a and rest_b) else ("added" if rest_b else "removed"),
                        "before": rest_a, "after": rest_b})

    # QR — рядом с отметкой выпуска (часто правее её рамки, векторный), иначе у штампа.
    qa_box = _around(_zone_bbox(rec_a, "release_stamp") or _zone_bbox(rec_a, "stamp"))
    qb_box = _around(_zone_bbox(rec_b, "release_stamp") or _zone_bbox(rec_b, "stamp"))
    qa = read_qr(page_a, qa_box) if qa_box else None
    qb = read_qr(page_b, qb_box) if qb_box else None
    if qa or qb:
        def show(q: dict | None) -> str:
            if not q:
                return ""
            return ", ".join(q["content"]) if q["decoded"] else f"(не читается, отпечаток {q['fingerprint']})"
        key_a = (qa or {}).get("content") or (qa or {}).get("fingerprint")
        key_b = (qb or {}).get("content") or (qb or {}).get("fingerprint")
        if key_a != key_b:
            out.append({"zone": "qr", "field": "qr", "label": "QR-код",
                        "change": "changed" if (qa and qb) else ("added" if qb else "removed"),
                        "before": show(qa), "after": show(qb)})

    ia = _stamp_images(doc_a, page_a, _zone_bbox(rec_a, "stamp"))
    ib = _stamp_images(doc_b, page_b, _zone_bbox(rec_b, "stamp"))
    if ia != ib:
        out.append({"zone": "signatures", "field": "images", "label": "Подписи (изображения в штампе)",
                    "change": "changed" if (ia and ib) else ("added" if ib else "removed"),
                    "before": f"{len(ia)} шт." if ia else "", "after": f"{len(ib)} шт." if ib else ""})
    return out
