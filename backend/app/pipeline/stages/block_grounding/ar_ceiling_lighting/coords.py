"""Каноническое координатное пространство профиля.

Проблема: PyMuPDF ``get_texttrace``/``get_drawings`` отдают координаты,
сдвинутые на origin CropBox, а pdfplumber и ``get_text("words")`` после
сброса CropBox — сырое пространство MediaBox. Канонизация: CropBox
сбрасывается в MediaBox (в памяти, файл не меняется), после чего ВСЕ
API работают в одном top-left-пространстве MediaBox. Исходный CropBox
запоминается как границы видимого графического блока (block_scope).

Самопроверка обязательна и fail-closed: расхождение систем координат
или ненулевой rotation → :class:`CanonicalSpaceError`, не тихая каша.
"""
from __future__ import annotations

import fitz


class CanonicalSpaceError(RuntimeError):
    """Координатное пространство не удалось привести к каноническому виду."""


class CanonicalPage:
    """Страница в каноническом пространстве MediaBox (y вниз).

    Атрибуты:
      page        — fitz.Page с уже сброшенным CropBox;
      block_rect  — исходный CropBox = граница видимого блока (кортеж x0,y0,x1,y1);
      media_rect  — MediaBox (кортеж);
      self_check  — отчёт самопроверки (для metrics.json).
    """

    def __init__(self, doc: fitz.Document, page: fitz.Page, block_rect, self_check: dict):
        self.doc = doc
        self.page = page
        self.block_rect = tuple(round(v, 3) for v in block_rect)
        self.media_rect = tuple(round(v, 3) for v in page.rect)
        self.self_check = self_check


def span_text(span: dict) -> str:
    return "".join(chr(char[0]) for char in span["chars"])


def open_canonical(pdf_path: str, page_index: int = 0) -> CanonicalPage:
    doc = fitz.open(pdf_path)
    if page_index >= len(doc):
        raise CanonicalSpaceError(f"страницы {page_index} нет в PDF ({len(doc)} стр.)")
    page = doc[page_index]
    if page.rotation:
        # Профиль калиброван на rotation=0; поворот требует отдельной
        # нормализации drawings vs text — честный отказ вместо мусора.
        raise CanonicalSpaceError(f"page.rotation={page.rotation}: профиль требует rotation=0")

    media = fitz.Rect(page.mediabox)
    crop = fitz.Rect(page.cropbox)  # уже в top-left пространстве MediaBox
    block_rect = (crop.x0, crop.y0, crop.x1, crop.y1)
    page.set_cropbox(page.mediabox)

    self_check = _verify_alignment(page)
    self_check["mediabox"] = [round(v, 2) for v in media]
    self_check["block_rect"] = [round(v, 2) for v in block_rect]
    return CanonicalPage(doc, page, block_rect, self_check)


def _verify_alignment(page: fitz.Page) -> dict:
    """Сверка texttrace ↔ words (fitz) ↔ pdfplumber на общих словах.

    Берём до 8 «якорных» слов (длина ≥ 5, только цифры/точки/буквы), для
    каждого ищем совпадающий по тексту объект в других источниках и
    сравниваем bbox. Допуск по x — 1.0 pt (метрики шрифта по y могут
    отличаться, поэтому по y допуск шире).
    """
    report: dict = {"checked_words": 0, "max_dx": 0.0, "max_dy": 0.0, "pdfplumber": "not_checked"}
    spans = page.get_texttrace()
    by_text: dict[str, list] = {}
    for span in spans:
        text = span_text(span).strip()
        if len(text) >= 5 and not text.isspace():
            by_text.setdefault(text, []).append(span["bbox"])

    words = [w for w in page.get_text("words") if len(str(w[4]).strip()) >= 5]
    checked = 0
    for word in words:
        boxes = by_text.get(str(word[4]).strip())
        if not boxes:
            continue
        best = min(boxes, key=lambda b: abs(b[0] - word[0]) + abs(b[1] - word[1]))
        dx = abs(best[0] - word[0])
        dy = abs(best[1] - word[1])
        if dx > 1.0:
            continue  # слово могло склеиться из нескольких спанов — не якорь
        report["max_dx"] = max(report["max_dx"], round(dx, 3))
        report["max_dy"] = max(report["max_dy"], round(dy, 3))
        checked += 1
        if checked >= 8:
            break
    report["checked_words"] = checked
    if checked == 0 and words:
        raise CanonicalSpaceError("самопроверка координат: ни одно якорное слово не совпало "
                                  "между texttrace и get_text('words')")
    if report["max_dy"] > 6.0:
        raise CanonicalSpaceError(f"самопроверка координат: расхождение по y {report['max_dy']} pt")
    return report


def verify_pdfplumber(cp: CanonicalPage, pdf_path: str, page_index: int = 0) -> dict:
    """Опциональная сверка с pdfplumber (если пакет установлен).

    pdfplumber работает в сыром MediaBox-пространстве — после канонизации
    его слова должны совпадать с fitz-словами почти точно.
    """
    try:
        import pdfplumber  # noqa: WPS433 — опциональная зависимость
    except ImportError:
        cp.self_check["pdfplumber"] = "not_installed"
        return cp.self_check
    with pdfplumber.open(pdf_path) as pdf:
        ppage = pdf.pages[page_index]
        pwords = ppage.extract_words()
    fitz_words = {}
    for w in cp.page.get_text("words"):
        fitz_words.setdefault(str(w[4]).strip(), []).append(w[:4])
    matched = 0
    total = 0
    max_dx = 0.0
    for word in pwords:
        boxes = fitz_words.get(word["text"].strip())
        if not boxes:
            continue
        total += 1
        best = min(boxes, key=lambda b: abs(b[0] - word["x0"]) + abs(b[1] - word["top"]))
        dx = abs(best[0] - word["x0"])
        if dx <= 1.0:
            matched += 1
            max_dx = max(max_dx, round(dx, 3))
    cp.self_check["pdfplumber"] = {
        "words_total": len(pwords),
        "joined_by_text": total,
        "bbox_agree": matched,
        "max_dx": max_dx,
    }
    if total >= 10 and matched < total * 0.9:
        raise CanonicalSpaceError("pdfplumber и fitz разошлись по координатам слов")
    return cp.self_check
