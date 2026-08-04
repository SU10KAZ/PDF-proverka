"""Каноническое координатное пространство профиля.

Проблема: PyMuPDF ``get_texttrace``/``get_drawings`` отдают координаты,
сдвинутые на origin CropBox, а pdfplumber и ``get_text("words")`` после
сброса CropBox — сырое пространство MediaBox. Канонизация: CropBox
сбрасывается в MediaBox (в памяти, файл не меняется), после чего ВСЕ
API работают в одном top-left-пространстве MediaBox. Исходный CropBox
запоминается как границы видимого графического блока (block_scope).

Две НЕЗАВИСИМЫЕ проверки (не смешивать):

* ``coordinate_alignment`` — совпадение геометрии bbox между
  источниками. Совпадения ТЕКСТА не требует: пары строятся по ближайшим
  bbox. Провал → ``CanonicalSpaceError`` (fail-closed).
* ``text_decoding_agreement`` — читаемость и совпадение символов между
  источниками (качество ToUnicode). Провал НЕ ломает координаты:
  состояние ``text_decoding_partial``/``text_decoding_unusable`` уходит
  в предупреждения, извлечение геометрии продолжается.

Ненулевой rotation профиль пока не поддерживает — типизированный отказ
``RotationUnsupported`` (статус no_graph, не error).
"""
from __future__ import annotations

import fitz


class CanonicalSpaceError(RuntimeError):
    """Координатное пространство не удалось привести к каноническому виду."""


class RotationUnsupported(CanonicalSpaceError):
    """page.rotation != 0: профиль пока не поддерживает поворот."""


class CanonicalPage:
    """Страница в каноническом пространстве MediaBox (y вниз).

    Атрибуты:
      page        — fitz.Page с уже сброшенным CropBox;
      block_rect  — исходный CropBox = граница видимого блока (кортеж x0,y0,x1,y1);
      media_rect  — MediaBox (кортеж);
      crop_equals_media — у листа не было «скрытой» области за кропом;
      self_check  — отчёт самопроверки (для metrics.json);
      text_decoding — состояние text_decoding_agreement;
      warnings    — предупреждения канонизации (не фатальные).
    """

    def __init__(self, doc: fitz.Document, page: fitz.Page, block_rect, self_check: dict):
        self.doc = doc
        self.page = page
        self.block_rect = tuple(round(v, 3) for v in block_rect)
        self.media_rect = tuple(round(v, 3) for v in page.rect)
        self.crop_equals_media = (
            abs(self.block_rect[0] - self.media_rect[0]) < 0.5
            and abs(self.block_rect[1] - self.media_rect[1]) < 0.5
            and abs(self.block_rect[2] - self.media_rect[2]) < 0.5
            and abs(self.block_rect[3] - self.media_rect[3]) < 0.5
        )
        self.self_check = self_check
        self.text_decoding = self_check.get("text_decoding", {}).get("state", "text_decoding_valid")
        self.warnings: list[str] = list(self_check.get("warnings") or [])


def span_text(span: dict) -> str:
    return "".join(chr(char[0]) for char in span["chars"])


def open_canonical(pdf_path: str, page_index: int = 0) -> CanonicalPage:
    doc = fitz.open(pdf_path)
    if page_index >= len(doc):
        raise CanonicalSpaceError(f"страницы {page_index} нет в PDF ({len(doc)} стр.)")
    page = doc[page_index]
    if page.rotation:
        # Поведение get_text и get_drawings при повороте расходится —
        # честный типизированный отказ вместо мусора.
        raise RotationUnsupported(f"page.rotation={page.rotation}: профиль требует rotation=0")

    media = fitz.Rect(page.mediabox)
    crop = fitz.Rect(page.cropbox)  # уже в top-left пространстве MediaBox
    block_rect = (crop.x0, crop.y0, crop.x1, crop.y1)
    page.set_cropbox(page.mediabox)

    self_check = _verify_page(page)
    self_check["mediabox"] = [round(v, 2) for v in media]
    self_check["block_rect"] = [round(v, 2) for v in block_rect]
    return CanonicalPage(doc, page, block_rect, self_check)


def _pair_by_geometry(anchor_boxes: list, other_boxes: list, *, tol: float = 1.2) -> tuple[int, float, float]:
    """Пары bbox ↔ bbox по ближайшему левому верхнему углу (текст не нужен)."""
    paired = 0
    max_dx = max_dy = 0.0
    if not other_boxes:
        return 0, 0.0, 0.0
    for ab in anchor_boxes:
        best = min(other_boxes, key=lambda b: abs(b[0] - ab[0]) + abs(b[1] - ab[1]))
        dx = abs(best[0] - ab[0])
        dy = abs(best[1] - ab[1])
        if dx <= tol and dy <= 6.0:
            paired += 1
            max_dx = max(max_dx, round(dx, 3))
            max_dy = max(max_dy, round(dy, 3))
    return paired, max_dx, max_dy


def _verify_page(page: fitz.Page) -> dict:
    """coordinate_alignment + text_decoding_agreement между texttrace и words.

    coordinate_alignment: fail-closed (исключение). Пары строятся сначала
    по совпадающему тексту, а при провале текста — по чистой геометрии
    ближайших bbox, чтобы различие кодировки не выглядело ошибкой
    координат.
    """
    report: dict = {
        "coordinate_alignment": {"state": "canonical_space_invalid", "checked": 0,
                                 "max_dx": 0.0, "max_dy": 0.0, "mode": None},
        "text_decoding": {"state": "text_decoding_valid", "checked": 0, "agree": 0,
                          "unreadable_spans": 0},
        "warnings": [],
        "pdfplumber": "not_checked",
    }
    spans = page.get_texttrace()
    span_items = []
    unreadable = 0
    for span in spans:
        text = span_text(span).strip()
        if not text:
            continue
        # CID-мусор / провал ToUnicode: управляющие, replacement char, PUA
        bad = sum(1 for ch in text if ch == "�" or 0xE000 <= ord(ch) <= 0xF8FF or ord(ch) < 9)
        if bad > len(text) * 0.5:
            unreadable += 1
        span_items.append((text, span["bbox"]))
    report["text_decoding"]["unreadable_spans"] = unreadable

    words = [(str(w[4]).strip(), w[:4]) for w in page.get_text("words") if str(w[4]).strip()]

    # --- text_decoding_agreement: совпадение символов на якорных словах ---
    by_text: dict[str, list] = {}
    for text, bbox in span_items:
        if len(text) >= 5:
            by_text.setdefault(text, []).append(bbox)
    agree = checked = 0
    coord_pairs: list[tuple] = []  # (span_bbox, word_bbox) от текстовых пар
    for text, wb in words:
        if len(text) < 5:
            continue
        checked += 1
        boxes = by_text.get(text)
        if boxes:
            best = min(boxes, key=lambda b: abs(b[0] - wb[0]) + abs(b[1] - wb[1]))
            if abs(best[0] - wb[0]) <= 1.0:
                agree += 1
                coord_pairs.append((best, wb))
        if checked >= 60:
            break
    td = report["text_decoding"]
    td["checked"] = checked
    td["agree"] = agree
    if checked == 0:
        td["state"] = "text_decoding_unusable" if unreadable else "text_decoding_partial"
    elif agree >= max(3, checked * 0.6):
        td["state"] = "text_decoding_valid"
    elif agree > 0:
        td["state"] = "text_decoding_partial"
    else:
        td["state"] = "text_decoding_unusable"
    if td["state"] != "text_decoding_valid":
        report["warnings"].append(
            f"TEXT_DECODING_{'PARTIAL' if td['state'] == 'text_decoding_partial' else 'UNUSABLE'}: "
            f"совпало {agree} из {checked} якорных слов; текстовые связи понижаются, "
            "геометрия извлекается полностью")

    # --- coordinate_alignment ---
    ca = report["coordinate_alignment"]
    if coord_pairs:
        ca["mode"] = "text_anchored"
        ca["checked"] = len(coord_pairs[:8])
        for sb, wb in coord_pairs[:8]:
            ca["max_dx"] = max(ca["max_dx"], round(abs(sb[0] - wb[0]), 3))
            ca["max_dy"] = max(ca["max_dy"], round(abs(sb[1] - wb[1]), 3))
        ca["state"] = "coordinates_valid" if ca["max_dy"] <= 6.0 else "canonical_space_invalid"
    else:
        # кодировки разошлись — координаты проверяем чистой геометрией
        anchor = [bbox for _, bbox in span_items[:40]]
        paired, max_dx, max_dy = _pair_by_geometry(anchor, [wb for _, wb in words])
        ca["mode"] = "geometry_only"
        ca["checked"] = paired
        ca["max_dx"] = max_dx
        ca["max_dy"] = max_dy
        if not span_items or not words:
            # текст-слоя (или words-представления) нет — координатному
            # пространству нечего ломать, геометрия извлекается дальше
            ca["state"] = "coordinates_valid"
            ca["mode"] = "no_text_layer"
        else:
            need = min(len(anchor), len(words))
            if paired >= min(need, max(2, need // 2)):
                ca["state"] = "coordinates_valid"
            else:
                ca["state"] = "canonical_space_invalid"
    if ca["state"] != "coordinates_valid":
        raise CanonicalSpaceError(
            f"coordinate_alignment: bbox texttrace и words не совпали "
            f"(mode={ca['mode']}, пар {ca['checked']}, dy {ca['max_dy']})")

    # совместимость со старым форматом metrics.self_check
    report["checked_words"] = ca["checked"]
    report["max_dx"] = ca["max_dx"]
    report["max_dy"] = ca["max_dy"]
    return report


def verify_pdfplumber(cp: CanonicalPage, pdf_path: str, page_index: int = 0) -> dict:
    """Сверка с pdfplumber (если установлен): координаты отдельно от текста.

    Совпадение текста слов — вопрос кодировки (text_decoding), а не
    координат: при расхождении текста пары строятся по геометрии.
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
    fitz_boxes = []
    for w in cp.page.get_text("words"):
        fitz_words.setdefault(str(w[4]).strip(), []).append(w[:4])
        fitz_boxes.append(w[:4])
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
    result = {
        "words_total": len(pwords),
        "joined_by_text": total,
        "bbox_agree": matched,
        "max_dx": max_dx,
        "state": "coordinates_valid",
    }
    if total >= 10 and matched < total * 0.9:
        # текстовые пары разошлись — прежде чем винить координаты,
        # проверяем чистую геометрию (кодировка ≠ координаты)
        anchor = [(w["x0"], w["top"], w["x1"], w["bottom"]) for w in pwords[:40]]
        paired, gdx, gdy = _pair_by_geometry(anchor, fitz_boxes)
        result["geometry_pairs"] = paired
        if paired >= max(3, len(anchor) * 0.5):
            result["state"] = "coordinates_valid"
            result["note"] = "текст разошёлся (кодировка), геометрия совпала"
            cp.warnings.append("PDFPLUMBER_TEXT_DECODING_DIFFERS: пары по геометрии, "
                               f"{paired} совпавших bbox")
        else:
            result["state"] = "canonical_space_invalid"
            cp.self_check["pdfplumber"] = result
            raise CanonicalSpaceError("pdfplumber и fitz разошлись по координатам слов "
                                      f"(гео-пар {paired})")
    cp.self_check["pdfplumber"] = result
    return cp.self_check
