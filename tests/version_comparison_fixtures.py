"""Синтетические PDF для тестов «Сравнения версий» (план §10.1).

Лист A3 с рамкой и штампом по ГОСТ, надписями и линиями. Варианты:
изменение цифры и буквы, линии, точки, толщины, цвета, светлой заливки;
сдвиг, поворот, масштаб, атрибут /Rotate; наложенный последним потоком
штамп «В производство работ» с QR; полупрозрачное предупреждение-картинка;
предупреждение, вшитое в основной поток; аннотации; текст кривыми.
"""
from __future__ import annotations

import io
import random
from pathlib import Path
from typing import Any

import cv2
import fitz
from PIL import Image, ImageDraw

MM = 72.0 / 25.4
W, H = 420 * MM, 297 * MM

BASE_CONTENT: dict[str, Any] = {
    "values": {"pp": "Рр=323.55кВт", "ip": "Iр=538.17 А", "cable": "ППГнг(A)-HF 5х95", "small": "1.5",
               "label": "ЩР1"},
    "extra_line": False, "remove_line": False, "dot": False, "thin_line": True, "tick": False,
    "line_width": 0.5, "line_color": (0, 0, 0), "light_fill": True, "light_fill_shift": 0.0,
    "baked_warning": False, "underlay": None, "fill_line": False, "crossing": None, "table": None,
    "cloud": False, "ticks": False, "squares": False,
}


def _unique_content(page: fitz.Page, seed: int, words: list[str]) -> None:
    """Содержимое, по которому листы заметно отличаются друг от друга."""
    rnd = random.Random(seed)
    sh = page.new_shape()
    for _ in range(14):
        x0, y0 = rnd.uniform(40, 330) * MM, rnd.uniform(20, 200) * MM
        sh.draw_line((x0, y0), (x0 + rnd.uniform(-60, 60) * MM, y0 + rnd.uniform(-40, 40) * MM))
    for _ in range(4):
        x0, y0 = rnd.uniform(40, 300) * MM, rnd.uniform(20, 180) * MM
        sh.draw_rect(fitz.Rect(x0, y0, x0 + rnd.uniform(10, 50) * MM, y0 + rnd.uniform(10, 40) * MM))
    sh.finish(color=(0, 0, 0), width=0.7)
    sh.commit()
    for k, word in enumerate(words):
        page.insert_text((45 * MM + (k % 5) * 60 * MM, 25 * MM + (k // 5) * 9 * MM), word, fontname="F1", fontsize=9)


SPLIT_WORDS = (("ВРУ-1", "Ввод1", "QF1", "QF2", "QF3", "ЩР-1.1", "ЩР-1.2", "Рр=12кВт", "Iр=21А"),
               ("ВРУ-2", "Ввод2", "QF4", "QF5", "QF6", "ЩР-2.1", "ЩР-2.2", "Рр=15кВт", "Iр=26А"))


def _split_block(page: fitz.Page, k: int, x0: float) -> None:
    """Половина схемы: шина, отходящие линии и подписи (для разделения листа)."""
    sh = page.new_shape()
    sh.draw_line((x0, 60 * MM), (x0 + 130 * MM, 60 * MM))
    for j in range(5):
        x = x0 + 10 * MM + j * 25 * MM
        sh.draw_line((x, 60 * MM), (x, 150 * MM))
        sh.draw_rect(fitz.Rect(x - 4 * MM, 90 * MM, x + 4 * MM, 100 * MM))
    sh.finish(color=(0, 0, 0), width=0.7)
    sh.commit()
    for j, word in enumerate(SPLIT_WORDS[k]):
        page.insert_text((x0 + 2 * MM + (j % 5) * 25 * MM, 55 * MM + (j // 5) * 100 * MM), word, fontname="F1", fontsize=8)


def _content(page: fitz.Page, c: dict[str, Any]) -> None:
    if c.get("split") is not None:
        # Разделение листа: whole — обе половины; left — левая на месте; right — правая, сдвинутая влево.
        if c["split"] in ("whole", "left"):
            _split_block(page, 0, 40 * MM)
        if c["split"] == "whole":
            _split_block(page, 1, 200 * MM)
        if c["split"] == "right":
            _split_block(page, 1, 40 * MM)
        return
    if c.get("unique") is not None:
        _unique_content(page, int(c["unique"]), (c.get("marker") or "").split())
        return
    v = c["values"]
    sh = page.new_shape()
    # Сетка и схема.
    for i in range(6):
        x = 60 * MM + i * 45 * MM
        sh.draw_line((x, 30 * MM), (x, 180 * MM))
    if not c["remove_line"]:
        sh.draw_line((60 * MM, 100 * MM), (285 * MM, 100 * MM))
    sh.finish(color=(0, 0, 0), width=0.8)
    if c["extra_line"]:
        sh.draw_line((70 * MM, 120 * MM), (200 * MM, 140 * MM))
        sh.finish(color=(0, 0, 0), width=0.8)
    # Линия, у которой меняют толщину и цвет.
    sh.draw_line((70 * MM, 60 * MM), (250 * MM, 60 * MM))
    sh.finish(color=c["line_color"], width=c["line_width"])
    if c["thin_line"]:
        sh.draw_line((70 * MM, 70 * MM), (250 * MM, 70 * MM))
        sh.finish(color=(0, 0, 0), width=0.1 * MM)
    if c["dot"]:
        sh.draw_circle((300 * MM, 150 * MM), 0.15 * MM)
        sh.finish(color=(0, 0, 0), fill=(0, 0, 0), width=0)
    if c["tick"]:
        sh.draw_line((150 * MM, 101 * MM + 1 * MM), (151 * MM, 101 * MM + 1 * MM))
        sh.finish(color=(0, 0, 0), width=0.6)
    if c["light_fill"]:
        x0 = 300 * MM + c["light_fill_shift"]
        sh.draw_rect(fitz.Rect(x0, 40 * MM, x0 + 40 * MM, 70 * MM))
        sh.finish(color=None, fill=(0.86, 0.86, 0.86), width=0)
    if c["fill_line"]:
        # Тёмная линия поверх светлой заливки.
        sh.draw_line((305 * MM, 55 * MM), (335 * MM, 55 * MM))
        sh.finish(color=(0, 0, 0), width=0.8)
    if c["underlay"]:
        # Подложка серым тонким пером; «dup» — поверх тот же контур толстым
        # пером (дубль подложки, как в выгрузке ЭМ2 (10)).
        gray = (0.667, 0.667, 0.667)
        widths = [0.24] + ([1.2] if c["underlay"] == "dup" else [])
        for wdt in widths:
            sh.draw_line((40 * MM, 200 * MM), (200 * MM, 200 * MM))
            sh.draw_circle((215 * MM, 200 * MM), 4 * MM)
            sh.finish(color=gray, width=wdt)
    if c["table"] is not None:
        # Таблица нагрузок: сетка 4×3 и подписи; dx/dy — сдвиг всей таблицы.
        t = c["table"]
        tx, ty = 40 * MM + t.get("dx", 0.0), 200 * MM + t.get("dy", 0.0)
        for k in range(5):
            sh.draw_line((tx, ty + k * 7 * MM), (tx + 90 * MM, ty + k * 7 * MM))
        for k in range(4):
            sh.draw_line((tx + k * 30 * MM, ty), (tx + k * 30 * MM, ty + 28 * MM))
        sh.finish(color=(0, 0, 0), width=0.5)
    if c["crossing"]:
        # Синий кабель и серая линия подложки пересекаются; меняется только
        # порядок наложения (кто сверху).
        def blue():
            sh.draw_line((260 * MM, 185 * MM), (260 * MM, 215 * MM))
            sh.finish(color=(0, 0, 1), width=1.2)

        def grey():
            sh.draw_line((245 * MM, 200 * MM), (275 * MM, 200 * MM))
            sh.finish(color=(0.667, 0.667, 0.667), width=1.2)
        for draw in ((blue, grey) if c["crossing"] == "gray_top" else (grey, blue)):
            draw()
    if c["cloud"]:
        # Облако изменений: отдельные дуги 5 × 1,6 мм в ряд (как в ЭМ2 (10) PDF 51).
        for k in range(7):
            x = 110 * MM + k * 5.6 * MM
            sh.draw_bezier((x, 120 * MM), (x + 1 * MM, 117.9 * MM), (x + 4 * MM, 117.9 * MM), (x + 5 * MM, 120 * MM))
        sh.finish(color=(0, 0, 0), width=0.25 * MM)
    if c["ticks"]:
        # Ряд параллельных вертикальных штрихов 3 мм (жгут линий, штриховка).
        for k in range(8):
            x = 112 * MM + k * 1.5 * MM
            sh.draw_line((x, 130 * MM), (x, 133 * MM))
        sh.finish(color=(0, 0, 0), width=0.35 * MM)
    if c["squares"]:
        # Ряд залитых квадратов 2 × 2 мм (маркеры).
        for k in range(5):
            x = 112 * MM + k * 3 * MM
            sh.draw_rect(fitz.Rect(x, 145 * MM, x + 2 * MM, 147 * MM))
        sh.finish(color=(0, 0, 0), fill=(0, 0, 0), width=0)
    sh.commit()
    if c.get("marker"):
        # Уникальное содержимое листа: по нему листы различаются при сопоставлении.
        words = c["marker"].split()
        for k, word in enumerate(words):
            page.insert_text((60 * MM + (k % 4) * 50 * MM, 22 * MM + (k // 4) * 6 * MM), word, fontname="F1",
                             fontsize=9)
    page.insert_text((70 * MM, 50 * MM), v["pp"], fontname="F1", fontsize=10)
    page.insert_text((70 * MM, 90 * MM), v["ip"], fontname="F1", fontsize=10)
    page.insert_text((150 * MM, 90 * MM), v["cable"], fontname="F1", fontsize=9)
    page.insert_text((200 * MM, 160 * MM), v["small"], fontname="F1", fontsize=5.1)
    page.insert_text((230 * MM, 160 * MM), v["label"], fontname="F1", fontsize=5.1)
    page.insert_text((70 * MM, 170 * MM), "Кабель силовой с медными жилами", fontname="F1", fontsize=8)
    if c["table"] is not None:
        t = c["table"]
        tx, ty = 40 * MM + t.get("dx", 0.0), 200 * MM + t.get("dy", 0.0)
        cells = [["Щит", "Руст,кВт", "Iр,А"], ["ЩР-1", "5,5", "8,4"], ["ЩР-2", t.get("p2", "7,5"), "11,4"],
                 ["ЩР-3", "3,0", "4,6"]]
        for r_, row in enumerate(cells):
            for k, text in enumerate(row):
                page.insert_text((tx + k * 30 * MM + 2 * MM, ty + r_ * 7 * MM + 5 * MM), text, fontname="F1", fontsize=8)
    if c["baked_warning"]:
        # Предупреждение в ТОМ ЖЕ потоке, что и чертёж (отделить нельзя).
        sh = page.new_shape()
        sh.insert_text((120 * MM, 130 * MM), "НЕ СОГЛАСОВАНО", fontname="F1", fontsize=40, color=(0.85, 0.2, 0.2))
        sh.commit()


_FONT_CACHE: dict[str, bytes] = {}


def _cyr_font() -> bytes:
    if "f" not in _FONT_CACHE:
        for cand in ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                     "/usr/share/fonts/dejavu/DejaVuSans.ttf",
                     "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"):
            if Path(cand).exists():
                _FONT_CACHE["f"] = Path(cand).read_bytes()
                break
        else:
            raise RuntimeError("нет TTF-шрифта с кириллицей для тестов")
    return _FONT_CACHE["f"]


def _frame_and_stamp(page: fitz.Page, s: dict[str, Any]) -> None:
    sh = page.new_shape()
    sh.draw_rect(fitz.Rect(20 * MM, 5 * MM, W - 5 * MM, H - 5 * MM))
    x0, y0 = W - 5 * MM - 185 * MM, H - 5 * MM - 55 * MM
    sh.draw_rect(fitz.Rect(x0, y0, W - 5 * MM, H - 5 * MM))
    sh.draw_line((x0 + 65 * MM, y0), (x0 + 65 * MM, H - 5 * MM))
    sh.draw_line((x0 + 135 * MM, y0 + 15 * MM), (x0 + 135 * MM, H - 5 * MM))
    sh.finish(color=(0, 0, 0), width=0.8)
    sh.commit()
    ly = H - 5 * MM - 31 * MM
    for dx, label in ((1, "Изм."), (9, "Кол.уч."), (20, "Лист"), (29, "№док."), (42, "Подп."), (55, "Дата")):
        page.insert_text((x0 + dx * MM, ly), label, fontname="F1", fontsize=6)
    for k, row in enumerate(s.get("izm_rows", [])):
        page.insert_text((x0 + 1 * MM, ly - (k + 1) * 5 * MM), row, fontname="F1", fontsize=6)
    page.insert_text((x0 + 80 * MM, y0 + 6 * MM), s.get("designation", "ТЕСТ-01-ЭМ"), fontname="F1", fontsize=10)
    page.insert_text((x0 + 70 * MM, y0 + 20 * MM), "Объект испытаний", fontname="F1", fontsize=7)
    sy = H - 5 * MM - 27 * MM
    page.insert_text((x0 + 137 * MM, sy), "Стадия", fontname="F1", fontsize=6)
    page.insert_text((x0 + 152 * MM, sy), "Лист", fontname="F1", fontsize=6)
    page.insert_text((x0 + 166 * MM, sy), "Листов", fontname="F1", fontsize=6)
    page.insert_text((x0 + 154 * MM, sy + 6 * MM), str(s.get("sheet_no", "1")), fontname="F1", fontsize=8)
    page.insert_text((x0 + 70 * MM, H - 5 * MM - 4 * MM), s.get("title", "План 1 этажа"), fontname="F1", fontsize=8)


def _qr_png(seed: int) -> bytes:
    """Настоящий QR (OpenCV) со ссылкой, зависящей от seed, — как QR отметки выпуска."""
    img = cv2.QRCodeEncoder.create().encode(f"https://qr.example.test/d/{seed:04d}")
    img = cv2.resize(img, None, fx=4, fy=4, interpolation=cv2.INTER_NEAREST)
    ok, buf = cv2.imencode(".png", img)
    assert ok
    return bytes(buf)


def _release_overlay(page: fitz.Page, date: str, qr_seed: int) -> None:
    """Штамп выпуска — ПОСЛЕДНИМ потоком содержимого, как в PDF-редакторах."""
    page.insert_font(fontname="F2", fontbuffer=_cyr_font())
    x0, y0 = W - 70 * MM, H - 85 * MM
    sh = page.new_shape()
    sh.draw_rect(fitz.Rect(x0, y0, x0 + 45 * MM, y0 + 20 * MM))
    sh.finish(color=(0.1, 0.5, 0.75), width=0.8)
    sh.insert_text((x0 + 2 * MM, y0 + 5 * MM), "В ПРОИЗВОДСТВО РАБОТ", fontname="F2", fontsize=6, color=(0.1, 0.5, 0.75))
    sh.insert_text((x0 + 2 * MM, y0 + 12 * MM), "ГИП", fontname="F2", fontsize=6, color=(0.1, 0.5, 0.75))
    sh.insert_text((x0 + 2 * MM, y0 + 17 * MM), f"Иванов И.И. {date}", fontname="F2", fontsize=6,
                   color=(0.1, 0.5, 0.75))
    sh.commit()
    page.insert_image(fitz.Rect(x0 + 47 * MM, y0, x0 + 62 * MM, y0 + 15 * MM), stream=_qr_png(qr_seed))


def _warning_png() -> bytes:
    img = Image.new("RGBA", (800, 560), (255, 255, 255, 0))
    d = ImageDraw.Draw(img)
    for k in range(4):
        d.text((60 + k * 160, 80 + k * 110), "NOT APPROVED", fill=(210, 40, 50, 255))
    d.rectangle([0, 0, 799, 559], fill=(255, 255, 255, 30))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _annotations(page: fitz.Page, annots: list[dict[str, Any]]) -> None:
    for a in annots:
        if a["type"] == "freetext":
            an = page.add_freetext_annot(fitz.Rect(*a["rect"]), a["text"], fontsize=8)
            an.set_info(title=a.get("author", "заказчик"))
            an.update()
        elif a["type"] == "cloud":
            an = page.add_polygon_annot([fitz.Point(*p) for p in a["points"]])
            an.set_info(title=a.get("author", "проектировщик"), subject="Облако")
            an.set_colors(stroke=(1, 0, 0))
            an.update()


def _logical_page(spec: dict[str, Any]) -> fitz.Document:
    doc = fitz.open()
    page = doc.new_page(width=W, height=H)
    content = dict(BASE_CONTENT)
    content["values"] = dict(BASE_CONTENT["values"], **spec.get("values", {}))
    for k, v in spec.get("content", {}).items():
        content[k] = v
    content["marker"] = spec.get("marker", "")
    content["unique"] = spec.get("unique")
    content["split"] = spec.get("split")
    page.insert_font(fontname="F1", fontbuffer=_cyr_font())
    _frame_and_stamp(page, spec.get("stamp", {}))
    if spec.get("so_rows") is not None:
        _so_table(page, spec["so_rows"])
    elif spec.get("permit_rows") is not None:
        _permit_table(page, spec["permit_rows"], spec.get("permit_number", "100/26"))
    else:
        _content(page, content)
    page.clean_contents()  # один поток содержимого, как у выгрузки из CAD
    return doc


def _permit_table(page: fitz.Page, rows: list[tuple[str, str, str, str]], number: str) -> None:
    """«Разрешение на внесение изменений»: Изм. | Лист | Содержание изменения | Код."""
    x0, top = 30 * MM, 15 * MM
    widths = (15, 25, 140, 20)
    xs = [x0]
    for w in widths:
        xs.append(xs[-1] + w * MM)
    page.insert_text((x0 + 2 * MM, top - 4 * MM), "Разрешение", fontname="F1", fontsize=8)
    page.insert_text((x0 + 2 * MM, top + 1 * MM), number, fontname="F1", fontsize=8)
    head_y, row_h = top + 10 * MM, 10 * MM
    bottom = head_y + (len(rows) + 2) * row_h
    sh = page.new_shape()
    for x in xs:
        sh.draw_line((x, head_y), (x, bottom))
    for k in range(len(rows) + 3):
        sh.draw_line((xs[0], head_y + k * row_h), (xs[-1], head_y + k * row_h))
    sh.finish(color=(0, 0, 0), width=0.6)
    sh.commit()
    for title, cx in zip(("Изм.", "Лист", "Содержание изменения", "Код"), xs):
        page.insert_text((cx + 2 * MM, head_y + 6 * MM), title, fontname="F1", fontsize=8)
    for k, row in enumerate(rows):
        y = head_y + (k + 1) * row_h + 6 * MM
        for text, cx in zip(row, xs):
            page.insert_text((cx + 2 * MM, y), text, fontname="F1", fontsize=8)


# Спецификация по ГОСТ 21.110 (форма 1): графы и ширины, мм.
SO_COLUMNS = (("pos", "Поз.", 15), ("name", "Наименование и техническая характеристика", 110),
              ("mark", "Тип, марка", 55), ("code", "Код продукции", 30), ("supplier", "Поставщик", 40),
              ("unit", "Ед. изм.", 18), ("qty", "Кол.", 18), ("mass", "Масса 1 ед., кг", 20),
              ("note", "Примечание", 79))
SO_ROW_MM = 8.0
SO_ROWS_PER_PAGE = 24


def _so_table(page: fitz.Page, rows: list[dict[str, Any]]) -> None:
    """Таблица СО: шапка, вертикальные линии граф, линия под каждой строкой.

    Строка — словарь граф; ``section`` — заголовок раздела по центру граф
    наименования, ``center`` — текст наименования по центру (подстрока).
    """
    x0, top, head = 25 * MM, 10 * MM, 25 * MM
    xs = [x0]
    for _f, _t, w in SO_COLUMNS:
        xs.append(xs[-1] + w * MM)
    y_end = top + head + SO_ROWS_PER_PAGE * SO_ROW_MM * MM
    sh = page.new_shape()
    for x in xs:
        sh.draw_line((x, top), (x, y_end))
    sh.draw_line((xs[0], top), (xs[-1], top))
    for k in range(SO_ROWS_PER_PAGE + 1):
        y = top + head + k * SO_ROW_MM * MM
        sh.draw_line((xs[0], y), (xs[-1], y))
    sh.finish(color=(0, 0, 0), width=0.6)
    sh.commit()
    for (_f, title, _w), cx in zip(SO_COLUMNS, xs):
        page.insert_text((cx + 1.5 * MM, top + 14 * MM), title[:22], fontname="F1", fontsize=6)
    for k, row in enumerate(rows[:SO_ROWS_PER_PAGE]):
        base = top + head + k * SO_ROW_MM * MM + 5.5 * MM
        if row.get("section"):
            text = row["section"]
            mid = (xs[1] + xs[3]) / 2
            page.insert_text((mid - len(text) * 2.2, base), text, fontname="F1", fontsize=8)
            continue
        for (field, _t, _w), cx0, cx1 in zip(SO_COLUMNS, xs, xs[1:]):
            text = row.get(field)
            if not text:
                continue
            if field == "name" and row.get("center"):
                page.insert_text(((cx0 + cx1) / 2 - len(text) * 2.0, base), text, fontname="F1", fontsize=7)
            else:
                page.insert_text((cx0 + 1.5 * MM, base), text, fontname="F1", fontsize=7)


def build_page(doc: fitz.Document, spec: dict[str, Any]) -> fitz.Page:
    """Добавить в ``doc`` лист по описанию варианта."""
    logical = _logical_page(spec)
    tr = spec.get("transform")
    if spec.get("as_curves"):
        svg = logical[0].get_svg_image(text_as_path=True)
        logical = fitz.open(stream=fitz.open(stream=svg.encode("utf-8"), filetype="svg").convert_to_pdf(),
                            filetype="pdf")
    if tr or spec.get("as_curves") or spec.get("rotate_attr"):
        if spec.get("rotate_attr"):
            page = doc.new_page(width=H, height=W)
            page.show_pdf_page(page.rect, logical, 0, rotate=spec["rotate_attr"])
            page.set_rotation(spec["rotate_attr"])
        else:
            page = doc.new_page(width=W, height=H)
            dx, dy = (tr or {}).get("dx", 0.0), (tr or {}).get("dy", 0.0)
            sc = (tr or {}).get("scale", 1.0)
            rot = (tr or {}).get("rot", 0.0)
            rect = fitz.Rect(dx, dy, dx + W * sc, dy + H * sc)
            page.show_pdf_page(rect, logical, 0, rotate=rot, keep_proportion=True)
    else:
        doc.insert_pdf(logical)
        page = doc[-1]
    for ct in spec.get("curve_text", []):
        # Надпись кривыми (без текстового слоя) на листе, где остальной текст есть.
        tmp = fitz.open()
        tp = tmp.new_page(width=60 * MM, height=10 * MM)
        tp.insert_font(fontname="F1", fontbuffer=_cyr_font())
        tp.insert_text((1 * MM, 7 * MM), ct["text"], fontname="F1", fontsize=ct.get("size", 10))
        svg = tp.get_svg_image(text_as_path=True)
        vec = fitz.open(stream=fitz.open(stream=svg.encode("utf-8"), filetype="svg").convert_to_pdf(), filetype="pdf")
        x, y = ct["at"]
        page.show_pdf_page(fitz.Rect(x * MM, y * MM, x * MM + 60 * MM, y * MM + 10 * MM), vec, 0)
    if spec.get("stamp_note"):
        # Примечание у верхнего края штампа: центр слова внутри зоны штампа,
        # верх прописных букв выше её границы.
        x0, y0 = W - 5 * MM - 185 * MM, H - 5 * MM - 55 * MM
        page.insert_font(fontname="F1", fontbuffer=_cyr_font())
        page.insert_text((x0 + 3 * MM, y0 + 1.1 * MM), spec["stamp_note"], fontname="F1", fontsize=12)
    if spec.get("release"):
        rel = spec["release"]
        _release_overlay(page, rel.get("date", "09.07.2025"), rel.get("qr_seed", 1))
    if spec.get("warning"):
        page.insert_image(page.rect, stream=_warning_png(), keep_proportion=False)
    if spec.get("annots"):
        _annotations(page, spec["annots"])
    return page


def make_pdf(path: Path, pages: list[dict[str, Any]]) -> Path:
    doc = fitz.open()
    for spec in pages:
        build_page(doc, spec)
    path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(path))
    return path


def sheet(sheet_no: str, title: str, **kw: Any) -> dict[str, Any]:
    spec = {"stamp": {"sheet_no": sheet_no, "title": title, "designation": kw.pop("designation", "ТЕСТ-01-ЭМ"),
                      "izm_rows": kw.pop("izm_rows", [])}}
    spec.update(kw)
    return spec
