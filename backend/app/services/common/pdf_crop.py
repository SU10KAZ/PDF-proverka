"""Локальная вырезка кропов блоков из исходного PDF (заглушка вместо портала).

Портал отдаёт кропы по /api/crops/<токен>, но для большинства дисциплин
(АР/ПС/ЭО) это растр 300 DPI — текст оттуда достаётся только нейросетью.
Пока портал не перевёл кропы на вектор (обещано), режем блоки сами из
исходного PDF комплекта: у нас есть сам PDF, page_index и coords_norm из
`*_blocks.json`. Такой кроп сохраняет вектор-слой страницы — pdftotext и
вектограф работают по нему дословно, без OCR-галлюцинаций и гомоглифов.

Механика: одностраничный PDF (insert_pdf страницы-источника) + CropBox по
bbox блока. Координаты coords_norm нормированы в ВИЗУАЛЬНОЙ ориентации
(после /Rotate, см. docs/new_upload_format.md) — в систему страницы их
переводит page.derotation_matrix (проверено на живом АР: rotation 0 и 90,
текст кропа совпал с MD-блоком дословно).

Координатные системы (эмпирика PyMuPDF 1.27, проверено ревью на матрице
24 кейсов: 4 поворота × 3 mediabox × full/inset cropbox):
- derotation_matrix даёт координаты с якорем (0,0) в верхнем-левом углу
  ВИДИМОЙ области (CropBox∩MediaBox — именно по ней строится page.rect);
- set_cropbox ждёт систему «x как в PDF, y вниз от ВЕРХА mediabox» (как
  getter page.cropbox) → сдвиг на верхний-левый угол видимой области;
- getter page.cropbox возвращает СЫРОЙ /CropBox без клипа к mediabox —
  при свесе за mediabox использовать его якорем нельзя (кроп молча уедет
  на величину свеса), поэтому якорь считаем от пересечения.

Ограничение viewport-кропа: контент вне CropBox из файла не удаляется —
кроп страницы-скана весит как вся страница (гарды размера — на вызывающей
стороне, crop_cache). Полигональные блоки режутся по bbox без маски.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


class PdfCropError(Exception):
    """Вырезка кропа не удалась; str(e) — короткая машинная причина."""


def _import_fitz():
    """PyMuPDF как PdfCropError при отсутствии — фолбэк-скачивание живо."""
    try:
        import fitz  # PyMuPDF
        return fitz
    except Exception as e:
        raise PdfCropError(f"fitz_unavailable: {e}") from e


def open_pdf(pdf_path):
    """Открыть исходный PDF один раз на весь комплект (fitz.Document).

    fitz.Document не потокобезопасен — использовать из одного потока.
    """
    fitz = _import_fitz()
    try:
        return fitz.open(str(pdf_path))
    except Exception as e:
        raise PdfCropError(f"open: {e}") from e


def extract_block_crop(
    doc,
    page_index: int,
    coords_norm,
    out_path,
    *,
    min_side_norm: float = 0.001,
) -> int:
    """Вырезать блок в одностраничный PDF out_path, вернуть размер в байтах.

    doc — открытый fitz.Document исходника; coords_norm — [x0,y0,x1,y1]
    top-left визуальной ориентации. Бросает PdfCropError с причиной
    (bad_page / bad_geometry / bad_rotation / write: …). Запись атомарна
    (tmp + os.replace) — прибитый процесс не оставляет битый файл под
    целевым именем.
    """
    fitz = _import_fitz()

    if coords_norm is None or len(coords_norm) < 4:
        raise PdfCropError("bad_geometry: no coords_norm")
    try:
        x0, y0, x1, y1 = (float(v) for v in coords_norm[:4])
    except (TypeError, ValueError):
        raise PdfCropError("bad_geometry: non-numeric coords_norm")
    x0, x1 = sorted((min(max(x0, 0.0), 1.0), min(max(x1, 0.0), 1.0)))
    y0, y1 = sorted((min(max(y0, 0.0), 1.0), min(max(y1, 0.0), 1.0)))
    if (x1 - x0) < min_side_norm or (y1 - y0) < min_side_norm:
        raise PdfCropError("bad_geometry: degenerate rect")

    if not isinstance(page_index, int) or not (0 <= page_index < doc.page_count):
        raise PdfCropError(f"bad_page: {page_index!r}")

    out = fitz.open()
    try:
        out.insert_pdf(doc, from_page=page_index, to_page=page_index)
        page = out[0]

        # /Rotate не кратный 90 (невалидный PDF): ядро MuPDF (page.rect) и
        # обёртка (page.rotation/derotation_matrix) округляют его ПО-РАЗНОМУ
        # → системы координат внутренне противоречивы, кроп молча уезжает.
        raw_rot = out.xref_get_key(page.xref, "Rotate")
        if raw_rot and raw_rot[0] == "int":
            try:
                if int(raw_rot[1]) % 90 != 0:
                    raise PdfCropError(f"bad_rotation: /Rotate {raw_rot[1]}")
            except ValueError:
                pass

        # видимая область = /CropBox ∩ /MediaBox в системе set_cropbox
        # (x как в PDF, y вниз от верха mediabox); getter cropbox отдаёт
        # сырой /CropBox — при свесе за mediabox якорь без клипа неверен
        mb = page.mediabox
        visible = page.cropbox & fitz.Rect(mb.x0, 0.0, mb.x1, mb.y1 - mb.y0)
        if visible.is_empty or visible.is_infinite:
            raise PdfCropError("bad_geometry: empty visible area")

        visual = page.rect  # видимая ориентация (с учётом /Rotate)
        # гард консистентности: page.rect обязан совпадать с видимой
        # областью (стороны переставлены при 90/270) — ловит расхождение
        # ядра и обёртки, недостижимое через page.rotation (оно уже
        # нормализовано)
        vw, vh = visible.width, visible.height
        if page.rotation in (90, 270):
            vw, vh = vh, vw
        if abs(visual.width - vw) > 1.0 or abs(visual.height - vh) > 1.0:
            raise PdfCropError(
                f"bad_rotation: page.rect {visual.width:.0f}x{visual.height:.0f} "
                f"vs visible {vw:.0f}x{vh:.0f}"
            )

        rect = fitz.Rect(
            x0 * visual.width, y0 * visual.height,
            x1 * visual.width, y1 * visual.height,
        )
        unrot = rect * page.derotation_matrix
        unrot.normalize()
        target = (unrot + (visible.x0, visible.y0, visible.x0, visible.y0)) & visible
        if target.is_empty or target.is_infinite:
            raise PdfCropError("bad_geometry: rect outside page")
        try:
            page.set_cropbox(target)
        except Exception as e:
            raise PdfCropError(f"bad_geometry: set_cropbox: {e}") from e

        dst = Path(out_path)
        tmp = dst.with_name(dst.name + f".{os.getpid()}.tmp")
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            out.save(str(tmp), garbage=3, deflate=True)
            os.replace(tmp, dst)
        except Exception as e:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise PdfCropError(f"write: {e}") from e
        return dst.stat().st_size
    finally:
        out.close()


def resolve_version_pdf(version_dir) -> Optional[Path]:
    """Найти исходный PDF комплекта версии для локальной вырезки кропов.

    Приоритет: каноническая рабочая копия 02_work/document.pdf; иначе —
    единственный *.pdf в 01_input (при нескольких кандидатах выбрать нельзя —
    возвращаем None, вызывающий уйдёт в фолбэк-скачивание).
    """
    version_dir = Path(version_dir)
    work_pdf = version_dir / "02_work" / "document.pdf"
    if work_pdf.is_file():
        return work_pdf
    inp = version_dir / "01_input"
    if not inp.is_dir():
        return None
    pdfs = sorted(p for p in inp.rglob("*.pdf")
                  if p.is_file() and "crops" not in p.relative_to(inp).parts)
    return pdfs[0] if len(pdfs) == 1 else None
