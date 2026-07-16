"""Тесты локальной вырезки кропов из PDF (pdf_crop.py).

Геометрия проверяется самосогласованно: в синтетический PDF кладём текст
в известные точки, coords_norm блока получаем через page.search_for
(визуальные координаты — как у портала), вырезаем и проверяем, что в кропе
остался ровно нужный текст. Повороты 0/90/180/270 — ключевые случаи:
формулы visual→unrotated из docs/new_upload_format.md здесь заменяет
page.derotation_matrix.
"""
from __future__ import annotations

import pytest

fitz = pytest.importorskip("fitz")

from backend.app.services.common.pdf_crop import (
    PdfCropError,
    extract_block_crop,
    open_pdf,
    resolve_version_pdf,
)


def _make_pdf(path, rotation: int = 0) -> None:
    """Страница 600×400 с двумя надписями в разных углах."""
    doc = fitz.open()
    page = doc.new_page(width=600, height=400)
    page.insert_text((50, 100), "ALPHA", fontsize=20)
    page.insert_text((400, 350), "BRAVO", fontsize=20)
    if rotation:
        page.set_rotation(rotation)
    doc.save(str(path))
    doc.close()


def _coords_norm_for(doc, needle: str, pad: float = 0.03) -> list[float]:
    """Визуальные coords_norm вокруг найденного текста (как отдаёт портал).

    search_for возвращает НЕповёрнутые координаты — портал же отдаёт
    визуальные (после /Rotate), поэтому конвертируем rotation_matrix'ом.
    """
    page = doc[0]
    rect = page.search_for(needle)[0] * page.rotation_matrix
    rect.normalize()
    vr = page.rect
    return [
        max(0.0, (rect.x0 - pad * vr.width) / vr.width),
        max(0.0, (rect.y0 - pad * vr.height) / vr.height),
        min(1.0, (rect.x1 + pad * vr.width) / vr.width),
        min(1.0, (rect.y1 + pad * vr.height) / vr.height),
    ]


def _crop_text(path) -> str:
    doc = fitz.open(str(path))
    try:
        return doc[0].get_text("text")
    finally:
        doc.close()


@pytest.mark.parametrize("rotation", [0, 90, 180, 270])
def test_crop_isolates_target_text_all_rotations(tmp_path, rotation):
    src_path = tmp_path / "src.pdf"
    _make_pdf(src_path, rotation=rotation)
    doc = open_pdf(src_path)
    try:
        coords = _coords_norm_for(doc, "ALPHA")
        out = tmp_path / f"crop_{rotation}.pdf"
        size = extract_block_crop(doc, 0, coords, out)
        assert size == out.stat().st_size > 0
        text = _crop_text(out)
        assert "ALPHA" in text
        assert "BRAVO" not in text
    finally:
        doc.close()


@pytest.mark.parametrize("rotation", [0, 90])
def test_crop_centered_mediabox_cad_export(tmp_path, rotation):
    """CAD-экспорт с mediabox от (−w/2,−h/2): якорь cropbox ≠ якорь mediabox.

    Реальный случай ОВ0-1 (110 из 467 страниц): без сдвига в систему
    mediabox прямоугольник «вылетал» за страницу (rect outside page).
    """
    src_path = tmp_path / "src.pdf"
    doc = fitz.open()
    page = doc.new_page(width=600, height=400)
    # после сдвига mediabox видимой останется область x∈[0,300], y∈[200,400]
    # (координаты контента в PDF абсолютные) — надписи кладём внутрь неё
    page.insert_text((50, 250), "ALPHA", fontsize=20)
    page.insert_text((200, 370), "BRAVO", fontsize=20)
    page.set_mediabox(fitz.Rect(-300, -200, 300, 200))
    if rotation:
        page.set_rotation(rotation)
    doc.save(str(src_path))
    doc.close()

    doc = open_pdf(src_path)
    try:
        coords = _coords_norm_for(doc, "ALPHA")
        out = tmp_path / "crop.pdf"
        extract_block_crop(doc, 0, coords, out)
        text = _crop_text(out)
        assert "ALPHA" in text
        assert "BRAVO" not in text
    finally:
        doc.close()


def test_crop_clamps_out_of_range_coords(tmp_path):
    src_path = tmp_path / "src.pdf"
    _make_pdf(src_path)
    doc = open_pdf(src_path)
    try:
        out = tmp_path / "crop.pdf"
        # выход за [0,1] и перепутанный порядок углов — нормализуется:
        # y-диапазон после клампа/сортировки = [0, 0.6] → ALPHA (y≈0.25)
        # внутри, BRAVO (y≈0.875) обязан быть отрезан (иначе «клэмп»
        # молча выродился в кроп всей страницы)
        extract_block_crop(doc, 0, [1.5, 0.6, -0.2, -0.1], out)
        text = _crop_text(out)
        assert "ALPHA" in text
        assert "BRAVO" not in text
    finally:
        doc.close()


def test_crop_cropbox_overhangs_mediabox(tmp_path):
    """Сырой /CropBox со свесом за /MediaBox: якорь = пересечение.

    Getter page.cropbox отдаёт /CropBox БЕЗ клипа к mediabox — якорь по
    нему смещал кроп ровно на величину свеса (тихо, без исключения).
    """
    src_path = tmp_path / "src.pdf"
    doc = fitz.open()
    page = doc.new_page(width=600, height=400)
    page.insert_text((150, 150), "ALPHA", fontsize=20)
    page.insert_text((450, 350), "BRAVO", fontsize=20)
    doc.xref_set_key(page.xref, "CropBox", "[-100 0 600 450]")  # свес 100pt слева, 50pt сверху
    doc.save(str(src_path))
    doc.close()

    doc = open_pdf(src_path)
    try:
        coords = _coords_norm_for(doc, "ALPHA")
        out = tmp_path / "crop.pdf"
        extract_block_crop(doc, 0, coords, out)
        text = _crop_text(out)
        assert "ALPHA" in text
        assert "BRAVO" not in text
    finally:
        doc.close()


def test_crop_rotate_not_multiple_of_90_raises(tmp_path):
    """/Rotate 45 (невалидный PDF): ядро и обёртка PyMuPDF нормализуют его
    по-разному → координатные системы противоречивы. Раньше кроп молча
    уезжал; теперь — PdfCropError → фолбэк-скачивание."""
    src_path = tmp_path / "src.pdf"
    doc = fitz.open()
    page = doc.new_page(width=600, height=400)
    page.insert_text((50, 100), "ALPHA", fontsize=20)
    doc.xref_set_key(page.xref, "Rotate", "45")
    doc.save(str(src_path))
    doc.close()

    doc = open_pdf(src_path)
    try:
        with pytest.raises(PdfCropError, match="bad_rotation"):
            extract_block_crop(doc, 0, [0.1, 0.1, 0.9, 0.9], tmp_path / "c.pdf")
        assert not (tmp_path / "c.pdf").exists()
    finally:
        doc.close()


def test_missing_fitz_raises_pdf_crop_error(tmp_path, monkeypatch):
    """Без PyMuPDF open_pdf обязан бросить PdfCropError (не ImportError) —
    иначе download-фолбэк в crop_cache мёртв."""
    import sys
    src_path = tmp_path / "src.pdf"
    _make_pdf(src_path)
    monkeypatch.setitem(sys.modules, "fitz", None)  # import fitz → ImportError
    with pytest.raises(PdfCropError, match="fitz_unavailable"):
        open_pdf(src_path)


def test_crop_write_is_atomic_no_tmp_leftover(tmp_path):
    src_path = tmp_path / "src.pdf"
    _make_pdf(src_path)
    doc = open_pdf(src_path)
    try:
        out = tmp_path / "crops" / "crop.pdf"
        extract_block_crop(doc, 0, [0.02, 0.15, 0.4, 0.35], out)
        assert out.is_file()
        assert list(out.parent.glob("*.tmp")) == []
    finally:
        doc.close()


def test_crop_bad_geometry_raises(tmp_path):
    src_path = tmp_path / "src.pdf"
    _make_pdf(src_path)
    doc = open_pdf(src_path)
    try:
        out = tmp_path / "crop.pdf"
        with pytest.raises(PdfCropError, match="bad_geometry"):
            extract_block_crop(doc, 0, None, out)
        with pytest.raises(PdfCropError, match="bad_geometry"):
            extract_block_crop(doc, 0, [0.5, 0.5, 0.5, 0.5], out)  # вырожденный
        with pytest.raises(PdfCropError, match="bad_geometry"):
            extract_block_crop(doc, 0, [0.1, "x", 0.2, 0.3], out)
        assert not out.exists()
    finally:
        doc.close()


def test_crop_bad_page_raises(tmp_path):
    src_path = tmp_path / "src.pdf"
    _make_pdf(src_path)
    doc = open_pdf(src_path)
    try:
        with pytest.raises(PdfCropError, match="bad_page"):
            extract_block_crop(doc, 5, [0.1, 0.1, 0.9, 0.9], tmp_path / "c.pdf")
        with pytest.raises(PdfCropError, match="bad_page"):
            extract_block_crop(doc, -1, [0.1, 0.1, 0.9, 0.9], tmp_path / "c.pdf")
    finally:
        doc.close()


def test_open_pdf_missing_raises(tmp_path):
    with pytest.raises(PdfCropError, match="open"):
        open_pdf(tmp_path / "нет_такого.pdf")


def test_resolve_version_pdf_prefers_work_copy(tmp_path):
    (tmp_path / "02_work").mkdir(parents=True)
    (tmp_path / "01_input").mkdir(parents=True)
    (tmp_path / "02_work" / "document.pdf").write_bytes(b"%PDF-1.7 work")
    (tmp_path / "01_input" / "a.pdf").write_bytes(b"%PDF-1.7 a")
    assert resolve_version_pdf(tmp_path) == tmp_path / "02_work" / "document.pdf"


def test_resolve_version_pdf_single_input(tmp_path):
    (tmp_path / "01_input").mkdir(parents=True)
    (tmp_path / "01_input" / "a.pdf").write_bytes(b"%PDF-1.7 a")
    # кэш кропов (сам из PDF) кандидатом не считается
    (tmp_path / "01_input" / "crops").mkdir()
    (tmp_path / "01_input" / "crops" / "blk_x.pdf").write_bytes(b"%PDF crop")
    assert resolve_version_pdf(tmp_path) == tmp_path / "01_input" / "a.pdf"


def test_resolve_version_pdf_ambiguous_or_missing(tmp_path):
    assert resolve_version_pdf(tmp_path) is None
    (tmp_path / "01_input").mkdir(parents=True)
    assert resolve_version_pdf(tmp_path) is None
    (tmp_path / "01_input" / "a.pdf").write_bytes(b"%PDF a")
    (tmp_path / "01_input" / "b.pdf").write_bytes(b"%PDF b")
    assert resolve_version_pdf(tmp_path) is None  # двусмысленно — фолбэк наружу
