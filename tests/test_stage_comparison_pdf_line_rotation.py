"""Рамки строк нативного текстового слоя на повёрнутой странице.

Однолинейные схемы часто выпускают через `/Rotate 270`. `_pdf_text_lines` уже
приводит прямоугольник строки к зрительным координатам, и второй поворот в
`_normalized_bbox` транспонировал рамку: горизонтальная строка становилась
вертикальной полоской шириной в четыре тысячных страницы. Подсветка
доказательства вставала не на своё место, а вырожденные рамки отбрасывались
совсем — на этом листе терялось 70 строк из 282.

Проверка идёт не «на глаз», а против независимого эталона: штатный поиск
PyMuPDF отдаёт прямоугольник в НЕповёрнутых координатах, поэтому правильная
зрительная рамка — это результат ровно одного применения матрицы поворота.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.services.stage_comparison import text_comparison


ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison"
ROTATED_PDF = (
    STORE
    / "stage_1/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1"
    / "versions/v001/02_work/document.pdf"
)
#: Подпись, которая на листе читается горизонтально. До правки её рамка была
#: развёрнута на 90°.
HORIZONTAL_LABEL = "Шкаф упр. насосов"


@pytest.fixture(scope="module")
def rotated_page():
    fitz = pytest.importorskip("fitz")
    if not ROTATED_PDF.is_file():
        pytest.skip("корпус ГРЩ не установлен")
    document = fitz.open(str(ROTATED_PDF))
    try:
        page = document[0]
        assert page.rotation == 270
        fragments = text_comparison._pdf_line_fragments(document, {1}, "left", fitz)[1]
        yield page, fragments, fitz
    finally:
        document.close()


def _normalize(page, rect) -> dict[str, float]:
    area = page.rect
    return {
        "x": (rect.x0 - area.x0) / area.width,
        "y": (rect.y0 - area.y0) / area.height,
        "width": rect.width / area.width,
        "height": rect.height / area.height,
    }


def test_box_matches_pymupdf_search_rotated_once(rotated_page):
    """Рамка строки совпадает с независимым эталоном."""
    page, fragments, _ = rotated_page
    found = page.search_for(HORIZONTAL_LABEL)
    assert found, "эталонная подпись не найдена в документе"
    expected = _normalize(page, found[0] * page.rotation_matrix)
    fragment = next(
        item for item in fragments if item["text"].startswith("Шкаф упр")
    )
    actual = fragment["bboxes"][0]
    for key, value in expected.items():
        assert actual[key] == pytest.approx(value, abs=1e-4), key


def test_horizontal_label_is_wider_than_tall(rotated_page):
    """Транспонированная рамка была бы уже строки и выше подписи."""
    _, fragments, _ = rotated_page
    fragment = next(
        item for item in fragments if item["text"].startswith("Шкаф упр")
    )
    box = fragment["bboxes"][0]
    assert box["width"] > box["height"]


def test_boxes_stay_inside_the_page(rotated_page):
    _, fragments, _ = rotated_page
    for fragment in fragments:
        box = fragment["bboxes"][0]
        assert 0.0 <= box["x"] <= 1.0
        assert 0.0 <= box["y"] <= 1.0
        assert box["x"] + box["width"] <= 1.0 + 1e-6
        assert box["y"] + box["height"] <= 1.0 + 1e-6


def test_vertical_drawing_labels_are_still_vertical(rotated_page):
    """Правка не разворачивает всё подряд.

    Часть подписей на схеме действительно набрана вдоль столбцов; их рамки
    обязаны остаться вытянутыми по вертикали.
    """
    _, fragments, _ = rotated_page
    vertical = [
        item
        for item in fragments
        if item["bboxes"][0]["height"] > item["bboxes"][0]["width"]
    ]
    assert vertical
    assert len(vertical) < len(fragments)
