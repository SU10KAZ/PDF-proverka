"""test_vectograf_bbox_clip — клип слов геометрии Вектографа по области выделения блока.

`_clip_words_to_bbox` оставляет только слова, чей ЦЕНТР попадает в bbox блока (coords_norm,
page-normalized). Защищает топологию от чужого текста листа (соседняя схема/таблица/штамп),
который `get_text("words")` отдаёт со ВСЕЙ страницы. Самодостаточно: синтетические
word-кортежи (x0,y0,x1,y1,text,...), без данных проекта.
"""
from __future__ import annotations

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    _clip_words_to_bbox,
    _clip_words_to_polygon,
    _filter_text_lines_to_region,
    _point_in_polygon,
)

PAGE_W, PAGE_H = 1000.0, 1000.0


def _w(x0, y0, x1, y1, text):
    # форма PyMuPDF get_text("words"): (x0, y0, x1, y1, "word", block, line, word)
    return (x0, y0, x1, y1, text, 0, 0, 0)


# Левая половина листа (0..500 px) — «свой» блок; правая (500..1000) — соседняя схема.
LEFT = [_w(100, 100, 150, 120, "QF1.1"), _w(120, 300, 180, 320, "К1.1.1"),
        _w(90, 500, 140, 520, "ВА-47")]
RIGHT = [_w(700, 100, 760, 120, "QF9.9"), _w(720, 300, 790, 320, "К9.9.9")]
ALL = LEFT + RIGHT


def _texts(ws):
    return {w[4] for w in ws}


def test_keeps_only_words_inside_bbox():
    """bbox левой половины → чужие QF правой половины отсечены."""
    kept = _clip_words_to_bbox(ALL, [0.0, 0.0, 0.5, 1.0], PAGE_W, PAGE_H)
    assert _texts(kept) == {"QF1.1", "К1.1.1", "ВА-47"}
    assert "QF9.9" not in _texts(kept)


def test_center_rule_not_overlap():
    """Слово попадает по ЦЕНТРУ: край на границе, но центр снаружи — отсекается."""
    straddle = [_w(480, 100, 560, 120, "EDGE")]  # центр 520 > 500
    kept = _clip_words_to_bbox(straddle, [0.0, 0.0, 0.5, 1.0], PAGE_W, PAGE_H)
    assert kept == []


def test_default_boundary_does_not_include_neighbour_text():
    """Текст в прежнем внешнем запасе 1% не принадлежит блоку."""
    near = [_w(505, 100, 515, 120, "NEIGHBOUR")]
    kept = _clip_words_to_bbox(
        near, [0.0, 0.0, 0.5, 1.0], PAGE_W, PAGE_H
    )
    assert kept == []


def test_margin_tolerance():
    """Явный технический margin остаётся доступен, но не используется по умолчанию."""
    near = [_w(505, 100, 515, 120, "NEAR")]  # центр 510 → norm 0.51, в пределах +0.01
    kept = _clip_words_to_bbox(near, [0.0, 0.0, 0.5, 1.0], PAGE_W, PAGE_H, margin=0.01)
    assert _texts(kept) == {"NEAR"}


def test_missing_bbox_returns_no_page_text():
    assert _clip_words_to_bbox(ALL, None, PAGE_W, PAGE_H) == []


def test_malformed_bbox_returns_no_page_text():
    assert _clip_words_to_bbox(ALL, [1, 2, 3], PAGE_W, PAGE_H) == []
    assert _clip_words_to_bbox(ALL, [0.5, 0.5, 0.1, 0.1], PAGE_W, PAGE_H) == []  # x1<x0


def test_empty_clip_on_busy_page_does_not_leak_full_page():
    """Пустой клип не должен подменяться текстом всего листа."""
    many = ALL * 3  # 15 слов
    tiny = [0.4, 0.4, 0.4001, 0.4001]  # внутри 0 слов
    assert _clip_words_to_bbox(many, tiny, PAGE_W, PAGE_H) == []


def test_zero_page_dims_return_no_page_text():
    assert _clip_words_to_bbox(ALL, [0.0, 0.0, 0.5, 1.0], 0, 0) == []


def test_small_result_kept_when_page_is_small():
    """<3 слов остаётся, но и лист маленький (<10) → это НЕ подозрительный клип, отдаём как есть."""
    two_left = LEFT[:2]  # 2 слова, оба в bbox
    kept = _clip_words_to_bbox(two_left, [0.0, 0.0, 0.5, 1.0], PAGE_W, PAGE_H)
    assert _texts(kept) == {"QF1.1", "К1.1.1"}


# ── Полигональный клип (point-in-polygon по polygon_points_norm) ──────────────

# L-образный контур: занимает левую половину, но с «вырезом» в правом-нижнем углу.
# Прямоугольный bbox накрыл бы весь квадрат [0..0.5]×[0..1], полигон — только L.
L_SHAPE = [[0.0, 0.0], [0.5, 0.0], [0.5, 0.4], [0.25, 0.4], [0.25, 1.0], [0.0, 1.0]]


def test_point_in_polygon_basic():
    square = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]
    assert _point_in_polygon(0.5, 0.5, square) is True
    assert _point_in_polygon(1.5, 0.5, square) is False


def test_polygon_excludes_notch_that_bbox_would_keep():
    """Слово в «вырезе» L-контура (внутри bbox, но вне полигона) — отсекается по полигону."""
    notch = [_w(350, 700, 400, 720, "CHUZHOY")]  # центр (0.375, 0.71): x>0.25 при y>0.4 → вне L
    kept = _clip_words_to_polygon(notch + LEFT, L_SHAPE, PAGE_W, PAGE_H)
    assert "CHUZHOY" not in _texts(kept)
    assert {"QF1.1", "К1.1.1", "ВА-47"} <= _texts(kept)  # свои слова остались


def test_polygon_too_few_vertices_returns_no_page_text():
    assert _clip_words_to_polygon(ALL, [[0.0, 0.0], [1.0, 1.0]], PAGE_W, PAGE_H) == []
    assert _clip_words_to_polygon(ALL, None, PAGE_W, PAGE_H) == []


def test_polygon_empty_clip_on_busy_page_does_not_leak_full_page():
    """Полигон в чужих координатах не должен возвращать текст всего листа."""
    many = ALL * 3
    far = [[9.0, 9.0], [9.1, 9.0], [9.1, 9.1], [9.0, 9.1]]
    assert _clip_words_to_polygon(many, far, PAGE_W, PAGE_H) == []


def test_polygon_zero_page_dims_return_no_page_text():
    assert _clip_words_to_polygon(ALL, L_SHAPE, 0, 0) == []


# ── Фильтр текст-строк по области (правило «вектограф = только текст полигона») ────

# region_words = слова ВНУТРИ полигона (только «свои» токены схемы).
REGION = [_w(0, 0, 0, 0, t) for t in
          ("QF1.1", "К2.1.1", "598кВт", "ВА-333А", "250А", "Ру=1173.0", "Кс=0.165")]


def test_filter_keeps_inregion_lines_drops_others():
    text = "\n".join([
        "К2.1.1 : 598кВт - ВА-333А 250А",      # свои токены → остаётся
        "Ру=1173.0 Кс=0.165",                   # расчёт панели → остаётся
        "Степень защиты IP31.",                 # примечание (чужие токены) → уходит
        "Проверка коэффициентов трансформации", # ТТ-заголовок → уходит
    ])
    out = _filter_text_lines_to_region(text, REGION).split("\n")
    assert "К2.1.1 : 598кВт - ВА-333А 250А" in out
    assert "Ру=1173.0 Кс=0.165" in out
    assert "Степень защиты IP31." not in out
    assert "Проверка коэффициентов трансформации" not in out


def test_filter_preserves_order_and_blank_lines():
    text = "К2.1.1\n\n598кВт"
    assert _filter_text_lines_to_region(text, REGION) == "К2.1.1\n\n598кВт"


def test_filter_failsoft_no_region_returns_text():
    text = "любой текст"
    assert _filter_text_lines_to_region(text, []) == text
    assert _filter_text_lines_to_region(text, None) == text


def test_filter_failsoft_empty_result_returns_original():
    # ни один токен не в region → результат пуст → откат на исходный текст
    text = "Степень защиты IP31.\nКлиматическое исполнение УХЛ4."
    assert _filter_text_lines_to_region(text, REGION) == text
