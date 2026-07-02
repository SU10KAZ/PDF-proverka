"""Тесты профиля water_supply_scheme (граф стояков ВК из вектор-слоя).

Юнит-тесты — синтетические (без PDF). Интеграция на реальном пилот-блоке — опциональна
(experiments/ в gitignore, в CI пропускается).
"""
import os
import pytest

from backend.app.pipeline.stages.block_grounding.water_supply_geometry import (
    levels_by_elevation,
    _extract_floors,
    _extract_segments,
    _dia_mm,
    _cluster_by_x,
    _water_distinct_tokens,
    build_water_graph,
    render_water_graph_markdown,
)


def W(x0, y0, x1, y1, text):
    # формат fitz get_text("words"): (x0,y0,x1,y1,text,block,line,word)
    return (x0, y0, x1, y1, text, 0, 0, 0)


def test_levels_by_elevation_linear_recovers():
    # этажи с шагом 3.15 м, y убывает при росте отметки (верх листа = высокий этаж)
    words = []
    for i, elev in enumerate([0.0, 3.15, 6.30, 9.45, 12.60]):
        y = 500 - i * 30
        words.append(W(100, y, 160, y + 10, f"+{elev:.3f}".replace(".", ".")))
    fn, q = levels_by_elevation(words)
    assert fn is not None and q >= 0.9
    # монотонность: меньшая y → большая отметка
    assert fn(500 - 4 * 30) > fn(500)


def test_levels_by_elevation_needs_three():
    words = [W(0, 0, 10, 10, "+3.150"), W(0, 30, 10, 40, "+6.300")]
    fn, q = levels_by_elevation(words)
    assert fn is None and q == 0.0


def test_extract_floors_reads_number():
    words = [W(100, 200, 140, 212, "Этаж"), W(145, 200, 160, 212, "16"),
             W(100, 260, 140, 272, "Этаж"), W(145, 260, 160, 272, "15")]
    floors = _extract_floors(words)
    assert sorted(f[2] for f in floors) == [15, 16]


def test_extract_segments_dia_and_wall():
    words = [W(300, 300, 330, 312, "В2.2"), W(335, 300, 370, 312, "⌀57x"),
             W(375, 300, 400, 312, "3,5")]
    segs = _extract_segments(words)
    assert len(segs) == 1
    s = segs[0]
    assert s[2] == "В2.2" and s[3] == "⌀57x" and s[4] == "3,5"


def test_dia_mm():
    assert _dia_mm("⌀57x") == 57
    assert _dia_mm("∅100") == 100
    assert _dia_mm(None) is None
    assert _dia_mm("abc") is None


def test_cluster_by_x():
    items = [(100, 0, "a"), (110, 0, "b"), (500, 0, "c"), (505, 0, "d")]
    cl = _cluster_by_x(items, tol=30)
    assert len(cl) == 2 and len(cl[0]) == 2 and len(cl[1]) == 2


def test_water_tokens_gate():
    # электрический текст → НЕТ water-токенов → пусто (fail-soft: электроблок не уйдёт в water)
    assert _water_distinct_tokens("QF3.1 ВА-300 РП1 ГРЩ шинопровод К1.1.6") == []
    assert _water_distinct_tokens("Освещение коридора мощность 5 кВт") == []
    # водяной текст → токены есть
    assert _water_distinct_tokens("В2.2 ⌀57x стояк Ст.1 К13н") != []


# ── интеграция на реальном пилот-блоке (skip, если PDF отсутствует) ──
_PILOT = ("experiments/блоки разных дисциплин/ВК/"
          "03_13АВ-РД-ВК2-К6_V1__4VEF-CC3P-P7K.pdf")


@pytest.mark.skipif(not os.path.exists(_PILOT), reason="пилот-PDF отсутствует (experiments/ в gitignore)")
def test_pilot_block_extraction():
    import fitz
    from pathlib import Path
    vtext = fitz.open(_PILOT)[0].get_text()
    g = build_water_graph(Path(_PILOT), vtext)
    assert g is not None
    assert g["profile_id"] == "water_supply_scheme"
    assert g["risers_total"] >= 3
    assert "В2.2" in g["systems"]
    # per-riser регрессия отметок должна быть точной (не смешивать здания)
    assert g["levels"]["y_to_elevation_quality_per_riser"] >= 0.7
    md = render_water_graph_markdown(g)
    assert "Схема стояков ВК" in md and "Отметка" in md
