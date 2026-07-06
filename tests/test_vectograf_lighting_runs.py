"""test_vectograf_lighting_runs — верхняя «гребёнка» освещения (прогоны к светильникам).

`_extract_lighting_runs` вытаскивает кабель-прогоны освещения (ППГнг 3х1.5 в П.NN/Пг.NN L=Nм),
нарисованные НАД рядом QF, привязка к фидеру — по X-колонке снаружи. Самодостаточно:
синтетические word-кортежи (x0,y0,x1,y1,text,...), без данных проекта.
"""
from __future__ import annotations

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    _extract_lighting_runs,
    _fmt_lighting_run,
)

PAGE_H = 1000.0  # top-полоса = Y < 0.30·H = 300


def _w(x0, y0, x1, y1, t):
    return (x0, y0, x1, y1, t, 0, 0, 0)


# Две колонки гребёнки в верхней полосе; марка/сечение/прокладка ВЫШЕ якоря L=.
WORDS = [
    _w(90, 20, 140, 30, "ППГнг(A)-HF"), _w(95, 45, 120, 55, "3х1.5"),
    _w(125, 45, 150, 55, "П.32"), _w(100, 65, 130, 75, "L=9м"),
    _w(290, 20, 340, 30, "ППГнг(A)-HF"), _w(295, 45, 320, 55, "3х1.5"),
    _w(325, 45, 355, 55, "Пг.20"), _w(300, 65, 335, 75, "L=275м"),
]


def test_extracts_runs_with_cable_laying_length():
    runs = _extract_lighting_runs(WORDS, PAGE_H)
    assert len(runs) == 2
    r0 = min(runs, key=lambda r: r["x"])
    assert r0["cable"] == "ППГнг(A)-HF 3х1.5"
    assert r0["laying"] == "П.32"
    assert r0["length_m"] == 9
    r1 = max(runs, key=lambda r: r["x"])
    assert r1["laying"] == "Пг.20"
    assert r1["length_m"] == 275


def test_ignores_tokens_below_top_band():
    # тот же кабель, но в нижней полосе (Y=500 > 300) — не гребёнка
    low = [_w(95, 495, 120, 505, "3х1.5"), _w(100, 515, 130, 525, "L=9м")]
    assert _extract_lighting_runs(low, PAGE_H) == []


def test_empty_and_zero_page():
    assert _extract_lighting_runs([], PAGE_H) == []
    assert _extract_lighting_runs(WORDS, 0) == []


def test_length_without_m_suffix():
    w = [_w(95, 45, 120, 55, "3х1.5"), _w(100, 65, 130, 75, "L=110")]
    runs = _extract_lighting_runs(w, PAGE_H)
    assert len(runs) == 1 and runs[0]["length_m"] == 110


def test_fmt_lighting_run():
    assert _fmt_lighting_run({"cable": "ППГнг(A)-HF 3х1.5", "laying": "П.32", "length_m": 9}) \
        == "ППГнг(A)-HF 3х1.5 П.32 L=9м"
    assert _fmt_lighting_run(None) == ""
    assert _fmt_lighting_run({}) == ""
