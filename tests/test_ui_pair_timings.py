"""Static smoke: колонки 🟦 Qwen / 🟪 Opus переживают refresh страницы.

Баг: после F5 scQOItemFor возвращал null (in-memory job потерян) → «—».
Фикс: scQOItemFor падает на persisted timings (scQOPairTimings), которые
грузятся scQOLoadPairTimings() с backend /pipeline-qwen-opus/pair-timings.
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")


def test_pair_timings_ref_and_loader_exist():
    assert "const scQOPairTimings = ref({" in JS
    assert "async function scQOLoadPairTimings(" in JS
    assert "/pipeline-qwen-opus/pair-timings" in JS


def test_itemfor_falls_back_to_persisted_timings():
    sc = JS[JS.index("function scQOItemFor("):]
    sc = sc[: sc.index("async function scQOLoadPairTimings(")]
    # приоритет живого job, затем fallback на scQOPairTimings
    assert "scQOJob.value" in sc
    assert "scQOPairTimings.value" in sc
    # больше не безусловный `return null` при отсутствии job
    assert sc.count("return null") <= 2  # только финальный + защитный


def test_loader_called_on_session_load():
    # в scLoadSession после restore вызывается загрузка таймингов
    sc = JS[JS.index("async function scLoadSession("):]
    sc = sc[: sc.index("async function ", 1)]
    assert "scQOLoadPairTimings()" in sc


def test_loader_called_after_job_completion():
    # после терминального статуса в scQOPollJob кешируем тайминги
    sc = JS[JS.index("function scQOPollJob("):]
    sc = sc[: sc.index("async function scQOCancel(") if "async function scQOCancel(" in sc else len(sc)]
    assert "scQOLoadPairTimings" in sc


def test_lane_cell_glyphs_unchanged():
    # scQOLaneCell по-прежнему рендерит ✓/…/✗/⏱/⊘ (работает и на fallback-объекте)
    assert "function scQOLaneCell(" in JS
    for g in ("'✓ '", "'✗'", "'⏱'", "'⊘'"):
        assert g in JS, g


def test_exposed_in_setup_return():
    assert "scQOPairTimings, scQOLoadPairTimings," in JS
