"""Static smoke: колонки 🟦 Qwen / 🟪 Opus переживают refresh страницы.

Баг: после F5 scQOItemFor возвращал null (in-memory job потерян) → «—».
Фикс: scQOItemFor падает на persisted timings (scQOPairTimings), которые
грузятся scQOLoadPairTimings() с backend /pipeline-qwen-opus/pair-timings.

Регрессия (PR #16 object-autoselect): путь scTryAutoLoadSession грузит сессию
МИНУЯ scLoadSession, поэтому loader там тоже надо звать. Плюс: scQOItemFor
должен предпочитать persisted timing терминальному in-memory job (иначе
устаревший qopipe failed/skipped перебивает свежий ручной repair), а
scQOItemLaneMs — уметь брать готовый *_duration_sec без started_at.
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


def test_autoload_path_calls_loader():
    # object-autoselect путь (scTryAutoLoadSession) минует scLoadSession —
    # должен сам подтянуть persisted timings + active job, иначе после F5 «—».
    sc = JS[JS.index("async function scTryAutoLoadSession("):]
    sc = sc[: sc.index("async function ", 1)]
    assert "scQOLoadPairTimings()" in sc
    assert "scQORestoreActive()" in sc


def test_itemfor_prefers_persisted_over_terminal_job():
    sc = JS[JS.index("function scQOItemFor("):]
    sc = sc[: sc.index("async function scQOLoadPairTimings(")]
    # живой job берётся ТОЛЬКО если running/queued.
    assert "jobLive" in sc
    assert "['running', 'queued'].includes(job.status)" in sc
    # persisted timing проверяется РАНЬШЕ терминального job-item.
    i_persisted = sc.index("scQOPairTimings.value")
    i_terminal = sc.rindex("job.items")
    assert i_persisted < i_terminal


def test_lane_ms_duration_sec_fallback():
    sc = JS[JS.index("function scQOItemLaneMs("):]
    sc = sc[: sc.index("function scQOItemLaneLabel(")]
    # без started_at берём готовый *_duration_sec (repair/manual timing).
    assert "_duration_sec'" in sc
    assert "* 1000" in sc


def test_lane_cell_done_without_duration_shows_check():
    sc = JS[JS.index("function scQOLaneCell("):]
    sc = sc[: sc.index("function scQOLaneColor(")]
    # done без длительности → «✓» (не вводящее «✓ 0с»).
    assert "dur ? '✓ ' + dur : '✓'" in sc
