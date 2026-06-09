"""Static grep-style smoke для UI «Сравнение стадий → 1. Загрузка»:

Task 1 — колонки времени 🟦 Qwen / 🟪 Opus в таблице пар.
Task 2 — режим «Очистить анализ» у кнопки «Обработать выбранные».

Source of truth — `frontend/` (webapp/static проверяется parity-тестом отдельно).
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
HTML = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")


# ─── Task 1: Qwen/Opus columns ───────────────────────────────────────────

def test_html_has_qwen_opus_columns():
    assert "🟦 Qwen" in HTML
    assert "🟪 Opus" in HTML
    # cells use the lane renderer for both lanes
    assert "scQOLaneCell(scQOItemFor(p.id),'qwen')" in HTML
    assert "scQOLaneCell(scQOItemFor(p.id),'opus')" in HTML


def test_js_lane_cell_status_glyphs():
    assert "function scQOLaneCell(" in JS
    # spec glyphs present in the lane renderer
    for token in ("'✓ '", "'… '", "'✗'", "'⏱'", "'⊘'"):
        assert token in JS, token


def test_js_qwen_running_shows_block_progress():
    """Qwen-дорожка обрабатываемой пары показывает «… N/M» из live-блоков."""
    # lane cell consults the live block detail for the current qwen pair
    assert "scQOCurrentBlock" in JS
    assert "cb.index + '/' + cb.total" in JS


# ─── Task 2: clear-analysis mode ─────────────────────────────────────────

def test_html_has_clear_mode_checkbox_and_warning():
    # Режим очистки теперь — radio 'clear_and_run' в mode-селекторе (раньше чекбокс).
    assert 'value="clear_and_run" v-model="scQOMode"' in HTML
    assert "Очистить анализ и запустить заново" in HTML
    # warning copy
    assert "Будут удалены найденные расхождения и ручные отметки проверки" in HTML
    assert "page_enriched.json не удаляются" in HTML
    assert "backup" in HTML.lower()
    # button reflects the mode
    assert "Очистить и запустить" in HTML


def test_js_calls_clear_analysis_endpoint():
    assert "function scQOClearAnalysis(" in JS
    assert "/pairs/clear-analysis" in JS
    assert '"clear_findings": true' in JS or "clear_findings: true" in JS


def test_js_sequences_clear_then_run():
    """«Очистить и запустить»: сначала clear, потом обычный pipeline."""
    # Режим выбирается mode-селектором: 'clear_and_run' (раньше чекбокс).
    assert "mode === 'clear_and_run'" in JS
    # clear precedes start within the confirmed handler
    sc = JS[JS.index("async function scQOStartConfirmed("):]
    sc = sc[: sc.index("async function ", 1)]
    assert "scQOClearAnalysis(" in sc
    assert "await scQOStart(" in sc
    assert sc.index("scQOClearAnalysis(") < sc.index("await scQOStart(")
    # running-job skip is surfaced, not silently ignored
    assert "running job" in sc


def test_js_normal_run_path_preserved():
    """Обычный запуск (без очистки) по-прежнему постит pipeline-qwen-opus."""
    assert "/pipeline-qwen-opus`" in JS
    assert "function scQOStart(" in JS
