"""Static grep-style smoke для UI «Сравнение стадий → 1. Загрузка».

После удаления локальных LLM-мощностей в таблице пар осталась одна колонка
времени — 🟪 Opus; колонка 🟦 Qwen убрана вместе с полосой распознавания.
Очистка анализа (/pairs/clear-analysis) вызывалась только из диалога запуска
кнопки «Обработать» — вместе с ним из фронтенда убрана.

Source of truth — `frontend/`.
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
HTML = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")


def test_html_has_opus_column_without_qwen():
    assert "🟪 Opus" in HTML
    assert "🟦 Qwen" not in HTML
    assert "scQOLaneCell(scQOItemFor(p.id),'opus')" in HTML
    assert "scQOLaneCell(scQOItemFor(p.id),'qwen')" not in HTML


def test_js_lane_cell_status_glyphs():
    assert "function scQOLaneCell(" in JS
    for token in ("'✓ '", "'… '", "'✗'", "'⏱'", "'⊘'"):
        assert token in JS, token


def test_js_no_longer_calls_clear_analysis_endpoint():
    # Единственным вызывающим была модалка запуска Opus; кнопки «Обработать»
    # остались на месте, но ничего не запускают.
    assert "scQOClearAnalysis" not in JS
    assert "/pairs/clear-analysis" not in JS
