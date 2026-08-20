"""Static smoke: кнопки «Обработать» остались, запуск сравнения из UI убран.

Пользовательский запрос: кнопки «Обработать» и «Обработать выбранные»
должны остаться на месте и быть активными, но клик по ним ничего не делает.
Вся пусковая цепочка (preflight → модалка выбора режима → POST
/pairs/opus-only и /pairs/clear-analysis) удалена из фронтенда.
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
HTML = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")


def test_buttons_remain_in_markup():
    assert "Обработать выбранные ({{ scQOSelectedCount }})" in HTML
    assert ">Обработать</button>" in HTML


def test_buttons_have_no_click_handler():
    for handler in ("scQOProcessPair", "scQOOpenConfirm", "scQOStartConfirmed"):
        assert handler not in HTML, handler


def test_launch_chain_removed_from_js():
    for name in (
        "scQOPreflight",
        "scQOOpenConfirm",
        "scQOProcessPair",
        "scQOStartConfirmed",
        "scQOStartOpusOnly",
        "scQOClearAnalysis",
    ):
        assert name not in JS, name


def test_launch_endpoints_are_gone():
    assert "/pairs/opus-only" not in JS
    assert "/pairs/clear-analysis" not in JS
    assert "/pipeline-qwen-opus" not in JS
    assert "/md-enrichment-jobs" not in JS


def test_mode_dialog_removed():
    assert "scQOMode" not in JS
    assert "scQOMode" not in HTML
    assert "Запустить сравнение Opus" not in HTML


def test_selection_ui_still_works():
    # Чекбоксы выбора пар и счётчик остаются — их убирать не просили.
    assert "scQOSelected[p.id]" in HTML
    assert "const scQOSelectedCount = computed(" in JS
