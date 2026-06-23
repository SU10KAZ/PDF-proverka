"""Static smoke: режим «Только Opus» в диалоге «Обработать выбранные».

В диалог добавлен mode-селектор (scQOMode) с 4 режимами; «Только Opus» вызывает
endpoint /pairs/opus-only (без Qwen). Обычный Qwen→Opus и clear-and-run работают
как раньше.
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
HTML = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")


def test_mode_ref_and_function_exist():
    assert "const scQOMode = ref('normal')" in JS
    assert "async function scQOStartOpusOnly(" in JS
    assert "scQOMode, scQOClearing" in JS  # exposed в setup return


def test_opus_only_calls_dedicated_endpoint():
    sc = JS[JS.index("async function scQOStartOpusOnly("):]
    sc = sc[: sc.index("\n        async function ", 1)] if "\n        async function " in sc else sc[:4000]
    assert "/pairs/opus-only" in sc
    # без Qwen: запрос не должен слать force_qwen/prebuild_large_sheets.
    assert "force_qwen" not in sc
    assert "prebuild_large_sheets" not in sc
    assert "clear_comparison_result" in sc


def test_start_confirmed_branches_on_mode():
    sc = JS[JS.index("async function scQOStartConfirmed("):]
    sc = sc[: sc.index("async function scQOStartOpusOnly(")]
    assert "scQOMode.value" in sc
    assert "'opus_only'" in sc
    assert "'clear_result_opus_only'" in sc
    assert "'clear_and_run'" in sc
    # обычный путь по-прежнему зовёт scQOStart (Qwen→Opus).
    assert "scQOStart(ids)" in sc


def test_dialog_has_four_mode_radios():
    for val in ("normal", "clear_and_run", "opus_only", "clear_result_opus_only"):
        assert f'value="{val}" v-model="scQOMode"' in HTML, val


def test_opus_only_warning_says_no_qwen():
    # предупреждение режима «Только Opus» явно говорит, что Qwen не запускается.
    assert "только сравнение Opus по уже готовым enriched MD" in HTML
    assert "Qwen и распознавание графики повторно запускаться не будут" in HTML


def test_button_label_adapts_to_mode():
    assert "Запустить только Opus" in HTML
    # обычная подпись сохранена.
    assert "Запустить обработку" in HTML


def test_normal_and_clear_modes_preserved():
    # обычный pipeline-запуск (Qwen→Opus) и clear-analysis не удалены.
    assert "/pipeline-qwen-opus" in JS
    assert "/pairs/clear-analysis" in JS
