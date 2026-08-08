"""Static smoke: диалог «Обработать выбранные» после удаления локальных LLM.

Распознавание графики (Qwen) удалено с платформы, поэтому в mode-селекторе
(scQOMode) остались только Opus-режимы, а запуск идёт через endpoint
/pairs/opus-only по уже готовым enriched MD.
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
HTML = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")


def test_mode_ref_and_function_exist():
    assert "const scQOMode = ref('opus_only')" in JS
    assert "async function scQOStartOpusOnly(" in JS
    assert "scQOMode, scQOClearing" in JS  # exposed в setup return


def test_opus_only_calls_dedicated_endpoint():
    sc = JS[JS.index("async function scQOStartOpusOnly("):]
    sc = sc[: sc.index("\n        async function ", 1)] if "\n        async function " in sc else sc[:4000]
    assert "/pairs/opus-only" in sc
    assert "force_qwen" not in sc
    assert "prebuild_large_sheets" not in sc
    assert "clear_comparison_result" in sc


def test_start_confirmed_runs_opus_only():
    sc = JS[JS.index("async function scQOStartConfirmed("):]
    sc = sc[: sc.index("async function scQOStartOpusOnly(")]
    assert "scQOMode.value" in sc
    assert "'clear_result_opus_only'" in sc
    assert "await scQOStartOpusOnly(" in sc


def test_dialog_has_only_opus_mode_radios():
    for val in ("opus_only", "clear_result_opus_only"):
        assert f'value="{val}" v-model="scQOMode"' in HTML, val
    for gone in ("normal", "clear_and_run"):
        assert f'value="{gone}" v-model="scQOMode"' not in HTML, gone


def test_opus_only_copy_mentions_ready_enriched_md():
    assert "Сравнение Opus по уже готовым enriched MD" in HTML


def test_button_label_adapts_to_mode():
    assert "Запустить Opus" in HTML
    assert "Очистить результат и запустить Opus" in HTML


def test_local_llm_endpoints_are_gone():
    assert "/pipeline-qwen-opus" not in JS
    assert "/md-enrichment-jobs" not in JS
    # clear-analysis остаётся — он не про локальные модели
    assert "/pairs/clear-analysis" in JS
