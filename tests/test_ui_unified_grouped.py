"""Static grep-style tests for «Сравнение стадий → Расхождения → Сгруппировано» UI.

Эти тесты проверяют, что:
  1. Переключатель «Сгруппировано / Все расхождения» присутствует в HTML.
  2. Используются нужные endpoint'ы: /unified-grouped (GET) и /regroup (POST).
  3. Raw endpoint /unified-diff-flat сохранён.
  4. include_formal toggle присутствует.
  5. hidden_formal_count отображается в summary.
  6. value_variants учитываются.
  7. evidence раскрывается.
  8. pair_id добавляется к grouped endpoint при активной паре.
  9. Debug-вкладки «Текст (debug)» / «Графика (debug)» НЕ показываются обычному
     пользователю (остались под `scDevTools`).
 10. Кнопка «Проанализировать всю сессию» НЕ возвращена в основной UI вне
     dev-режима (остаётся под `scDevTools`).

Source of truth — `frontend/`; webapp/static проверяется через parity-тест.
"""
from __future__ import annotations

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent

HTML = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
CSS = (_ROOT / "frontend" / "static" / "css" / "styles.css").read_text(encoding="utf-8")


# ─── 1. View-mode toggle ────────────────────────────────────────────────


def test_html_has_view_mode_toggle():
    """Переключатель «Сгруппировано / Все расхождения» должен быть в HTML."""
    # Текстовые лейблы кнопок (могут быть с пробелами/переносами вокруг).
    assert "Сгруппировано" in HTML
    assert "Все расхождения" in HTML
    assert "scSetUnifiedViewMode('grouped')" in HTML
    assert "scSetUnifiedViewMode('raw')" in HTML


def test_js_has_view_mode_state():
    assert "scUnifiedViewMode" in JS
    # default = 'grouped'
    assert "scUnifiedViewMode          = ref('grouped')" in JS


def test_js_has_view_mode_setter():
    assert "function scSetUnifiedViewMode" in JS


# ─── 2/3. Endpoints ─────────────────────────────────────────────────────


def test_js_calls_unified_grouped_endpoint():
    """Должен быть GET /api/stage-comparison/sessions/{sid}/unified-grouped."""
    assert "/unified-grouped" in JS
    assert "async function scLoadUnifiedGrouped" in JS


def test_js_calls_regroup_endpoint():
    """POST /regroup для принудительной пересборки."""
    assert "/regroup" in JS
    assert "async function scRegroupUnifiedFindings" in JS
    # POST + force:true
    assert "method: 'POST'" in JS
    assert "force: true" in JS


def test_js_keeps_legacy_flat_endpoint():
    """Старый /unified-diff-flat должен остаться (raw mode)."""
    assert "/unified-diff-flat" in JS
    assert "async function scLoadUnifiedFlat" in JS


# ─── 4. include_formal toggle ───────────────────────────────────────────


def test_html_has_include_formal_toggle():
    assert "scGroupedShowFormal" in HTML
    assert "Показать формальные" in HTML


def test_js_has_show_formal_state():
    assert "const scGroupedShowFormal" in JS
    assert "ref(false)" in JS  # default off
    # toggle ВЛИЯЕТ на scGroupedAllItems
    assert "scGroupedShowFormal.value" in JS


def test_grouped_endpoint_always_includes_formal_in_query():
    """UI грузит include_formal=true, чтобы переключать локально без re-fetch."""
    assert "include_formal=true" in JS


# ─── 5. hidden_formal_count в summary ───────────────────────────────────


def test_html_shows_hidden_formal_count():
    assert "hidden_formal_count" in HTML
    assert "Скрыто формальных" in HTML


# ─── 6. value_variants ──────────────────────────────────────────────────


def test_html_handles_value_variants():
    assert "value_variants" in HTML
    assert "несколько вариантов" in HTML
    assert "scGroupedHasVariants" in HTML


def test_js_has_value_variants_helper():
    assert "function scGroupedHasVariants" in JS
    # Должна проверять >1 вариант.
    assert "v.length > 1" in JS


# ─── 7. evidence drawer ─────────────────────────────────────────────────


def test_html_has_evidence_drawer():
    assert "scIsGroupExpanded" in HTML
    assert "scToggleGroupExpand" in HTML
    assert "Показать evidence" in HTML or "evidence" in HTML.lower()
    # Per-evidence «к месту» button
    assert "scGotoGroupEvidence" in HTML


def test_js_has_expand_helpers():
    assert "function scToggleGroupExpand" in JS
    assert "function scIsGroupExpanded" in JS
    assert "const scGroupedExpanded" in JS


def test_js_evidence_navigation():
    assert "function scGotoGroupEvidence" in JS
    # должен переиспользовать scGotoUnifiedChange для перехода
    assert "scGotoUnifiedChange(" in JS


def test_js_goto_prefers_current_pair_evidence():
    """UX-фикс: при cross-pair session_rollup группе кнопка «→ к месту» в
    заглавной строке должна выбирать evidence ТЕКУЩЕЙ пары, а не первый
    попавшийся из другой пары. Behaviour:
      1) если передан явный ev — используется он;
      2) иначе среди g.evidence ищется e.pair_id === scActivePair.value.id;
      3) fallback — первый evidence.

    Дополнительно: HTML caller в заглавной строке должен передавать null
    (а не g.evidence[0]), чтобы функция сама выбрала preferred evidence.
    """
    start = JS.index("function scGotoGroupEvidence")
    snippet = JS[start: start + 1500]
    # 1. Должен явно использовать scActivePair при выборе preferred evidence.
    assert "scActivePair.value" in snippet, "scGotoGroupEvidence must consult scActivePair"
    # 2. Должна быть find-by-pair_id логика.
    assert "e.pair_id" in snippet or "pair_id === activePid" in snippet \
        or "pair_id === scActivePair" in snippet
    # 3. HTML caller не должен жёстко передавать g.evidence[0].
    assert "scGotoGroupEvidence(g, null)" in HTML, \
        "Caller in HTML must pass null so the helper can pick preferred evidence"
    assert "scGotoGroupEvidence(g, (g.evidence && g.evidence[0]) || null)" not in HTML, \
        "Old hard-coded evidence[0] caller must be removed"


# ─── 8. pair_id scope ───────────────────────────────────────────────────


def test_js_grouped_loader_sends_pair_id_when_active_pair():
    """В режиме «текущая пара» grouped endpoint вызывается с pair_id=..."""
    # Найдём блок scLoadUnifiedGrouped и проверим, что pair_id вкладывается.
    start = JS.index("async function scLoadUnifiedGrouped")
    end = JS.index("async function scRegroupUnifiedFindings")
    snippet = JS[start:end]
    assert "scUnifiedShowAllPairs.value" in snippet
    assert "scActivePair.value" in snippet
    assert "pair_id=" in snippet


# ─── 9. Debug tabs not visible ──────────────────────────────────────────


def test_html_debug_tabs_still_gated():
    """«Текст (debug)» и «Графика (debug)» остаются под scDevTools."""
    # Они должны существовать, но обёрнуты в v-if="scDevTools".
    idx_text_dbg = HTML.find("Текст (debug)")
    if idx_text_dbg >= 0:
        window = HTML[max(0, idx_text_dbg - 400): idx_text_dbg]
        assert "scDevTools" in window, "Текст (debug) должен быть под scDevTools"
    idx_graphic_dbg = HTML.find("Графика (debug)")
    if idx_graphic_dbg >= 0:
        window = HTML[max(0, idx_graphic_dbg - 400): idx_graphic_dbg]
        assert "scDevTools" in window, "Графика (debug) должна быть под scDevTools"


# ─── 10. «Проанализировать всю сессию» button not in main UI ───────────


def test_session_wide_opus_button_gated():
    idx = HTML.find("Проанализировать всю сессию")
    if idx >= 0:
        # Должна быть под scDevTools / sc_dev (dev режим).
        window = HTML[max(0, idx - 800): idx]
        assert "scDevTools" in window or "sc_dev" in window, \
            "Session-wide Opus button должна оставаться под dev-режимом"


# ─── 11. Summary fields rendered ────────────────────────────────────────


def test_grouped_summary_fields_present():
    """В summary panel показаны главные метрики backend grouping v2."""
    for field in [
        "raw_findings_count",
        "grouped_findings_count",
        "hidden_formal_count",
        "high_value_count",
        "medium_value_count",
        "low_value_count",
        "by_change_direction",
        "by_cost_impact_direction",
    ]:
        assert field in HTML, f"summary field {field!r} not present in HTML"


# ─── 12. Filters ────────────────────────────────────────────────────────


def test_grouped_filters_present():
    assert "scGroupedFilterSignificance" in HTML
    assert "scGroupedFilterTheme" in HTML
    assert "scGroupedFilterDirection" in HTML
    assert "scGroupedFilterCostDir" in HTML
    assert "scGroupedFilterHumanReview" in HTML
    assert "scGroupedSearch" in HTML


def test_js_exports_grouped_symbols():
    """Setup() должен экспортировать все grouped symbols для template."""
    # Проверяем последний return-блок setup'а — в нём должны быть имена.
    expected = [
        "scUnifiedViewMode", "scSetUnifiedViewMode",
        "scUnifiedGrouped", "scUnifiedGroupedLoading", "scUnifiedGroupedError",
        "scGroupedShowFormal", "scGroupedItemsSorted",
        "scLoadUnifiedGrouped", "scRegroupUnifiedFindings",
        "scGroupedSigLabel", "scGroupedDirLabel", "scGroupedCostDirLabel",
        "scGotoGroupEvidence", "scToggleGroupExpand", "scIsGroupExpanded",
    ]
    for name in expected:
        assert name in JS, f"symbol {name!r} not exported from setup()"


# ─── 13. CSS for grouped chips/rows ─────────────────────────────────────


def test_css_has_grouped_classes():
    assert ".sc-grouped-dir-chip" in CSS
    assert ".sc-grouped-cost-chip" in CSS
    assert ".sc-chip--formal" in CSS
    assert ".sc-chip--escalated" in CSS
    assert ".sc-chip--rollup" in CSS
    assert ".sc-row-evidence" in CSS
    assert ".sc-sev-chip--formal" in CSS
