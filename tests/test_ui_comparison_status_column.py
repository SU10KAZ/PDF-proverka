"""Static smoke: колонка «Сравнение» переживает refresh и показывает режим.

Баг: после object-autoselect refresh (scTryAutoLoadSession минует scLoadSession)
не грузились scPairCompareStatus → у реально сравнённых пар колонка «Сравнение»
показывала «—», а индикация особого режима (fallback) пропадала.

Фикс:
* scTryAutoLoadSession теперь зовёт scLoadPairCompareStatuses();
* backend comparison-statuses отдаёт mode/present_one_side/requires_human_review;
* бейдж «✓ сравнено» НЕ зависит от экспертной проверки («Проверено 0/N»).
"""
from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
JS = (_ROOT / "frontend" / "static" / "js" / "app.js").read_text(encoding="utf-8")
HTML = (_ROOT / "frontend" / "index.html").read_text(encoding="utf-8")


def _fn(name: str, end_marker: str) -> str:
    i = JS.index(name)
    j = JS.index(end_marker, i + len(name))
    return JS[i:j]


def test_autoload_path_loads_compare_statuses():
    # object-autoselect refresh путь должен сам подтянуть статусы сравнения.
    sc = JS[JS.index("async function scTryAutoLoadSession("):]
    sc = sc[: sc.index("async function ", 1)]
    assert "scLoadPairCompareStatuses()" in sc


def test_done_badge_says_compared():
    fn = _fn("function scOpusBadgeFromRecord(", "function scRecogElapsedLabel(")
    assert "'✓ сравнено'" in fn


def test_done_badge_has_fallback_suffix():
    fn = _fn("function scOpusBadgeFromRecord(", "function scRecogElapsedLabel(")
    assert "(fallback)" in fn


def test_comparison_badge_independent_of_expert_review():
    # бейдж сравнения строится из статуса сравнения, не из экспертной разметки.
    fn = _fn("function scOpusPairBadge(", "function scOpusBadgeFromRecord(")
    rec = _fn("function scOpusBadgeFromRecord(", "function scRecogElapsedLabel(")
    for forbidden in ("scExpert", "expert_review", "v2_review", "scV2"):
        assert forbidden not in fn, forbidden
        assert forbidden not in rec, forbidden


def test_badge_passes_mode_and_counts():
    fn = _fn("function scOpusPairBadge(", "function scOpusBadgeFromRecord(")
    assert "_present_one_side: persisted.present_one_side_count" in fn
    assert "_requires_human_review: persisted.requires_human_review_count" in fn
    assert "_mode: persisted.mode" in fn


def test_missing_returns_dash():
    fn = _fn("function scOpusPairBadge(", "function scOpusBadgeFromRecord(")
    # последняя ветка — «не запускалось» → «—».
    assert "'—'" in fn
    assert "сравнение не запускалось" in fn


def test_comparison_column_uses_badge_not_review():
    # колонка «Сравнение» в index.html рендерится через scOpusPairBadge.
    assert "scOpusPairBadge(p.id)" in HTML
