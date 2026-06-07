"""
test_stage_comparison_v2_summary_scope.py
-----------------------------------------
Регрессия для бага «завышенный счётчик экспертных решений в шапке V2».

Проблема (ИОС1.1): шапка V2 «Принято/Отклонено» показывала 26/4 = 30, хотя
honest backend-бейдж `_per_pair_status` давал «размечено 10 из 38». Причина —
`scExpertReviewSummary()` в V2-режиме НЕ скоупил решения по текущим изменениям
и считал осиротевшие после регенерации сравнения expert-решения (id вида
`v2_<hash>` — контент-хеши, меняются при регене).

Фикс: V2-ветка `scExpertReviewSummary` считает строго по текущим загруженным
изменениям пары (`scV2Data.items`) — по живому экспертному клику (ключ =
текущий `pid::item.id`) ИЛИ каноническому `review_status`. Сироты в счётчик
больше не попадают.

Это чисто frontend-фикс, поэтому guard статический: парсим тело функции в
`frontend/static/js/app.js` и проверяем, что V2-ветка скоупится по текущим
items/review_status, а старый unscoped-паттерн ушёл. Backend не менялся —
`_per_pair_status` уже считал правильно.

Run:
    python -m pytest tests/test_stage_comparison_v2_summary_scope.py -q
"""
from __future__ import annotations

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_APP_JS = _ROOT / "frontend" / "static" / "js" / "app.js"


def _extract_function(src: str, name: str) -> str:
    """Вернуть тело `function <name>() { ... }` по балансу фигурных скобок."""
    marker = f"function {name}()"
    start = src.find(marker)
    assert start != -1, f"{name} не найдена в app.js"
    brace = src.find("{", start)
    assert brace != -1, f"открывающая {{ для {name} не найдена"
    depth = 0
    for i in range(brace, len(src)):
        ch = src[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return src[start : i + 1]
    raise AssertionError(f"не закрылась функция {name}")


@pytest.fixture(scope="module")
def summary_fn() -> str:
    assert _APP_JS.exists(), f"нет файла {_APP_JS}"
    return _extract_function(_APP_JS.read_text(encoding="utf-8"), "scExpertReviewSummary")


def test_v2_branch_exists(summary_fn: str):
    # V2-ветка выделена явным if по режиму.
    assert "scV2View.value === 'v2'" in summary_fn


def test_v2_scoped_to_current_changes(summary_fn: str):
    # V2-счёт идёт по текущим загруженным изменениям пары и каноническому
    # review_status, а не по всем expert-решениям сессии.
    assert "scV2Data.value" in summary_fn, "V2-ветка должна скоупиться по scV2Data.items"
    assert "review_status" in summary_fn, "V2-ветка должна учитывать канонический review_status"


def test_old_unscoped_pattern_removed(summary_fn: str):
    # Старый баг: один общий проход с `viewOk = v2view ? raw.startsWith('v2_') ...`
    # БЕЗ скоупа по текущим строкам (known === null в V2). Этот паттерн должен уйти.
    assert "v2view ? raw.startsWith('v2_')" not in summary_fn, (
        "вернулся unscoped V2-паттерн — сироты снова накручивают счётчик"
    )


def test_classic_path_preserved(summary_fn: str):
    # Классический вид «Расхождения» по-прежнему скоупится через _scSummaryKnownKeys.
    assert "_scSummaryKnownKeys()" in summary_fn
    assert "_scExpertDecisionsForActivePair()" in summary_fn


def test_returns_accepted_rejected_total(summary_fn: str):
    for key in ("accepted", "rejected", "total"):
        assert key in summary_fn
