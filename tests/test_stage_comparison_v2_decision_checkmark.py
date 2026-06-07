"""
test_stage_comparison_v2_decision_checkmark.py
----------------------------------------------
Регрессия: V2 summary показывал «Принято: 8», а в колонке «Решение» галочек
было меньше (pf06effb7).

Причина — UI mapping: колонка «Решение» рисовала ✓/✗ ТОЛЬКО через
`scGetExpertDecision(it)`, который ищет решение по ключу `pid::item.id`
(`pid::v2_<hash>`). Часть решений в legacy `expert_review.json` хранится под
ключом `pid::raw_id` (`pid::chg_...`). Backend `review_status` резолвит их по
стабильному raw_id (и summary считает по нему), а фронт под `pid::v2_id` решения
не находил → строка confirmed без галочки.

Фикс — чисто frontend: display-only хелпер `scResolvedDecision()` (явное
expert-решение, иначе fallback на `review_status`). Колонка «Решение» V2 рисует
✓/✗ через него. ВАЖНО: `scGetExpertDecision` НЕ меняется — он остаётся источником
для логики редактирования (`scSetExpertDecision` toggle), иначе первый клик по
«унаследованной» строке снял бы отметку вместо подтверждения.

Двойной тест:
* static guard — хелпер есть/экспортирован; V2-ячейка «Решение» использует
  scResolvedDecision; scGetExpertDecision НЕ содержит review_status (toggle-safe);
  scSetExpertDecision по-прежнему опирается на scGetExpertDecision;
* node-исполнение — поведение fallback'а (явное решение приоритетно, mixed
  сохраняется, при отсутствии — review_status, иначе null).

Run:
    python -m pytest tests/test_stage_comparison_v2_decision_checkmark.py -q
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_APP_JS = _ROOT / "frontend" / "static" / "js" / "app.js"
_INDEX = _ROOT / "frontend" / "index.html"


def _extract_function(src: str, name: str) -> str:
    marker = f"function {name}("
    start = src.find(marker)
    assert start != -1, f"{name} не найдена"
    brace = src.find("{", start)
    depth = 0
    for i in range(brace, len(src)):
        ch = src[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return src[start : i + 1]
    raise AssertionError(f"не закрылась {name}")


@pytest.fixture(scope="module")
def app_js() -> str:
    return _APP_JS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def index_html() -> str:
    return _INDEX.read_text(encoding="utf-8")


# ─── Static guards ──────────────────────────────────────────────────────────

def test_helper_defined_and_exported(app_js: str):
    assert "function scResolvedDecision(" in app_js
    assert "scResolvedDecision," in app_js, "scResolvedDecision не проброшен в return"


def test_scGetExpertDecision_stays_explicit_only(app_js: str):
    # Edit-логика (toggle) обязана видеть ТОЛЬКО явные клики. Если в
    # scGetExpertDecision просочится review_status, первый клик по
    # унаследованной строке снимет отметку.
    body = _extract_function(app_js, "scGetExpertDecision")
    assert "review_status" not in body, (
        "scGetExpertDecision не должен зависеть от review_status (ломает toggle)"
    )


def test_scSetExpertDecision_uses_explicit_resolver(app_js: str):
    # current для toggle берётся из scGetExpertDecision, НЕ из scResolvedDecision.
    body = _extract_function(app_js, "scSetExpertDecision")
    assert "scGetExpertDecision(" in body
    assert "scResolvedDecision(" not in body, (
        "scSetExpertDecision не должен использовать display-резолвер (двойной клик)"
    )


def test_v2_decision_column_uses_resolved(index_html: str):
    # В блоке V2 (sc-v2, до классического вида) колонка «Решение» рисует ✓/✗
    # через scResolvedDecision. Берём срез V2-блока до классического (scV2View!=='v2').
    v2_start = index_html.find('class="sc-v2"')
    classic = index_html.find("scV2View!=='v2'")
    assert 0 < v2_start < classic
    v2 = index_html[v2_start:classic]
    # 4 кнопочных выражения + 2 в reason-cell.
    assert v2.count("scResolvedDecision(it) === 'accepted'") >= 2
    assert v2.count("scResolvedDecision(it) === 'rejected'") >= 2
    assert "v-if=\"scResolvedDecision(it)\"" in v2, "reason-cell V2 не на scResolvedDecision"
    # Клик-хендлер остаётся на scSetExpertDecision (edit-логика).
    assert "scSetExpertDecision(it, 'accepted')" in v2


def test_classic_view_untouched(index_html: str):
    # Классический dev/debug-вид (scV2View!=='v2') не трогаем — он на
    # scGetExpertDecision как и раньше.
    classic = index_html[index_html.find("scV2View!=='v2'"):]
    assert "scGetExpertDecision(it) === 'accepted'" in classic
    assert "scResolvedDecision(" not in classic, "классический вид не должен меняться"


# ─── Node execution of the resolver ─────────────────────────────────────────

def _resolved(app_js: str, stub_decision: str, item: dict):
    node = shutil.which("node")
    if not node:
        pytest.skip("node недоступен")
    fn = _extract_function(app_js, "scResolvedDecision")
    harness = (
        f"function scGetExpertDecision(){{ return {stub_decision}; }}\n"
        f"{fn}\n"
        f"process.stdout.write(JSON.stringify(scResolvedDecision({json.dumps(item)})));\n"
    )
    out = subprocess.run([node, "-e", harness], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_explicit_decision_wins_over_status(app_js: str):
    # #13-кейс: явное решение под pid::v2_id = accepted, а review_status — другой;
    # display обязан показать явное решение.
    assert _resolved(app_js, '"accepted"', {"review_status": "rejected"}) == "accepted"
    assert _resolved(app_js, '"rejected"', {"review_status": "confirmed"}) == "rejected"


def test_mixed_preserved(app_js: str):
    assert _resolved(app_js, '"mixed"', {"review_status": "confirmed"}) == "mixed"


def test_fallback_to_review_status(app_js: str):
    # #4/#6-кейс: явного решения под pid::v2_id нет → падаем на review_status.
    assert _resolved(app_js, "null", {"review_status": "confirmed"}) == "accepted"
    assert _resolved(app_js, "null", {"review_status": "rejected"}) == "rejected"


def test_no_decision_no_status_is_null(app_js: str):
    assert _resolved(app_js, "null", {"review_status": "not_reviewed"}) is None
    assert _resolved(app_js, "null", {}) is None
