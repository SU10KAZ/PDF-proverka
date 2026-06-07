"""
test_stage_comparison_v2_summary_labels.py
------------------------------------------
Регрессия для уточнения V2-счётчиков ревью в шапке «Расхождения».

Проблема: в шапке V2 «10 из 38» можно было прочитать как «принято + отклонено»,
хотя 10 = принято + отклонено + ИСКЛЮЧЕНО (автоматически отфильтрованные
формальные изменения). Понятия `обработано`, `экспертно решено`, `принято`,
`отклонено`, `исключено`, `не проверено` были не разведены.

Фикс — чисто frontend: новый хелпер `scV2ReviewProgress()` в
`frontend/static/js/app.js` + явные подписи в `frontend/index.html`.

Контракт (backend summary считает корректно, фронт только разводит подписи):

    total          = engineering_total + excluded_total
    processed       = confirmed + rejected + excluded
    expert_decided  = confirmed + rejected
    not_reviewed    = total - processed

«Принято/Отклонено» считаются строго по ИНЖЕНЕРНЫМ (не исключённым) строкам,
поэтому toggle «Показать формальные» (include_excluded) не двоит счётчик.

Тест двойной:
* static guard — новые подписи и хелпер присутствуют, хелпер проброшен в return;
* node-исполнение — реально гоняем `scV2ReviewProgress()` на канонических данных
  ИОС1.1 (confirmed=8, rejected=1, excluded=1, engineering_total=37) и проверяем
  числа + инвариант processed + not_reviewed == total + стабильность к toggle.

Run:
    python -m pytest tests/test_stage_comparison_v2_summary_labels.py -q
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
def app_js() -> str:
    assert _APP_JS.exists(), f"нет файла {_APP_JS}"
    return _APP_JS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def index_html() -> str:
    assert _INDEX.exists(), f"нет файла {_INDEX}"
    return _INDEX.read_text(encoding="utf-8")


# ─── Static guards ──────────────────────────────────────────────────────────

def test_helper_exists_and_exported(app_js: str):
    assert "function scV2ReviewProgress()" in app_js, "нет хелпера scV2ReviewProgress"
    # Проброшен в setup() return, иначе шаблон его не увидит.
    body = _extract_function(app_js, "scV2ReviewProgress")
    for key in ("processed", "expert_decided", "confirmed", "rejected",
                "excluded", "not_reviewed", "total"):
        assert key in body, f"хелпер не возвращает {key}"
    # В app.js имя встречается дважды: объявление функции + проброс в return
    # setup() (использование — в index.html, проверяется отдельно).
    assert app_js.count("scV2ReviewProgress") >= 2, (
        "scV2ReviewProgress должен быть объявлен и проброшен в return setup()"
    )


def test_helper_scopes_confirmed_rejected_to_engineering(app_js: str):
    body = _extract_function(app_js, "scV2ReviewProgress")
    # «Принято/Отклонено» — только по не исключённым строкам.
    assert "excluded_from_main" in body, (
        "confirmed/rejected должны скоупиться по инженерным (не excluded) строкам"
    )
    assert "review_status" in body
    # total и excluded берём из стабильного backend-summary.
    assert "excluded_total" in body
    assert "engineering_total" in body


def test_index_has_unambiguous_labels(index_html: str):
    for label in ("Обработано:", "Экспертно решено:", "Принято:",
                  "Отклонено:", "Исключено:", "Не проверено:"):
        assert label in index_html, f"в шапке V2 нет подписи «{label}»"
    # Шапка вызывает новый хелпер.
    assert "scV2ReviewProgress()" in index_html


# ─── Node execution of the real formula ─────────────────────────────────────

def _run_progress(app_js: str, summary: dict, items: list[dict]) -> dict:
    """Исполнить scV2ReviewProgress() в node на заданных данных."""
    node = shutil.which("node")
    if not node:  # окружение без node — static guards уже отработали
        pytest.skip("node недоступен")
    fn = _extract_function(app_js, "scV2ReviewProgress")
    harness = (
        f"const scV2Data = {{ value: {{ summary: {json.dumps(summary)}, "
        f"items: {json.dumps(items)} }} }};\n"
        f"{fn}\n"
        f"process.stdout.write(JSON.stringify(scV2ReviewProgress()));\n"
    )
    out = subprocess.run([node, "-e", harness], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, f"node error: {out.stderr}"
    return json.loads(out.stdout)


def _ios11_items() -> list[dict]:
    """37 инженерных строк ИОС1.1: 8 принято, 1 отклонено, 28 не проверено."""
    items = []
    for _ in range(8):
        items.append({"excluded_from_main": False, "review_status": "confirmed"})
    items.append({"excluded_from_main": False, "review_status": "rejected"})
    for _ in range(28):
        items.append({"excluded_from_main": False, "review_status": "not_reviewed"})
    return items


def test_canonical_ios11_default_view(app_js: str):
    summary = {"engineering_total": 37, "excluded_total": 1, "total": 37,
               "confirmed": 8, "rejected": 1, "not_reviewed": 28}
    r = _run_progress(app_js, summary, _ios11_items())
    assert r == {
        "total": 38,
        "processed": 10,
        "expert_decided": 9,
        "confirmed": 8,
        "rejected": 1,
        "excluded": 1,
        "not_reviewed": 28,
    }


def test_toggle_show_formal_does_not_double_count(app_js: str):
    """С include_excluded=true исключённая строка попадает в items; даже если
    её отметили confirmed, она НЕ должна попасть в «Принято» (она «Исключено»)."""
    items = _ios11_items()
    items.append({"excluded_from_main": True, "review_status": "confirmed"})
    # summary при include_excluded считает confirmed по всем 38 → 9, но фронт
    # обязан остаться на инженерных 8.
    summary = {"engineering_total": 37, "excluded_total": 1, "total": 38,
               "confirmed": 9, "rejected": 1, "not_reviewed": 28}
    r = _run_progress(app_js, summary, items)
    assert r["confirmed"] == 8, "исключённая confirmed-строка просочилась в «Принято»"
    assert r["excluded"] == 1
    assert r["processed"] == 10
    assert r["total"] == 38
    assert r["not_reviewed"] == 28


def test_invariant_processed_plus_not_reviewed_equals_total(app_js: str):
    summary = {"engineering_total": 12, "excluded_total": 3, "total": 12,
               "confirmed": 2, "rejected": 1, "not_reviewed": 9}
    items = ([{"excluded_from_main": False, "review_status": "confirmed"}] * 2
             + [{"excluded_from_main": False, "review_status": "rejected"}] * 1
             + [{"excluded_from_main": False, "review_status": "not_reviewed"}] * 9)
    r = _run_progress(app_js, summary, items)
    assert r["processed"] + r["not_reviewed"] == r["total"]
    assert r["expert_decided"] == r["confirmed"] + r["rejected"]
    assert r["processed"] == r["confirmed"] + r["rejected"] + r["excluded"]


def test_empty_summary_is_safe(app_js: str):
    r = _run_progress(app_js, {}, [])
    assert r == {"total": 0, "processed": 0, "expert_decided": 0,
                 "confirmed": 0, "rejected": 0, "excluded": 0, "not_reviewed": 0}
