"""
test_missing_norms_kb.py
-------------------------
Regression coverage for KB tab «Нормы для добавления».

Контекст бага:
- В колонке «НОРМА» текст не виден на светлом фоне — `.mn-doc-number`
  использовал хардкод `color: #f1f5f9`, что в light-теме сливается с фоном.
  Фикс — `color: var(--text)`.
- Кнопка действия для pending-нормы была подписана «✓ Добавлена», что
  выглядело как статус «уже добавлена». Фикс — «+ Добавить».
- Проверяем также, что бэкенд корректно фильтрует pending / added /
  dismissed и не путает их между собой; счётчики stats не зависят от
  применённого фильтра.

Покрытие синхронно по backend (новый) и webapp (legacy).

Run:
    python -m pytest tests/test_missing_norms_kb.py -v
"""
from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


_SERVICE_MODULES = [
    "backend.app.services.knowledge_base.missing_norms_service",
    "webapp.services.missing_norms_service",
]


@pytest.fixture
def isolated_vault(tmp_path, monkeypatch):
    """Подменяет _STORE_PATH на временный файл для каждого теста."""
    stores: dict[str, Path] = {}
    for mod_path in _SERVICE_MODULES:
        mod = importlib.import_module(mod_path)
        store = tmp_path / f"{mod_path.replace('.', '_')}.json"
        monkeypatch.setattr(mod, "_STORE_PATH", store)
        stores[mod_path] = store
    return stores


def _seed(store_path: Path) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "norms": {
            "СП 100.13330.2020": {
                "doc_number": "СП 100.13330.2020",
                "family": "СП",
                "status": "pending",
                "added_to_vault_at": None,
                "first_seen_at": "2026-04-01T10:00:00",
                "last_seen_at": "2026-04-01T10:00:00",
                "occurrences": [
                    {"project_id": "P-1", "findings": ["F-1"], "seen_at": "2026-04-01T10:00:00"},
                ],
            },
            "ГОСТ 11.22.2021": {
                "doc_number": "ГОСТ 11.22.2021",
                "family": "ГОСТ",
                "status": "added",
                "added_to_vault_at": "2026-04-15T12:00:00",
                "first_seen_at": "2026-03-01T09:00:00",
                "last_seen_at": "2026-04-01T11:00:00",
                "occurrences": [
                    {"project_id": "P-2", "findings": ["F-7", "F-8"], "seen_at": "2026-03-01T09:00:00"},
                ],
            },
            "ВСН 11-77": {
                "doc_number": "ВСН 11-77",
                "family": "ВСН",
                "status": "dismissed",
                "added_to_vault_at": None,
                "first_seen_at": "2026-02-01T08:00:00",
                "last_seen_at": "2026-02-15T08:00:00",
                "occurrences": [
                    {"project_id": "P-3", "findings": ["F-99"], "seen_at": "2026-02-15T08:00:00"},
                ],
            },
        },
    }
    store_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_pending_filter_returns_only_pending(module_path, isolated_vault):
    """Фильтр 'pending' возвращает строго pending-нормы — никаких added/dismissed."""
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)
    norms = mod.get_missing_norms(status="pending")
    statuses = {n["status"] for n in norms}
    assert statuses == {"pending"}, f"pending-фильтр выдал смесь статусов: {statuses}"
    docs = {n["doc_number"] for n in norms}
    assert docs == {"СП 100.13330.2020"}


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_added_filter_returns_only_added(module_path, isolated_vault):
    """Фильтр 'added' возвращает только добавленные."""
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)
    norms = mod.get_missing_norms(status="added")
    assert {n["status"] for n in norms} == {"added"}
    assert {n["doc_number"] for n in norms} == {"ГОСТ 11.22.2021"}


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_dismissed_filter_excludes_pending(module_path, isolated_vault):
    """not_needed/dismissed-нормы не попадают в pending-выдачу."""
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)
    dismissed = mod.get_missing_norms(status="dismissed")
    assert {n["status"] for n in dismissed} == {"dismissed"}

    pending = mod.get_missing_norms(status="pending")
    pending_docs = {n["doc_number"] for n in pending}
    assert "ВСН 11-77" not in pending_docs, "dismissed норма не должна попадать в pending"


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_stats_independent_of_filter(module_path, isolated_vault):
    """get_stats() считает по статусам независимо: фильтр UI не должен путать счётчики."""
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)
    stats = mod.get_stats()
    assert stats == {"pending": 1, "added": 1, "dismissed": 1, "total": 3}


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_mark_added_moves_pending_to_added(module_path, isolated_vault):
    """После mark_added норма больше не должна возвращаться в pending-выдачу
    и должна попадать в added-фильтр (т.е. UI-строка не может одновременно
    быть pending и показывать «✓ Добавлена»)."""
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)

    assert mod.mark_added("СП 100.13330.2020") is True

    pending_docs = {n["doc_number"] for n in mod.get_missing_norms(status="pending")}
    added_docs = {n["doc_number"] for n in mod.get_missing_norms(status="added")}
    assert "СП 100.13330.2020" not in pending_docs
    assert "СП 100.13330.2020" in added_docs

    stats = mod.get_stats()
    assert stats["pending"] == 0
    assert stats["added"] == 2


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_mark_added_persists_across_reload(module_path, isolated_vault, monkeypatch):
    """Статус сохраняется в файл, а не только в памяти: после повторного
    чтения mark_added всё ещё видна как 'added'."""
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)
    mod.mark_added("СП 100.13330.2020")

    raw = json.loads(isolated_vault[module_path].read_text(encoding="utf-8"))
    entry = raw["norms"]["СП 100.13330.2020"]
    assert entry["status"] == "added"
    assert entry["added_to_vault_at"], "added_to_vault_at должен быть проставлен"


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_mark_pending_restores_status(module_path, isolated_vault):
    """Возврат added-нормы обратно в pending очищает added_to_vault_at."""
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)
    assert mod.mark_pending("ГОСТ 11.22.2021") is True
    raw = json.loads(isolated_vault[module_path].read_text(encoding="utf-8"))
    entry = raw["norms"]["ГОСТ 11.22.2021"]
    assert entry["status"] == "pending"
    assert entry["added_to_vault_at"] is None


# ─── UI-side regressions ──────────────────────────────────────────────────────

_INDEX_HTML_FILES = [
    _ROOT / "frontend" / "index.html",
    _ROOT / "webapp" / "static" / "index.html",
]
_CSS_FILES = [
    _ROOT / "frontend" / "static" / "css" / "styles.css",
    _ROOT / "webapp" / "static" / "css" / "styles.css",
]


@pytest.mark.parametrize("css_path", _CSS_FILES)
def test_mn_doc_number_uses_theme_color(css_path):
    """Текст нормы должен наследоваться от темы (var(--text)), а не быть
    хардкоженным `#f1f5f9` — иначе он не виден в light-теме."""
    text = css_path.read_text(encoding="utf-8")
    assert ".mn-doc-number" in text, f"{css_path}: класс .mn-doc-number отсутствует"
    # Найти именно строку с .mn-doc-number
    line = next(ln for ln in text.splitlines() if ".mn-doc-number" in ln and "color:" in ln)
    assert "var(--text)" in line, (
        f"{css_path}: .mn-doc-number должен использовать var(--text), "
        f"иначе ломается контраст в light-теме. Got: {line}"
    )
    assert "#f1f5f9" not in line, (
        f"{css_path}: хардкод color: #f1f5f9 на .mn-doc-number ломает light-тему"
    )


@pytest.mark.parametrize("html_path", _INDEX_HTML_FILES)
def test_pending_action_button_label_is_action_not_status(html_path):
    """Кнопка для pending-нормы должна выглядеть как действие («+ Добавить»),
    а не как статус «✓ Добавлена», иначе пользователь видит pending-строку
    с псевдо-статусом «уже добавлена»."""
    text = html_path.read_text(encoding="utf-8")
    # Найти блок с markNormAdded
    idx = text.find("markNormAdded(norm.doc_number)")
    assert idx > 0, f"{html_path}: обработчик markNormAdded не найден"
    # Контекст вокруг (button → /button)
    window = text[idx: idx + 400]
    assert "✓ Добавлена" not in window, (
        f"{html_path}: pending-кнопка не должна быть подписана «✓ Добавлена» — "
        "это сбивает с толку (выглядит как статус, а не действие)."
    )
    assert "Добавить" in window, (
        f"{html_path}: ожидаем глагол «Добавить» в подписи действия. Window: {window!r}"
    )
