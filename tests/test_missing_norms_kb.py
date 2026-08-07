"""Regression-тесты простого реестра отсутствующих действующих норм."""
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
]


def _resolved(*, found: bool, status: str = "unknown", reason: str = "not_in_index") -> dict:
    return {
        "found": found,
        "status": status,
        "resolution_reason": reason,
        "supported_family": reason == "not_in_index",
    }


@pytest.fixture
def isolated_vault(tmp_path, monkeypatch):
    """Подменяет файл и нормативный resolver для каждого теста."""
    stores: dict[str, Path] = {}
    for mod_path in _SERVICE_MODULES:
        mod = importlib.import_module(mod_path)
        store = tmp_path / f"{mod_path.replace('.', '_')}.json"
        rules = tmp_path / f"{mod_path.replace('.', '_')}_review_rules.json"
        rules.write_text('{"normalizations": {}, "excluded": []}', encoding="utf-8")
        monkeypatch.setattr(mod, "_STORE_PATH", store)
        monkeypatch.setattr(mod, "_REVIEW_RULES_PATH", rules)

        def fake_resolve(doc: str) -> dict:
            if doc.startswith("БАЗА "):
                return _resolved(found=True, status="active", reason="exact")
            if doc.startswith("ОТМЕНЕНА "):
                return _resolved(found=True, status="cancelled", reason="manual_override")
            if doc.startswith("ЗАМЕНЕНА "):
                return _resolved(found=True, status="replaced", reason="manual_override")
            if doc.startswith("НЕПОДДЕРЖИВАЕМАЯ "):
                return _resolved(found=False, reason="unsupported_family")
            return _resolved(found=False)

        monkeypatch.setattr(mod, "resolve_norm_status", fake_resolve)
        stores[mod_path] = store
    return stores


def _seed(store_path: Path) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    store_path.write_text(
        json.dumps(["СП 100.13330.2020", "ГОСТ 11.22.2021"], ensure_ascii=False),
        encoding="utf-8",
    )


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_store_contains_only_norm_strings(module_path, isolated_vault):
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)

    assert mod.reconcile_missing_norms() == 2
    raw = json.loads(isolated_vault[module_path].read_text(encoding="utf-8"))
    assert raw == ["ГОСТ 11.22.2021", "СП 100.13330.2020"]
    assert all(isinstance(item, str) for item in raw)


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_only_pending_filter_has_entries(module_path, isolated_vault):
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)

    pending = mod.get_missing_norms(status="pending")
    assert {item["doc_number"] for item in pending} == {
        "СП 100.13330.2020",
        "ГОСТ 11.22.2021",
    }
    assert {item["status"] for item in pending} == {"pending"}
    assert mod.get_missing_norms(status="added") == []
    assert mod.get_missing_norms(status="dismissed") == []


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_stats_only_count_active_missing_norms(module_path, isolated_vault):
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)
    assert mod.get_stats() == {"pending": 2, "added": 0, "dismissed": 0, "total": 2}


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_reconcile_removes_known_cancelled_replaced_and_unsupported(module_path, isolated_vault):
    store = isolated_vault[module_path]
    store.write_text(json.dumps([
        "СП 100.13330.2020",
        "БАЗА ГОСТ 1",
        "ОТМЕНЕНА ГОСТ 2",
        "ЗАМЕНЕНА СП 3",
        "НЕПОДДЕРЖИВАЕМАЯ НОРМА",
    ], ensure_ascii=False), encoding="utf-8")
    mod = importlib.import_module(module_path)

    assert mod.reconcile_missing_norms() == 1
    assert json.loads(store.read_text(encoding="utf-8")) == ["СП 100.13330.2020"]


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_accumulate_accepts_only_missing_non_cancelled(module_path, isolated_vault, tmp_path):
    _seed(isolated_vault[module_path])
    queue = tmp_path / "missing_norms_queue.json"
    queue.write_text(json.dumps({"queue": [
        {"norm": "СП 200.13330.2024", "action": "add_document_to_vault"},
        {"norm": "БАЗА ГОСТ 1", "action": "add_document_to_vault"},
        {"norm": "ОТМЕНЕНА ГОСТ 2", "action": "add_document_to_vault"},
        {"norm": "СП 300.13330.2024", "status": "cancelled"},
        {"norm": "НЕПОДДЕРЖИВАЕМАЯ НОРМА", "action": "review_family_support"},
    ]}, ensure_ascii=False), encoding="utf-8")
    mod = importlib.import_module(module_path)

    assert mod.accumulate_from_queue("PROJECT-1", queue) == 1
    raw = json.loads(isolated_vault[module_path].read_text(encoding="utf-8"))
    assert raw == ["ГОСТ 11.22.2021", "СП 100.13330.2020", "СП 200.13330.2024"]


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_reviewed_errors_are_normalized_or_excluded(
    module_path, isolated_vault, tmp_path, monkeypatch
):
    mod = importlib.import_module(module_path)
    rules = tmp_path / "review_rules.json"
    rules.write_text(json.dumps({
        "normalizations": {"СП 10.1330.2020": "СП 10.13330.2020"},
        "excluded": ["ГОСТ ОШИБОЧНЫЙ"],
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(mod, "_REVIEW_RULES_PATH", rules)

    store = isolated_vault[module_path]
    store.write_text(json.dumps([
        "СП 10.1330.2020",
        "ГОСТ ОШИБОЧНЫЙ",
    ], ensure_ascii=False), encoding="utf-8")

    assert mod.reconcile_missing_norms() == 1
    assert json.loads(store.read_text(encoding="utf-8")) == ["СП 10.13330.2020"]

    queue = tmp_path / "missing_norms_queue.json"
    queue.write_text(json.dumps({"queue": [
        {"norm": "СП 10.1330.2020"},
        {"norm": "ГОСТ ОШИБОЧНЫЙ"},
    ]}, ensure_ascii=False), encoding="utf-8")
    assert mod.accumulate_from_queue("PROJECT-1", queue) == 0
    assert json.loads(store.read_text(encoding="utf-8")) == ["СП 10.13330.2020"]


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_mark_added_and_dismissed_remove_norms(module_path, isolated_vault):
    _seed(isolated_vault[module_path])
    mod = importlib.import_module(module_path)

    assert mod.mark_added("СП 100.13330.2020") is True
    assert mod.mark_dismissed("ГОСТ 11.22.2021") is True
    assert mod.mark_pending("СП 100.13330.2020") is False
    assert json.loads(isolated_vault[module_path].read_text(encoding="utf-8")) == []


@pytest.mark.parametrize("module_path", _SERVICE_MODULES)
def test_legacy_schema_is_migrated_without_metadata(module_path, isolated_vault):
    store = isolated_vault[module_path]
    store.write_text(json.dumps({
        "version": 1,
        "norms": {
            "СП 100.13330.2020": {"doc_number": "СП 100.13330.2020", "status": "pending"},
            "БАЗА ГОСТ 1": {"doc_number": "БАЗА ГОСТ 1", "status": "pending"},
            "ОТМЕНЕНА ГОСТ 2": {"doc_number": "ОТМЕНЕНА ГОСТ 2", "status": "dismissed"},
        },
    }, ensure_ascii=False), encoding="utf-8")
    mod = importlib.import_module(module_path)

    assert mod.reconcile_missing_norms() == 1
    assert json.loads(store.read_text(encoding="utf-8")) == ["СП 100.13330.2020"]


# ─── UI-side regressions ──────────────────────────────────────────────────────

_INDEX_HTML_FILES = [
    _ROOT / "frontend" / "index.html",
]
_CSS_FILES = [
    _ROOT / "frontend" / "static" / "css" / "styles.css",
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
