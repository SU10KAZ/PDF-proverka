"""Счётчики объектов в переключателе шапки (GET /api/objects/stats).

Проверяем, что бэкенд считает те же два числа, что показывает строка «Итого»
на Главной выбранного объекта:

  * `not_started`  — «Не запускались на проверку» (ни замечаний, ни оптимизаций);
  * `no_decisions` — «Нет решений эксперта» (total − expert_checked).

Регрессия, которую закрывают тесты: счёт должен идти по УНИКАЛЬНЫМ проектам
(карточки-версии одного проекта — один проект, берётся последняя версия), иначе
цифра в выпадашке разъедется с цифрой на Главной после переключения объекта.

Запуск:
    python -m pytest tests/test_object_stats_picker.py -v
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.common import object_stats


def _card(pid, *, findings=0, opt=0, fstatus=None, ostatus=None,
          base=None, version_no=1):
    return {
        "project_id": pid,
        "base_project_key": base or pid,
        "version_no": version_no,
        "findings_count": findings,
        "optimization_count": opt,
        "findings_review_status": fstatus,
        "optimization_review_status": ostatus,
    }


@pytest.fixture(autouse=True)
def _clean_cache():
    object_stats.invalidate()
    yield
    object_stats.invalidate()


def _stub_cards(monkeypatch, mapping):
    monkeypatch.setattr(object_stats, "_object_cards",
                        lambda oid: list(mapping.get(oid, [])))


def _stub_objects(monkeypatch, ids):
    from backend.app.services.common import object_service
    monkeypatch.setattr(
        object_service, "list_objects",
        lambda: [{"id": i, "name": f"obj {i}"} for i in ids])


def test_not_started_counts_projects_without_any_audit_result(monkeypatch):
    _stub_cards(monkeypatch, {"o1": [
        _card("A"),                       # ни замечаний, ни оптимизаций
        _card("B", findings=3),           # аудит был
        _card("C", opt=2),                # только оптимизации — аудит был
    ]})
    s = object_stats.compute_object_stats("o1")
    assert s["total"] == 3
    assert s["not_started"] == 1


def test_no_decisions_includes_projects_without_audit(monkeypatch):
    _stub_cards(monkeypatch, {"o1": [
        _card("A"),                                            # без аудита
        _card("B", findings=3, fstatus="complete"),            # закрыт
        _card("C", findings=3),                                # вердиктов нет
    ]})
    s = object_stats.compute_object_stats("o1")
    assert s["expert_checked"] == 1
    assert s["no_decisions"] == 2


def test_empty_optimization_category_does_not_block_expert_check(monkeypatch):
    """Проект без оптимизаций закрывается одними замечаниями (их статус пуст)."""
    _stub_cards(monkeypatch, {"o1": [
        _card("A", findings=5, fstatus="complete", opt=0, ostatus=None),
    ]})
    assert object_stats.compute_object_stats("o1")["expert_checked"] == 1


def test_version_cards_counted_once_by_latest(monkeypatch):
    """«X» и «X V2» — один проект; статус берётся у последней версии."""
    _stub_cards(monkeypatch, {"o1": [
        _card("X", base="X", findings=4, fstatus="complete"),
        _card("X V2", base="X", version_no=2),   # свежая версия без аудита
    ]})
    s = object_stats.compute_object_stats("o1")
    assert s["total"] == 1
    assert s["not_started"] == 1      # последняя версия не проверялась
    assert s["expert_checked"] == 0


def test_broken_object_is_soft_failed(monkeypatch):
    def boom(_oid):
        raise RuntimeError("storage unavailable")
    monkeypatch.setattr(object_stats, "_object_cards", boom)
    s = object_stats.compute_object_stats("o1")
    assert s["error"] is True
    assert s["not_started"] == 0


def test_stats_are_cached_until_invalidated(monkeypatch):
    calls = []

    def counting(oid):
        calls.append(oid)
        return [_card("A")]

    _stub_objects(monkeypatch, ["o1"])
    monkeypatch.setattr(object_stats, "_object_cards", counting)
    object_stats.list_object_stats()
    object_stats.list_object_stats()
    assert len(calls) == 1            # второй вызов — из кеша
    object_stats.list_object_stats(force=True)
    assert len(calls) == 2


def test_endpoint_returns_stats_per_object(monkeypatch):
    _stub_objects(monkeypatch, ["o1", "o2"])
    _stub_cards(monkeypatch, {
        "o1": [_card("A"), _card("B", findings=2, fstatus="complete")],
        "o2": [_card("C", findings=1)],
    })
    resp = TestClient(app).get("/api/objects/stats")
    assert resp.status_code == 200
    stats = resp.json()["stats"]
    assert stats["o1"]["not_started"] == 1
    assert stats["o1"]["no_decisions"] == 1
    assert stats["o2"]["no_decisions"] == 1
    assert stats["o2"]["not_started"] == 0
