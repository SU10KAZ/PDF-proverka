"""
test_users.py
-------------
Тесты раздела «Пользователи» (сотрудники-эксперты).

Покрывает:
* CRUD: список / добавление / переключение текущего / обновление / удаление;
* транслитерация фамилии в id и разрешение коллизий id;
* активность пользователя из decisions_log.json: матч по полному имени,
  группировка по проектам, счётчики findings/optimizations/accepted/rejected,
  игнор чужих reviewer'ов;
* REST-роутер /api/users (TestClient, auth выключен).

Run:
    python -m pytest tests/test_users.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.common.user_service as user_service  # noqa: E402


@pytest.fixture
def tmp_users(tmp_path, monkeypatch):
    """Изолированный файл users.json на время теста."""
    f = tmp_path / "users.json"
    monkeypatch.setattr(user_service, "USERS_FILE", f)
    return f


def test_crud_lifecycle(tmp_users):
    assert user_service.list_users() == []

    u1 = user_service.add_user("Узун", "А. И.", role="admin")
    assert u1["id"] == "uzun"
    assert u1["name"] == "Узун А. И."
    # первый добавленный становится текущим
    assert user_service.get_current_id() == "uzun"

    u2 = user_service.add_user("Гривапш", "А. А.")
    assert u2["id"] == "grivapsh"
    assert len(user_service.list_users()) == 2

    # переключение текущего
    user_service.switch_user("grivapsh")
    assert user_service.get_current_id() == "grivapsh"

    # обновление пересобирает name
    upd = user_service.update_user("grivapsh", initials="Б. Б.")
    assert upd["name"] == "Гривапш Б. Б."

    # удаление текущего сбрасывает current на первого оставшегося
    user_service.delete_user("grivapsh")
    assert user_service.get_current_id() == "uzun"
    assert len(user_service.list_users()) == 1


def test_id_collision_resolution(tmp_users):
    a = user_service.add_user("Иванов", "А. А.")
    b = user_service.add_user("Иванов", "Б. Б.")
    assert a["id"] == "ivanov"
    assert b["id"] == "ivanov_2"


def test_empty_surname_rejected(tmp_users):
    with pytest.raises(ValueError):
        user_service.add_user("   ")


def test_activity_attribution(tmp_users, monkeypatch):
    user_service.add_user("Узун", "А. И.", role="admin")

    log = [
        {"expert_reviewer": "Узун А. И.", "source_project": "EOM/ГРЩ", "section": "EOM",
         "item_id": "F-001", "item_type": "finding", "expert_decision": "accepted",
         "summary": "Нет селективности", "sheet": "7", "expert_date": "2026-05-29T10:00:00"},
        {"expert_reviewer": "Узун А. И.", "source_project": "EOM/ГРЩ", "section": "EOM",
         "item_id": "OPT-002", "item_type": "optimization", "expert_decision": "rejected",
         "summary": "Замена кабеля", "expert_reason": "дорого", "expert_date": "2026-05-29T11:00:00"},
        {"expert_reviewer": "Кто-то Д. Р.", "source_project": "OV/1",
         "item_id": "F-9", "item_type": "finding", "expert_decision": "accepted",
         "expert_date": "2026-05-28T09:00:00"},
    ]
    monkeypatch.setattr(user_service, "_load_decisions_log", lambda: log)

    act = user_service.get_user_activity("uzun")
    assert act["totals"] == {
        "projects": 1, "decisions": 2, "findings": 1,
        "optimizations": 1, "accepted": 1, "rejected": 1,
    }
    proj = act["projects"][0]
    assert proj["source_project"] == "EOM/ГРЩ"
    # items отсортированы по дате убыванию
    assert proj["items"][0]["item_id"] == "OPT-002"


def test_activity_matches_by_id_and_surname(tmp_users, monkeypatch):
    user_service.add_user("Репников", "И. А.")
    log = [
        {"expert_reviewer": "repnikov", "source_project": "P1", "item_id": "F-1",
         "item_type": "finding", "expert_decision": "accepted", "expert_date": "2026-05-01"},
        {"expert_reviewer": "Репников", "source_project": "P2", "item_id": "F-2",
         "item_type": "finding", "expert_decision": "rejected", "expert_date": "2026-05-02"},
    ]
    monkeypatch.setattr(user_service, "_load_decisions_log", lambda: log)
    act = user_service.get_user_activity("repnikov")
    assert act["totals"]["projects"] == 2
    assert act["totals"]["decisions"] == 2


def test_unknown_user_activity_raises(tmp_users):
    with pytest.raises(ValueError):
        user_service.get_user_activity("nobody")


def test_get_user_by_login(tmp_users):
    user_service.add_user("Узун", "А. И.", role="admin", login="uzun")
    user_service.add_user("Репников", "И. А.")  # login defaults to id
    assert (user_service.get_user_by_login("uzun") or {}).get("id") == "uzun"
    assert (user_service.get_user_by_login("UZUN") or {}).get("id") == "uzun"  # case-insensitive
    assert (user_service.get_user_by_login("repnikov") or {}).get("id") == "repnikov"  # via id fallback
    assert user_service.get_user_by_login("ghost") is None
    assert user_service.get_user_by_login(None) is None


def test_add_user_login_defaults_to_id(tmp_users):
    u = user_service.add_user("Оларь", "М. И.")
    assert u["login"] == u["id"] == "olar"
    u2 = user_service.add_user("Гривапш", "А. А.", login="griv_custom")
    assert u2["login"] == "griv_custom"


def test_router_active_user_follows_login(tmp_users, monkeypatch):
    """При включённой авторизации current_id = залогиненный сотрудник.

    Вызываем view-функцию напрямую (без middleware), подменяя резолв сессии.
    """
    import asyncio
    import backend.app.api.routers.users as users_router

    user_service.add_user("Узун", "А. И.", login="uzun")
    user_service.add_user("Репников", "И. А.", login="repnikov")
    # глобально активным остаётся uzun (первый)
    assert user_service.get_current_id() == "uzun"

    # сессия залогинена как repnikov; роутеру важен только .enabled
    monkeypatch.setattr(users_router, "_session_username", lambda request: "repnikov")
    monkeypatch.setattr(users_router.portal_auth, "get_settings",
                        lambda: SimpleNamespace(enabled=True))

    d = asyncio.run(users_router.list_users(request=SimpleNamespace()))
    assert d["current_id"] == "repnikov"          # активный = залогиненный, не глобальный uzun
    assert d["auth_enabled"] is True
    assert d["logged_in_matched"] is True

    # неизвестный логин → падаем обратно на глобальный current_id
    monkeypatch.setattr(users_router, "_session_username", lambda request: "ghost")
    d2 = asyncio.run(users_router.list_users(request=SimpleNamespace()))
    assert d2["current_id"] == "uzun"
    assert d2["logged_in_matched"] is False


def test_router_endpoints(tmp_users, monkeypatch):
    from fastapi.testclient import TestClient
    import backend.app.main as main

    client = TestClient(main.app)

    r = client.get("/api/users")
    assert r.status_code == 200
    base_count = len(r.json()["users"])

    r = client.post("/api/users", json={"surname": "Тестов", "initials": "Т. Т."})
    assert r.status_code == 200
    uid = r.json()["user"]["id"]
    assert uid == "testov"

    r = client.post("/api/users/switch", json={"id": uid})
    assert r.status_code == 200
    assert client.get("/api/users").json()["current_id"] == uid

    r = client.get(f"/api/users/{uid}/activity")
    assert r.status_code == 200
    assert "totals" in r.json()

    r = client.delete(f"/api/users/{uid}")
    assert r.status_code == 200
    assert len(client.get("/api/users").json()["users"]) == base_count
