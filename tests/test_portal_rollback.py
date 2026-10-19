"""Откат (план доступа, поправка 0.6): не возвращает отозванные cookie, старые
пароли, доступ заблокированных и прежние права viewer. Временные данные."""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from backend.app.core import portal_auth
from backend.app.services.accounts import db, store
import backend.app.services.common.user_service as user_service

H = {"X-Requested-With": "AuditManager"}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setenv("PORTAL_AUTH_MODE", "accounts")
    monkeypatch.setenv("PORTAL_ACCOUNTS_DB", str(tmp_path / "acc.sqlite3"))
    monkeypatch.setenv("PORTAL_PASSWORD_ROUNDS", "1000")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "secret-A-" + "a" * 40)
    monkeypatch.setenv("PORTAL_SESSION_COOKIE_NAME", "am_session")
    monkeypatch.setenv("PORTAL_COOKIE_SECURE", "false")
    # прежняя строка env_users с СТАРЫМИ паролями всех сотрудников
    monkeypatch.setenv("PORTAL_AUTH_USERS", ",".join(
        f"{u}:{portal_auth.hash_password('old-' + u)}" for u in ("owner", "kulik", "pavel")))
    db.reset_for_tests()
    users = tmp_path / "users.json"
    users.write_text(json.dumps({"users": [{"id": "boss", "login": "owner", "name": "Владелец В.В."},
                                           {"id": "kulik", "login": "kulik", "name": "Кулик А.С."}]},
                                ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(user_service, "USERS_FILE", users)
    store.create_account(login="owner", display_name="Владелец", role="admin", password="owner-new-password", staff_id="boss")
    viewer = store.create_account(login="kulik", display_name="Кулик", role="viewer", password="kulik-new-password",
                                  staff_id="kulik", sections={"audit_results": {"view": True, "download": False}})
    blocked = store.create_account(login="pavel", display_name="Павел", role="viewer", password="pavel-new-password")
    store.block(None, blocked["id"])
    yield {"viewer": viewer}
    db.reset_for_tests()


@pytest.fixture(scope="module")
def app():
    from backend.app.main import app as _app
    return _app


def _login(app, login, pw):
    c = TestClient(app, headers=H)
    return c, c.post("/api/auth/login", json={"username": login, "password": pw})


def test_emergency_mode_then_back_to_accounts(env, app, monkeypatch):
    owner_c, _ = _login(app, "owner", "owner-new-password")
    viewer_c, _ = _login(app, "kulik", "kulik-new-password")
    assert viewer_c.get("/api/auth/me").json()["authenticated"]

    # 1. Аварийный режим: новый секрет, новое имя cookie, только владелец, НОВЫЙ пароль.
    monkeypatch.setenv("PORTAL_AUTH_MODE", "emergency_owner")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "secret-E-" + "e" * 40)
    monkeypatch.setenv("PORTAL_SESSION_COOKIE_NAME", "am_emergency")
    monkeypatch.setenv("PORTAL_EMERGENCY_OWNER", "owner:" + portal_auth.hash_password("emergency-pass-1"))
    for c in (owner_c, viewer_c):                                   # прежние cookie не действуют
        assert c.get("/api/projects").status_code == 401
    for login, pw in (("kulik", "old-kulik"), ("kulik", "kulik-new-password"), ("pavel", "old-pavel"),
                      ("owner", "old-owner"), ("owner", "owner-new-password")):
        assert _login(app, login, pw)[1].status_code == 401, (login, pw)   # ни старые, ни из БД
    em_c, r = _login(app, "owner", "emergency-pass-1")
    assert r.status_code == 200
    reg = TestClient(app, headers=H).post("/api/auth/register", json={
        "login": "newbie", "display_name": "Новый Н.Н.", "password": "long-password-1"})
    assert reg.status_code == 403

    # 2. Возврат в accounts с ЕЩЁ одним новым секретом: аварийная cookie не действует,
    #    блокировка и права viewer сохранены в БД.
    monkeypatch.setenv("PORTAL_AUTH_MODE", "accounts")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "secret-B-" + "b" * 40)
    monkeypatch.setenv("PORTAL_SESSION_COOKIE_NAME", "am_session")
    assert em_c.get("/api/projects").status_code == 401
    for c in (owner_c, viewer_c):                                   # и cookie до отката тоже
        assert c.get("/api/projects").status_code == 401
    assert _login(app, "pavel", "pavel-new-password")[1].json()["detail"]["code"] == "account_blocked"
    assert _login(app, "kulik", "old-kulik")[1].status_code == 401
    vc, r = _login(app, "kulik", "kulik-new-password")
    me = vc.get("/api/auth/me").json()["account"]
    assert me["role"] == "viewer" and me["sections"]["audit_results"] == {"view": True, "download": False}
    assert me["sections"]["stage_comparison"]["view"] is False


def test_env_users_mode_cannot_resurrect_old_secret_cookies(env, app, monkeypatch):
    """Даже если вернуть режим env_users, cookie, подписанные другим секретом и
    под другим именем, не принимаются (секрет и имя меняются при откате)."""
    owner_c, _ = _login(app, "owner", "owner-new-password")
    monkeypatch.setenv("PORTAL_AUTH_MODE", "env_users")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "secret-R-" + "r" * 40)
    monkeypatch.setenv("PORTAL_SESSION_COOKIE_NAME", "am_rollback")
    monkeypatch.setenv("PORTAL_AUTH_USERS", "owner:" + portal_auth.hash_password("rollback-owner-pass"))
    assert owner_c.get("/api/projects").status_code == 401
    assert _login(app, "kulik", "old-kulik")[1].status_code == 401     # остальных в строке нет
    assert _login(app, "owner", "rollback-owner-pass")[1].status_code == 200
