"""Режим аккаунтов портала (план доступа, этап 2): вход, статусы, сессии,
регистрация, смена пароля, защита изменяющих запросов, журнал, аварийный режим.
Только временная БД аккаунтов и временный users.json.
"""
from __future__ import annotations

import asyncio
import json

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from backend.app.core import access, portal_auth
from backend.app.services.accounts import db, store
import backend.app.services.common.user_service as user_service

H = {"X-Requested-With": "AuditManager"}
OWNER_PW = "owner-password-1"
VIEWER_PW = "viewer-password-1"


@pytest.fixture
def accounts_env(tmp_path, monkeypatch):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setenv("PORTAL_AUTH_MODE", "accounts")
    monkeypatch.setenv("PORTAL_ACCOUNTS_DB", str(tmp_path / "acc.sqlite3"))
    monkeypatch.setenv("PORTAL_PASSWORD_ROUNDS", "1000")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "accounts-secret-" + "z" * 40)
    monkeypatch.setenv("PORTAL_COOKIE_SECURE", "false")
    monkeypatch.setenv("PORTAL_REGISTRATION_ENABLED", "true")
    monkeypatch.delenv("PORTAL_AUTH_USERS", raising=False)
    db.reset_for_tests()
    users = tmp_path / "users.json"
    users.write_text(json.dumps({"users": [
        {"id": "boss", "login": "boss", "name": "Босс Б.Б.", "role": "admin"},
        {"id": "kulik", "login": "kulik", "name": "Кулик А. С.", "role": "expert"},
    ], "current_id": "kulik"}, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(user_service, "USERS_FILE", users)
    owner = store.create_account(login="owner", display_name="Владелец", role="admin",
                                 password=OWNER_PW, staff_id="boss")
    viewer = store.create_account(login="kulik", display_name="Кулик", role="viewer",
                                  password=VIEWER_PW, staff_id="kulik",
                                  sections={"audit_results": {"view": True, "download": False}})
    yield {"owner": owner, "viewer": viewer}
    db.reset_for_tests()


@pytest.fixture(scope="module")
def app():
    from backend.app.main import app as _app
    return _app


def _login(app, login, password):
    c = TestClient(app, headers=H)
    r = c.post("/api/auth/login", json={"username": login, "password": password})
    return c, r


def test_login_and_me_states(accounts_env, app):
    anon = TestClient(app).get("/api/auth/me").json()
    assert anon["authenticated"] is False and "reason" not in anon
    c, r = _login(app, "OWNER", OWNER_PW)                    # регистр логина не важен
    assert r.status_code == 200 and r.json()["authenticated"] is True
    me = c.get("/api/auth/me").json()
    assert me["authenticated"] and me["account"]["role"] == "admin"
    assert OWNER_PW not in json.dumps(me) and "pbkdf2" not in json.dumps(me)
    assert me["account"]["sections"]["schedule"] == {"view": True, "download": False}
    store.revoke_sessions(None, accounts_env["owner"]["id"])  # «CLI»: правка вне процесса API
    revoked = c.get("/api/auth/me")
    assert revoked.json() == {**revoked.json(), "authenticated": False, "reason": "session_revoked"}
    assert "portal_session" in revoked.headers.get("set-cookie", "")   # cookie удаляется


@pytest.mark.parametrize("status,code", [("pending", 403), ("rejected", 403), ("blocked", 403)])
def test_non_active_accounts_get_no_session(accounts_env, app, status, code):
    acc = store.create_pending(login="newbie", display_name="Новиков Н.Н.", password="long-password-1")
    if status == "rejected":
        store.reject(None, acc["id"])
    if status == "blocked":
        store.approve(None, acc["id"])
        store.block(None, acc["id"])
    c, r = _login(app, "newbie", "long-password-1")
    assert r.status_code == code and r.json()["detail"]["code"] == f"account_{status}"
    assert "set-cookie" not in r.headers
    assert c.get("/api/projects").status_code == 401
    wrong = _login(app, "newbie", "wrong-password-x")[1]
    assert wrong.status_code == 401 and wrong.json()["detail"]["code"] == "invalid_credentials"


def test_revocation_applies_to_existing_session(accounts_env, app):
    c, _ = _login(app, "kulik", VIEWER_PW)
    assert c.get("/api/auth/me").json()["authenticated"]
    oc, _ = _login(app, "owner", OWNER_PW)
    r = oc.post(f"/api/admin/accounts/{accounts_env['viewer']['id']}/block", json={"reason": "тест"})
    assert r.status_code == 200
    blocked = c.get("/api/projects")
    assert blocked.status_code == 401 and blocked.json()["detail"]["code"] == "session_revoked"
    # разблокировка не воскрешает старую cookie (версия сессии сменилась)
    oc.post(f"/api/admin/accounts/{accounts_env['viewer']['id']}/unblock")
    assert c.get("/api/projects").status_code == 401


def test_section_rights_change_applies_next_request(accounts_env, app):
    c, _ = _login(app, "kulik", VIEWER_PW)
    assert c.get("/api/auth/me").json()["account"]["sections"]["audit_results"]["view"] is True
    store.set_sections(None, accounts_env["viewer"]["id"], {})                  # через «CLI»
    assert c.get("/api/auth/me").json()["account"]["sections"]["audit_results"]["view"] is False


def test_registration(accounts_env, app, monkeypatch):
    c = TestClient(app, headers=H)
    r = c.post("/api/auth/register", json={"login": "contractor", "display_name": "Подрядчиков П.П.",
                                           "password": "long-password-1", "note": "ОВ"})
    assert r.status_code == 200
    acc = next(a for a in store.list_accounts() if a["login"] == "contractor")
    assert (acc["role"], acc["status"]) == ("viewer", "pending")
    assert not any(v["view"] for v in acc["sections"].values())
    for extra in ({"role": "admin"}, {"status": "active"}, {"sections": {"audit_results": {"view": True}}},
                  {"staff_id": "boss"}):
        bad = c.post("/api/auth/register", json={"login": "hacker1", "display_name": "Хакер Х.Х.",
                                                 "password": "secret-password-9", **extra})
        assert bad.status_code == 422
        assert "secret-password-9" not in bad.text                     # без эха значений
    assert not any(a["login"] == "hacker1" for a in store.list_accounts())
    taken = c.post("/api/auth/register", json={"login": "Kulik", "display_name": "Кулик К.К.",
                                               "password": "long-password-1"})
    assert taken.status_code == 409
    monkeypatch.setenv("PORTAL_REGISTRATION_ENABLED", "false")
    closed = c.post("/api/auth/register", json={"login": "late1", "display_name": "Поздний П.П.",
                                                "password": "long-password-1"})
    assert closed.status_code == 403 and closed.json()["detail"]["code"] == "registration_closed"


def test_must_change_password_flow(accounts_env, app):
    oc, _ = _login(app, "owner", OWNER_PW)
    temp = oc.post(f"/api/admin/accounts/{accounts_env['viewer']['id']}/reset-password").json()["temporary_password"]
    c, r = _login(app, "kulik", temp)
    assert r.json()["must_change_password"] is True
    assert c.get("/api/auth/me").json()["account"]["must_change_password"] is True
    for path in ("/api/projects", "/", "/static/js/app.js", "/admin/users"):
        resp = c.get(path, follow_redirects=False)
        assert resp.status_code in (302, 403), path
    with pytest.raises(WebSocketDisconnect):
        with c.websocket_connect("/ws/global") as ws:
            ws.receive_text()
    weak = c.post("/api/auth/password", json={"current_password": temp, "new_password": "short"})
    assert weak.status_code == 400
    ok = c.post("/api/auth/password", json={"current_password": temp, "new_password": "my-new-password-1"})
    assert ok.status_code == 200
    me = c.get("/api/auth/me").json()                               # новая cookie выдана
    assert me["authenticated"] and not me["account"]["must_change_password"]


def test_request_guards(accounts_env, app):
    c, _ = _login(app, "owner", OWNER_PW)
    no_header = TestClient(app)
    no_header.cookies = c.cookies
    r = no_header.post("/api/auth/sessions/revoke")
    assert r.status_code == 403 and r.json()["detail"]["code"] == "csrf_header_required"
    r = c.post("/api/auth/sessions/revoke", headers={"Origin": "https://evil.example"})
    assert r.json()["detail"]["code"] == "bad_origin"
    r = c.get("/api/auth/me", headers={"Sec-Fetch-Site": "cross-site"})
    assert r.json()["detail"]["code"] == "cross_site_request"
    same = c.get("/api/auth/me", headers={"Sec-Fetch-Site": "same-origin", "Origin": "http://testserver"})
    assert same.status_code == 200
    # машинный контур bearer не затрагивается проверками cookie-контура
    worker = TestClient(app).post("/api/v1/worker/heartbeat", headers={"Sec-Fetch-Site": "cross-site"})
    assert worker.status_code != 403 or "csrf" not in worker.text


def test_old_cookie_formats_rejected(accounts_env, app):
    settings = portal_auth.get_settings()
    c = TestClient(app, headers=H)
    c.cookies.set("portal_session", portal_auth.issue_token("owner", settings))      # cookie v1
    assert c.get("/api/projects").status_code == 401
    forged = portal_auth._sign({"v": 2, "a": accounts_env["owner"]["id"], "sv": 1, "iat": 0,
                                "exp": 9999999999}, portal_auth.PortalSettings(
        enabled=True, users={}, secret="другой секрет", ttl_seconds=3600,
        cookie_secure_mode="false", cookie_name="portal_session", mode="accounts"))
    c.cookies.set("portal_session", forged)
    assert c.get("/api/projects").status_code == 401


def test_admin_api_viewer_forbidden_and_last_admin(accounts_env, app):
    vc, _ = _login(app, "kulik", VIEWER_PW)
    assert vc.get("/api/admin/accounts").status_code == 403
    oc, _ = _login(app, "owner", OWNER_PW)
    listing = oc.get("/api/admin/accounts").json()
    assert "password_hash" not in json.dumps(listing)
    me_id = accounts_env["owner"]["id"]
    assert oc.post(f"/api/admin/accounts/{me_id}/block", json={}).status_code == 409
    assert oc.put(f"/api/admin/accounts/{me_id}/role", json={"role": "viewer"}).status_code == 409
    bad = oc.put(f"/api/admin/accounts/{accounts_env['viewer']['id']}/sections",
                 json={"sections": {"audit_results": {"view": False, "download": True}}})
    assert bad.status_code == 400 and bad.json()["detail"]["code"] == "download_requires_view"


def test_login_closed_without_active_admin(accounts_env, app):
    import sqlite3
    with sqlite3.connect(db.db_path()) as conn:
        conn.execute("UPDATE accounts SET status='blocked' WHERE role='admin'")
    r = _login(app, "kulik", VIEWER_PW)[1]
    assert r.status_code == 503 and r.json()["detail"]["code"] == "no_active_admin"
    assert TestClient(app).get("/api/info").status_code == 200   # healthcheck жив


def test_action_log_records_account_id(accounts_env, app, tmp_path, monkeypatch):
    from backend.app.core import action_log, config
    log_dir = tmp_path / "actions"
    monkeypatch.setattr(config, "ACTION_LOG_DIR", log_dir)
    c, _ = _login(app, "owner", OWNER_PW)
    c.get("/api/objects")
    lines = [json.loads(x) for f in log_dir.glob("*.jsonl") for x in f.read_text().splitlines()]
    hit = [e for e in lines if e.get("path") == "/api/objects"]
    assert hit and hit[-1]["actor"] == "owner" and hit[-1]["account_id"] == accounts_env["owner"]["id"]
    assert not any(OWNER_PW in json.dumps(e, ensure_ascii=False) for e in lines)
    _ = action_log


def test_distributed_workers_admin_only(accounts_env, app, monkeypatch):
    from backend.app.services.distributed_workers import authorization as dw
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ADMIN_SUBJECTS", "owner")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_OPERATOR_SUBJECTS", "kulik")   # прежнее назначение
    monkeypatch.setenv("DISTRIBUTED_WORKERS_VIEWER_SUBJECTS", "kulik")

    def actor_for(ctx):
        class R:
            pass
        r = R()
        r.state = type("S", (), {"portal_account": ctx})()
        r.cookies = {}
        return dw.resolve_actor(r)

    viewer_ctx = store.get_context(accounts_env["viewer"]["id"])
    owner_ctx = store.get_context(accounts_env["owner"]["id"])
    assert actor_for(viewer_ctx).permissions == frozenset()
    assert dw.PERM_ADMIN in actor_for(owner_ctx).permissions


def test_websocket_connect_rules(accounts_env, app):
    vc, _ = _login(app, "kulik", VIEWER_PW)
    with pytest.raises(WebSocketDisconnect) as exc:
        with vc.websocket_connect("/ws/global") as ws:
            ws.receive_text()
    assert exc.value.code == 4403
    anon = TestClient(app)
    with pytest.raises(WebSocketDisconnect) as exc2:
        with anon.websocket_connect("/ws/global") as ws:
            ws.receive_text()
    assert exc2.value.code == 4401


class _FakeWS:
    def __init__(self):
        self.closed = None

    async def accept(self):
        pass

    async def close(self, code=1000):
        self.closed = code

    async def send_text(self, _):
        pass


def test_websocket_revocation_api_and_cli(accounts_env):
    from backend.app.ws.manager import ConnectionManager

    settings = portal_auth.get_settings()
    owner_ctx = store.get_context(accounts_env["owner"]["id"])
    second = store.create_account(login="admin2", display_name="Админ 2", role="admin",
                                  password="admin2-password", staff_id=None)
    second_ctx = store.get_context(second["id"])

    async def scenario():
        mgr = ConnectionManager()
        ws_api, ws_cli, ws_ok = _FakeWS(), _FakeWS(), _FakeWS()
        await mgr.connect_global(ws_api, ctx=second_ctx, token=portal_auth.issue_token_v2(second_ctx, settings))
        await mgr.connect_global(ws_ok, ctx=owner_ctx, token=portal_auth.issue_token_v2(owner_ctx, settings))
        # правка через API → немедленное закрытие
        assert await mgr.revoke_account(second["id"]) == 1
        assert ws_api.closed == 4401
        # правка через CLI (вне процесса) → закрытие перепроверкой
        fresh = store.get_context(second["id"])
        await mgr.connect_global(ws_cli, ctx=fresh, token=portal_auth.issue_token_v2(fresh, settings))
        store.set_role(None, second["id"], "viewer")
        assert await mgr.recheck_once() == 1
        assert ws_cli.closed == 4401 and ws_ok.closed is None
        mgr._recheck_task.cancel()

    asyncio.run(scenario())


def test_emergency_owner_mode(accounts_env, app, monkeypatch):
    # cookie, выданная в режиме accounts, в аварийном режиме не действует
    c, _ = _login(app, "owner", OWNER_PW)
    monkeypatch.setenv("PORTAL_AUTH_MODE", "emergency_owner")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "emergency-secret-" + "e" * 40)
    monkeypatch.setenv("PORTAL_SESSION_COOKIE_NAME", "am_emergency")
    monkeypatch.setenv("PORTAL_EMERGENCY_OWNER", "owner:" + portal_auth.hash_password("emergency-pw-1"))
    monkeypatch.setenv("PORTAL_AUTH_USERS", "kulik:" + portal_auth.hash_password(VIEWER_PW))
    assert c.get("/api/projects").status_code == 401
    assert _login(app, "kulik", VIEWER_PW)[1].status_code == 401           # строка env_users не читается
    assert _login(app, "owner", OWNER_PW)[1].status_code == 401            # пароль из БД не действует
    oc, r = _login(app, "owner", "emergency-pw-1")
    assert r.status_code == 200
    assert oc.get("/api/info").json().get("data_roots")                    # владелец — admin
    assert oc.get("/api/admin/accounts").status_code == 503                # аккаунтами не управляем
    reg = TestClient(app, headers=H).post("/api/auth/register", json={
        "login": "late2", "display_name": "Поздний П.П.", "password": "long-password-1"})
    assert reg.status_code == 403
    monkeypatch.setenv("PORTAL_EMERGENCY_OWNER", "owner:x,second:y")       # больше одного — никого
    assert _login(app, "owner", "emergency-pw-1")[1].status_code == 401
    _ = access
