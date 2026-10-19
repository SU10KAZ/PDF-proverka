"""Этап 0 плана доступа: исправления, не зависящие от новой модели ролей.

* `GET /api/export/download/{filename}` больше не отдаёт файлы корня
  приложения (раньше — fallback в BASE_DIR) и доступен только администратору;
* `/api/info` анониму — без путей сервера;
* изменение сотрудников, расход/деньги, журнал действий, учётная запись
  модели — только администратор (в режиме env_users: users.json role=admin).
"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from backend.app.core import portal_auth
import backend.app.services.common.user_service as user_service

_SECRET = "stage0-secret-" + "y" * 40


@pytest.fixture
def staff(tmp_path, monkeypatch):
    f = tmp_path / "users.json"
    f.write_text(json.dumps({"users": [
        {"id": "boss", "login": "boss", "surname": "Босс", "initials": "Б.Б.",
         "name": "Босс Б.Б.", "role": "admin"},
        {"id": "expert1", "login": "exp", "surname": "Эксперт", "initials": "Э.Э.",
         "name": "Эксперт Э.Э.", "role": "expert"},
    ], "current_id": "boss"}, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(user_service, "USERS_FILE", f)
    return f


@pytest.fixture
def env_users(monkeypatch, staff):
    hashes = ",".join(f"{u}:{portal_auth.hash_password('pw-' + u)}" for u in ("boss", "exp", "ghost"))
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.delenv("PORTAL_AUTH_MODE", raising=False)
    monkeypatch.setenv("PORTAL_AUTH_USERS", hashes)
    monkeypatch.setenv("PORTAL_SESSION_SECRET", _SECRET)
    monkeypatch.setenv("PORTAL_COOKIE_SECURE", "false")


@pytest.fixture(scope="module")
def app():
    from backend.app.main import app as _app
    return _app


def _client(app, login=None):
    c = TestClient(app)
    if login:
        c.cookies.set("portal_session", portal_auth.issue_token(login, portal_auth.get_settings()))
    return c


@pytest.fixture
def reports(tmp_path, monkeypatch):
    from backend.app.core import config
    d = tmp_path / "отчет"
    d.mkdir()
    (d / "audit_report_20261019_120000_abc123.xlsx").write_bytes(b"PK\x03\x04xlsx")
    (d / ".hidden.xlsx").write_bytes(b"secret")
    monkeypatch.setattr(config, "REPORTS_DIR", d)
    return d


@pytest.mark.parametrize("name", [
    ".mcp.json", ".env", "CLAUDE.md", "AGENTS.md", "requirements.txt",
    "generate_excel_report.py", ".hidden.xlsx", "..", "x.xlsx.json",
])
def test_download_never_serves_root_or_hidden_files(env_users, reports, app, name):
    r = _client(app, "boss").get(f"/api/export/download/{name}")
    assert r.status_code == 404, (name, r.status_code)


def test_download_serves_report_to_admin_only(env_users, reports, app):
    name = "audit_report_20261019_120000_abc123.xlsx"
    ok = _client(app, "boss").get(f"/api/export/download/{name}")
    assert ok.status_code == 200 and ok.content.startswith(b"PK")
    assert _client(app, "exp").get(f"/api/export/download/{name}").status_code == 403
    assert _client(app).get(f"/api/export/download/{name}").status_code == 401


def test_api_info_short_for_anonymous_and_non_admin(env_users, app):
    for client in (_client(app), _client(app, "exp")):
        body = client.get("/api/info").json()
        assert body == {"app": "Audit Manager", "version": "1.0.0", "status": "ok"}
    full = _client(app, "boss").get("/api/info").json()
    assert "data_roots" in full and "claude_cli" in full


@pytest.mark.parametrize("method,path,body", [
    ("post", "/api/users", {"surname": "Новый"}),
    ("post", "/api/users/switch", {"id": "boss"}),
    ("put", "/api/users/expert1", {"role": "admin"}),
    ("delete", "/api/users/boss", None),
])
def test_staff_changes_admin_only(env_users, app, method, path, body):
    kwargs = {"json": body} if body is not None else {}
    r = getattr(_client(app, "exp"), method)(path, **kwargs)
    assert r.status_code == 403
    users = {u["id"]: u for u in user_service.list_users()}
    assert users["expert1"]["role"] == "expert" and "boss" in users


def test_admin_can_still_change_staff(env_users, app):
    r = _client(app, "boss").put("/api/users/expert1", json={"initials": "Э.Ю."})
    assert r.status_code == 200


@pytest.mark.parametrize("path", [
    "/api/usage/global", "/api/usage/paid-cost", "/api/usage/subscription-by-person",
    "/api/action-log", "/api/action-log/stats", "/api/audit/account",
])
def test_operational_info_admin_only(env_users, app, path):
    assert _client(app, "exp").get(path).status_code == 403
    assert _client(app, "ghost").get(path).status_code == 403  # логин без сотрудника


def test_staff_list_still_readable(env_users, app):
    r = _client(app, "exp").get("/api/users")
    assert r.status_code == 200 and r.json()["current_id"] == "expert1"


def test_auth_disabled_keeps_everything_open(monkeypatch, staff, app):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "false")
    assert _client(app).put("/api/users/expert1", json={"initials": "Х."}).status_code == 200
