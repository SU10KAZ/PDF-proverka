"""
test_portal_auth.py
-------------------
Тесты простой портальной аутентификации (логин/пароль, session-cookie).

Покрывает:
* без авторизации /api/... → 401, HTML → редирект на /login;
* успешный вход с валидным пользователем;
* отказ при неверном пароле / неизвестном логине;
* после logout доступ снова закрыт;
* при PORTAL_AUTH_ENABLED=false поведение как раньше (нет блокировки);
* unit: подпись/проверка/срок жизни токена, парсинг users, dummy-verify.

Run:
    python -m pytest tests/test_portal_auth.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.core import portal_auth  # noqa: E402

_USER = "ivan"
_PASSWORD = "s3cret-pw"
_SECRET = "test-secret-" + "x" * 40


@pytest.fixture
def auth_env(monkeypatch):
    """Включить portal auth с одним пользователем ivan."""
    pw_hash = portal_auth.hash_password(_PASSWORD)
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setenv("PORTAL_AUTH_USERS", f"{_USER}:{pw_hash}")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", _SECRET)
    monkeypatch.setenv("PORTAL_SESSION_TTL_HOURS", "24")
    monkeypatch.setenv("PORTAL_COOKIE_SECURE", "false")
    return {"user": _USER, "password": _PASSWORD}


@pytest.fixture
def disabled_env(monkeypatch):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "false")
    monkeypatch.delenv("PORTAL_AUTH_USERS", raising=False)
    monkeypatch.setenv("PORTAL_SESSION_SECRET", _SECRET)


@pytest.fixture(scope="module")
def app():
    from backend.app.main import app as _app
    return _app


@pytest.fixture
def client(app):
    # Без context-manager → lifespan (pipeline manager) не запускается.
    return TestClient(app)


# ─── Disabled: поведение как раньше ──────────────────────────────────────────
def test_disabled_does_not_block_api(disabled_env, client):
    resp = client.get("/openapi.json", follow_redirects=False)
    assert resp.status_code == 200


def test_disabled_me_reports_disabled(disabled_env, client):
    resp = client.get("/api/auth/me")
    assert resp.status_code == 200
    data = resp.json()
    assert data["auth_enabled"] is False
    assert data["authenticated"] is True


def test_disabled_login_page_redirects_home(disabled_env, client):
    resp = client.get("/login", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"


# ─── Enabled, без авторизации ─────────────────────────────────────────────────
def test_unauth_api_returns_401(auth_env, client):
    resp = client.get("/api/projects", follow_redirects=False)
    assert resp.status_code == 401


def test_unauth_unknown_api_path_also_401(auth_env, client):
    # middleware блокирует до роутинга — даже несуществующий /api путь
    resp = client.get("/api/this-does-not-exist", follow_redirects=False)
    assert resp.status_code == 401


def test_unauth_openapi_blocked(auth_env, client):
    resp = client.get("/openapi.json", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == portal_auth.LOGIN_PATH


def test_unauth_docs_blocked(auth_env, client):
    resp = client.get("/docs", follow_redirects=False)
    assert resp.status_code == 302


def test_unauth_spa_redirects_to_login(auth_env, client):
    resp = client.get("/", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == portal_auth.LOGIN_PATH


def test_unauth_static_blocked(auth_env, client):
    resp = client.get("/static/js/app.js", follow_redirects=False)
    assert resp.status_code in (302, 401)


def test_login_page_served_when_enabled(auth_env, client):
    resp = client.get("/login", follow_redirects=False)
    assert resp.status_code == 200
    assert "login-form" in resp.text


def test_me_reports_unauthenticated(auth_env, client):
    resp = client.get("/api/auth/me")
    assert resp.status_code == 200
    data = resp.json()
    assert data["auth_enabled"] is True
    assert data["authenticated"] is False


def test_info_healthcheck_stays_open(auth_env, client):
    # /api/info используется cron-watchdog → должен быть доступен без auth
    resp = client.get("/api/info", follow_redirects=False)
    assert resp.status_code == 200


# ─── Логин / выход ────────────────────────────────────────────────────────────
def test_login_success_sets_cookie_and_grants_access(auth_env, client):
    resp = client.post(
        "/api/auth/login",
        json={"username": _USER, "password": _PASSWORD},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["authenticated"] is True
    assert data["username"] == _USER
    assert portal_auth.get_settings().cookie_name in resp.cookies

    # Теперь защищённые ресурсы доступны (cookie сохранён в client)
    ok = client.get("/openapi.json", follow_redirects=False)
    assert ok.status_code == 200

    me = client.get("/api/auth/me")
    assert me.json()["authenticated"] is True
    assert me.json()["username"] == _USER


def test_login_wrong_password_rejected(auth_env, client):
    resp = client.post(
        "/api/auth/login",
        json={"username": _USER, "password": "wrong"},
    )
    assert resp.status_code == 401
    assert resp.json()["authenticated"] is False
    # доступ по-прежнему закрыт
    assert client.get("/api/projects", follow_redirects=False).status_code == 401


def test_login_unknown_user_rejected(auth_env, client):
    resp = client.post(
        "/api/auth/login",
        json={"username": "nobody", "password": _PASSWORD},
    )
    assert resp.status_code == 401
    assert resp.json()["authenticated"] is False


def test_logout_closes_access(auth_env, client):
    login = client.post(
        "/api/auth/login",
        json={"username": _USER, "password": _PASSWORD},
    )
    assert login.status_code == 200
    assert client.get("/openapi.json", follow_redirects=False).status_code == 200

    logout = client.post("/api/auth/logout")
    assert logout.status_code == 200

    # после logout cookie очищен → доступ закрыт
    assert client.get("/openapi.json", follow_redirects=False).status_code == 302
    assert client.get("/api/projects", follow_redirects=False).status_code == 401


def test_tampered_cookie_rejected(auth_env, client):
    cookie_name = portal_auth.get_settings().cookie_name
    client.cookies.set(cookie_name, "garbage.notavalidtoken")
    resp = client.get("/api/projects", follow_redirects=False)
    assert resp.status_code == 401


# ─── Unit: токен и парсинг ────────────────────────────────────────────────────
def test_token_roundtrip(auth_env):
    s = portal_auth.get_settings()
    token = portal_auth.issue_token(_USER, s)
    assert portal_auth.verify_token(token, s) == _USER


def test_token_tampered_signature(auth_env):
    s = portal_auth.get_settings()
    token = portal_auth.issue_token(_USER, s)
    bad = token[:-3] + ("aaa" if not token.endswith("aaa") else "bbb")
    assert portal_auth.verify_token(bad, s) is None


def test_token_wrong_secret(auth_env, monkeypatch):
    s = portal_auth.get_settings()
    token = portal_auth.issue_token(_USER, s)
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "a-completely-different-secret-value")
    s2 = portal_auth.get_settings()
    assert portal_auth.verify_token(token, s2) is None


def test_token_expired(auth_env, monkeypatch):
    s = portal_auth.get_settings()
    token = portal_auth.issue_token(_USER, s)
    real_time = portal_auth.time.time
    monkeypatch.setattr(portal_auth.time, "time", lambda: real_time() + s.ttl_seconds + 60)
    assert portal_auth.verify_token(token, s) is None


def test_token_user_removed_from_config(auth_env, monkeypatch):
    s = portal_auth.get_settings()
    token = portal_auth.issue_token(_USER, s)
    monkeypatch.setenv("PORTAL_AUTH_USERS", "someoneelse:$pbkdf2-sha256$1$x$y")
    s2 = portal_auth.get_settings()
    assert portal_auth.verify_token(token, s2) is None


def test_parse_users_multiple():
    raw = "ivan:$pbkdf2-sha256$AAA,petr:$pbkdf2-sha256$BBB , olga:$pbkdf2-sha256$CCC"
    users = portal_auth._parse_users(raw)
    assert set(users) == {"ivan", "petr", "olga"}
    assert users["ivan"] == "$pbkdf2-sha256$AAA"


def test_verify_credentials_unknown_user_is_false(auth_env):
    s = portal_auth.get_settings()
    assert portal_auth.verify_credentials("ghost", "whatever", s) is False
    assert portal_auth.verify_credentials(_USER, _PASSWORD, s) is True
