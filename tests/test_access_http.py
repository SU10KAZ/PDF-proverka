"""Политика маршрутов через настоящий стек middleware + зависимость (этап 3).

Обработчики запуска подменены «миной»: если viewer дотянется до запуска,
тест падает. Реальные сервисы, модели и очереди не вызываются.
"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from backend.app.services.accounts import db, store
import backend.app.services.common.user_service as user_service

H = {"X-Requested-With": "AuditManager"}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setenv("PORTAL_AUTH_MODE", "accounts")
    monkeypatch.setenv("PORTAL_ACCOUNTS_DB", str(tmp_path / "acc.sqlite3"))
    monkeypatch.setenv("PORTAL_PASSWORD_ROUNDS", "1000")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "http-secret-" + "q" * 40)
    monkeypatch.setenv("PORTAL_COOKIE_SECURE", "false")
    db.reset_for_tests()
    users = tmp_path / "users.json"
    users.write_text(json.dumps({"users": [{"id": "boss", "login": "boss", "name": "Босс Б.Б."}]},
                                ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(user_service, "USERS_FILE", users)
    store.create_account(login="owner", display_name="Владелец", role="admin",
                         password="owner-password-1", staff_id="boss")
    store.create_account(login="reader", display_name="Читатель", role="viewer",
                         password="reader-password-1",
                         sections={"audit_results": {"view": True, "download": False}})
    yield
    db.reset_for_tests()


@pytest.fixture(scope="module")
def app():
    from backend.app.main import app as _app
    return _app


def _client(app, login, pw):
    c = TestClient(app, headers=H)
    assert c.post("/api/auth/login", json={"username": login, "password": pw}).status_code == 200
    return c


def _mine(*_a, **_k):
    raise AssertionError("viewer дотянулся до запуска обработки")


@pytest.mark.parametrize("method,path,body", [
    ("post", "/api/audit/EOM%2FX/full-audit", {}),
    ("post", "/api/audit/batch", {"project_ids": ["EOM/X"]}),
    ("delete", "/api/audit/EOM%2FX/cancel", None),
    ("post", "/api/stage-comparison/sessions", {"stage_a_path": "/a", "stage_b_path": "/b"}),
    ("get", "/api/version-comparison/objects/obj/projects/p/matrix", None),
    ("post", "/api/knowledge-base/expert-review/EOM%2FX", {"decisions": []}),
    ("get", "/api/audit/live-status", None),
    ("get", "/api/users", None),
    ("put", "/api/schedule/plan", {}),
])
def test_viewer_cannot_launch_or_modify(env, app, monkeypatch, method, path, body):
    from backend.app.pipeline.manager import pipeline_manager
    for name in ("start_audit", "start_batch", "cancel", "start_prepare"):
        monkeypatch.setattr(pipeline_manager, name, _mine, raising=False)
    c = _client(app, "reader", "reader-password-1")
    kwargs = {"json": body} if body is not None else {}
    r = getattr(c, method)(path, **kwargs)
    assert r.status_code == 403, (path, r.status_code, r.text[:200])
    assert r.json()["detail"]["code"] == "admin_only"


def test_viewer_section_gate(env, app):
    c = _client(app, "reader", "reader-password-1")
    allowed = c.get("/api/findings/summary")
    assert allowed.status_code != 403
    for path in ("/api/schedule", "/api/knowledge-base/entries", "/api/stage-comparison/objects",
                 "/api/version-comparison/objects"):
        r = c.get(path)
        assert r.status_code == 403 and r.json()["detail"]["code"] == "forbidden_section", path
    dl = c.get("/api/export/audit-package/EOM%2FX")
    assert dl.status_code == 403 and dl.json()["detail"]["code"] == "forbidden_download"


def test_docs_admin_only(env, app):
    c = _client(app, "reader", "reader-password-1")
    assert c.get("/openapi.json").status_code == 403
    assert _client(app, "owner", "owner-password-1").get("/openapi.json").status_code == 200
