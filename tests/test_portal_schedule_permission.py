"""Calendar editing is granted separately and never opens other write routes."""
import sqlite3
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from backend.app.services.accounts import db, store
from backend.app.services.common import reserve_service, schedule_service


@pytest.fixture
def portal(tmp_path, monkeypatch):
    for key, value in {
        "PORTAL_AUTH_ENABLED": "true", "PORTAL_AUTH_MODE": "accounts",
        "PORTAL_ACCOUNTS_DB": str(tmp_path / "accounts.sqlite3"),
        "PORTAL_PASSWORD_ROUNDS": "1000", "PORTAL_COOKIE_SECURE": "false",
        "PORTAL_SESSION_SECRET": "calendar-test-secret-" + "s" * 40,
    }.items():
        monkeypatch.setenv(key, value)
    db.reset_for_tests()
    admin = store.create_account(login="owner", display_name="Owner", role="admin", password="owner-password-1")
    viewer = store.create_account(login="viewer", display_name="Viewer", role="viewer", password="viewer-password-1",
                                  sections={"schedule": {"view": True}, "audit_results": {"view": True}})
    calls = []
    monkeypatch.setattr(reserve_service, "snapshot", lambda actor: {"tasks": [], "revision": 0})

    def mutate(body, actor):
        calls.append((body, actor))
        return {"tasks": [], "revision": 1}

    def save_plans(**kwargs):
        calls.append(kwargs)
        return {"plans": kwargs["plans"]}

    monkeypatch.setattr(reserve_service, "mutate", mutate)
    monkeypatch.setattr(schedule_service, "save_plans", save_plans)
    from backend.app.main import app
    clients = []
    for login in ("owner", "viewer"):
        client = TestClient(app, headers={"X-Requested-With": "AuditManager"})
        assert client.post("/api/auth/login", json={"username": login, "password": login + "-password-1"}).status_code == 200
        clients.append(client)
    yield clients[0], clients[1], admin, viewer, calls
    db.reset_for_tests()


def operation(client, kind="add_preparation"):
    return client.post("/api/schedule/reserve/operations", json={
        "operation_id": str(uuid4()), "revision": 0, "kind": kind,
    })


def plan(client):
    return client.put("/api/schedule/plan", json={
        "period_type": "month", "period_start": "2026-10-01", "period_end": "2026-10-31",
        "plans": [{"engineer_id": "expert", "plan": 12}],
    })


def test_grant_all_calendar_operations_and_revoke_in_existing_session(portal):
    admin, viewer, owner, account, calls = portal
    path = f"/api/admin/accounts/{account['id']}/edit-schedule"
    assert viewer.get("/api/schedule/reserve").json()["can_edit"] is False
    assert operation(viewer).status_code == 403
    assert plan(viewer).status_code == 403
    assert viewer.put(path, json={"edit_schedule": True}).status_code == 403
    assert not calls
    assert admin.put(path, json={"edit_schedule": True}).status_code == 200
    caps = viewer.get("/api/auth/me").json()["account"]
    assert caps["can_edit_schedule"] is True and caps["is_admin"] is False and caps["role"] == "viewer"
    assert '"can_edit_schedule": true' in viewer.get("/").text
    assert viewer.get("/api/schedule/reserve").json()["can_edit"] is True
    for kind in ("add_preparation", "assign", "link", "move", "delete", "start", "undo"):
        assert operation(viewer, kind).status_code == 200
    assert all(actor == f"account:{account['id']}" for _, actor in calls)
    assert plan(viewer).status_code == 200
    assert calls[-1]["updated_by"] == "viewer"
    for method, route in (("get", "/api/admin/accounts"), ("get", "/api/users"),
                          ("post", "/api/audit/batch"), ("put", "/api/users/expert")):
        assert getattr(viewer, method)(route).status_code == 403
    assert admin.put(path, json={"edit_schedule": False}).status_code == 200
    assert viewer.get("/api/schedule/reserve").json()["can_edit"] is False
    assert operation(viewer).status_code == plan(viewer).status_code == 403
    events = [e for e in store.list_events() if e["event"] == "edit_schedule_changed"]
    assert len(events) == 2 and all(e["actor_account_id"] == owner["id"] for e in events)


def test_calendar_access_revocation_clears_grant_and_upload_is_independent(portal):
    _, viewer, owner, account, _ = portal
    store.set_edit_schedule(owner["id"], account["id"], True)
    store.set_upload_projects(owner["id"], account["id"], True)
    store.set_sections(owner["id"], account["id"], {"schedule": {"view": True}})
    assert store.get_account(account["id"])["upload_projects"] is False
    assert operation(viewer).status_code == 200
    store.set_sections(owner["id"], account["id"], {"audit_results": {"view": True}})
    assert store.get_account(account["id"])["edit_schedule"] is False
    assert operation(viewer).status_code == plan(viewer).status_code == 403
    store.set_sections(owner["id"], account["id"], {"schedule": {"view": True}})
    assert operation(viewer).status_code == 403


def test_grant_requires_active_account_with_calendar_access(portal):
    _, _, owner, account, _ = portal
    store.set_sections(owner["id"], account["id"], {})
    with pytest.raises(store.AccountError, match="просмотр графика"):
        store.set_edit_schedule(owner["id"], account["id"], True)
    store.set_sections(owner["id"], account["id"], {"schedule": {"view": True}})
    store.set_edit_schedule(owner["id"], account["id"], True)
    store.block(owner["id"], account["id"])
    assert store.get_context(account["id"]).can_edit_schedule is False
    with pytest.raises(store.AccountError):
        store.set_edit_schedule(owner["id"], account["id"], True)


def test_existing_database_adds_disabled_calendar_permission(portal):
    _, _, _, account, _ = portal
    with sqlite3.connect(db.db_path()) as conn:
        conn.execute("DROP TABLE account_schedule_permissions")
    db.reset_for_tests()
    assert store.get_account(account["id"])["edit_schedule"] is False
    assert store.get_context(account["id"]).can_edit_schedule is False
