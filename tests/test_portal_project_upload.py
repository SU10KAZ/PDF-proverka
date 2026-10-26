"""Project upload is an explicit, revocable permission, separate from audit starts."""
import sqlite3

import pytest
from fastapi.testclient import TestClient

from backend.app.services.accounts import db, store
from backend.app.services.common import project_service


@pytest.fixture
def portal(tmp_path, monkeypatch):
    for key, value in {
        "PORTAL_AUTH_ENABLED": "true", "PORTAL_AUTH_MODE": "accounts",
        "PORTAL_ACCOUNTS_DB": str(tmp_path / "accounts.sqlite3"),
        "PORTAL_PASSWORD_ROUNDS": "1000", "PORTAL_COOKIE_SECURE": "false",
        "PORTAL_SESSION_SECRET": "project-upload-secret-" + "s" * 40,
    }.items():
        monkeypatch.setenv(key, value)
    db.reset_for_tests()
    admin = store.create_account(login="owner", display_name="Owner", role="admin", password="owner-password-1")
    viewer = store.create_account(login="viewer", display_name="Viewer", role="viewer", password="viewer-password-1",
                                  sections={"audit_results": {"view": True}})
    calls = []

    def save(**kwargs):
        calls.append(kwargs)
        return {"project_id": "NEW"}

    monkeypatch.setattr(project_service, "save_uploaded_project_folder", save)
    monkeypatch.setattr(project_service, "precheck_uploaded_project_folder", lambda **kw: {"status": "ready"})
    from backend.app.main import app
    clients = []
    for login, password in (("owner", "owner-password-1"), ("viewer", "viewer-password-1")):
        client = TestClient(app, headers={"X-Requested-With": "AuditManager"})
        assert client.post("/api/auth/login", json={"username": login, "password": password}).status_code == 200
        clients.append(client)
    yield clients[0], clients[1], viewer, calls
    db.reset_for_tests()


def upload(client, suffix=""):
    return client.post("/api/projects/upload-folder" + suffix,
                       data={"object_id": "test-object", "project_name": "New", "discipline": "AR"},
                       files={"files": ("new.pdf", b"%PDF-1.7\n", "application/pdf")})


def test_admin_grants_upload_and_revokes_it_without_changing_viewer_role(portal):
    admin, viewer, account, calls = portal
    path = f"/api/admin/accounts/{account['id']}/upload-projects"
    assert account["upload_projects"] is False
    assert upload(viewer).status_code == 403
    assert viewer.put(path, json={"upload_projects": True}).status_code == 403
    assert admin.put(path, json={"upload_projects": True}).status_code == 200
    caps = viewer.get("/api/auth/me").json()["account"]
    assert caps["can_upload_projects"] is True and caps["is_admin"] is False
    assert upload(viewer, "/precheck").status_code == 200
    assert upload(viewer).status_code == 200
    assert calls[0]["object_id"] == "test-object"
    assert calls[0]["files"] == [("new.pdf", b"%PDF-1.7\n")]
    assert '"can_upload_projects": true' in viewer.get("/").text
    for path in ("/api/audit/batch", "/api/projects/register-external", "/api/projects/scan-external"):
        assert viewer.post(path, json={}).status_code == 403
    assert viewer.get("/api/admin/accounts").status_code == 403
    assert admin.put(f"/api/admin/accounts/{account['id']}/upload-projects", json={"upload_projects": False}).status_code == 200
    assert upload(viewer).status_code == 403
    assert len(calls) == 1
    assert any(e["event"] == "upload_projects_changed" for e in store.list_events())


def test_removing_audit_access_also_removes_upload_permission(portal):
    admin, viewer, account, calls = portal
    store.set_upload_projects(None, account["id"], True)
    store.set_sections(None, account["id"], {})
    assert store.get_account(account["id"])["upload_projects"] is False
    assert viewer.get("/api/auth/me").json()["account"]["can_upload_projects"] is False
    assert upload(viewer).status_code == 403
    response = admin.put(f"/api/admin/accounts/{account['id']}/upload-projects", json={"upload_projects": True})
    assert response.status_code == 400
    assert not calls


def test_existing_v1_database_adds_disabled_permission_table_without_rewriting_accounts(portal):
    _, _, account, _ = portal
    with sqlite3.connect(db.db_path()) as conn:
        conn.execute("DROP TABLE account_capabilities")
    db.reset_for_tests()
    assert store.get_account(account["id"])["upload_projects"] is False
    assert store.get_context(account["id"]).can_upload_projects is False
