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


@pytest.fixture
def comparison_uploads(portal, monkeypatch):
    from backend.app.api.routers import stage_comparison
    from backend.app.services.version_comparison import batches, jobs, store as versions
    from backend.app.services.common import object_service

    calls = []
    monkeypatch.setenv("VERSION_COMPARISON_ENABLED", "1")
    monkeypatch.setattr(object_service, "get_object_by_id", lambda oid: {"id": oid})
    monkeypatch.setattr(versions, "create_upload", lambda oid: "upload1")
    monkeypatch.setattr(versions, "add_file", lambda *args: calls.append("version_file") or [])
    monkeypatch.setattr(versions, "proposal", lambda *args: {"projects": []})
    monkeypatch.setattr(versions, "confirm", lambda *args: {"catalog": {"versions": {}}, "created_versions": []})
    monkeypatch.setattr(jobs, "ensure_profiles", lambda *args: calls.append("profiles"))
    monkeypatch.setattr(stage_comparison.stage_upload_mod, "replace_stage_from_folder",
                        lambda *args: calls.append("stage_folder") or {"pdf_count": 1})
    monkeypatch.setattr(stage_comparison.stage_upload_mod, "replace_stage_from_zip",
                        lambda *args: calls.append("stage_archive") or {"pdf_count": 1})
    monkeypatch.setattr(batches, "create", lambda oid, body: calls.append(body) or body)
    monkeypatch.setattr(batches, "get", lambda oid, bid: {"batch_id": bid, "compare_neighbors": bid == "comparison"})
    monkeypatch.setattr(batches, "start", lambda *args: calls.append("batch_start") or {"started": True})
    return calls


def upload_stage(client):
    return client.post("/api/stage-comparison/objects/test-object/stages/stage_1/upload-folder",
                       data={"relative_paths": '["project/test.pdf"]', "folder_name": "project"},
                       files={"files": ("test.pdf", b"%PDF-1.7\n", "application/pdf")})


def test_comparison_only_user_can_toggle_global_upload_without_audit_access(portal, comparison_uploads):
    admin, viewer, account, _ = portal
    store.set_sections(None, account["id"], {"version_comparison": {"view": True}, "stage_comparison": {"view": True}})
    path = f"/api/admin/accounts/{account['id']}/upload-projects"
    assert admin.put(path, json={"upload_projects": True}).status_code == 200
    caps = viewer.get("/api/auth/me").json()["account"]
    assert caps["uploads"] == {"audit_results": False, "version_comparison": True, "stage_comparison": True}
    assert caps["can_upload_projects"] is False and caps["is_admin"] is False
    assert upload(viewer).status_code == 403
    assert viewer.post("/api/version-comparison/objects/test-object/uploads").status_code == 200
    assert upload_stage(viewer).status_code == 200
    assert admin.put(path, json={"upload_projects": False}).status_code == 200
    assert viewer.post("/api/version-comparison/objects/test-object/uploads").status_code == 403
    assert upload_stage(viewer).status_code == 403


@pytest.mark.parametrize("section", ["audit_results", "version_comparison", "stage_comparison"])
def test_section_uploads_are_independent_and_revoke_without_logging_out(portal, comparison_uploads, section):
    admin, viewer, account, _ = portal
    sections = {s: {"view": True, "upload": s == section} for s in
                ("audit_results", "version_comparison", "stage_comparison")}
    path = f"/api/admin/accounts/{account['id']}/sections"
    assert viewer.put(path, json={"sections": sections}).status_code == 403
    assert admin.put(path, json={"sections": sections}).status_code == 200
    for target, send in (("audit_results", upload), ("stage_comparison", upload_stage),
                         ("version_comparison", lambda c: c.post("/api/version-comparison/objects/test-object/uploads"))):
        assert send(viewer).status_code == (200 if target == section else 403)
    sections[section]["view"] = sections[section]["upload"] = False
    assert admin.put(path, json={"sections": sections}).status_code == 200
    assert not any(store.get_account(account["id"])["uploads"].values())
    assert not any(viewer.get("/api/auth/me").json()["account"]["uploads"].values())


def test_version_upload_completes_without_granting_runs_or_comparison_batches(portal, comparison_uploads):
    _, viewer, account, _ = portal
    store.set_sections(None, account["id"], {"version_comparison": {"view": True, "upload": True}})
    base = "/api/version-comparison/objects/test-object"
    assert viewer.post(base + "/uploads").json()["upload_id"] == "upload1"
    assert viewer.post(base + "/uploads/upload1/files", files={"file": ("test.pdf", b"pdf")}).status_code == 200
    assert viewer.get(base + "/uploads/upload1/proposal").status_code == 200
    assert viewer.post(base + "/uploads/upload1/confirm", json={"projects": []}).status_code == 200
    body = {"batch_id": "new", "compare_neighbors": False, "items": [{"item_id": "item1", "title": "Project",
            "files": [{"file_id": "file1", "relative_path": "test.pdf", "size": 10}]}]}
    assert viewer.post(base + "/batches", json=body).status_code == 200
    assert viewer.post(base + "/batches", json={**body, "compare_neighbors": True}).status_code == 403
    assert viewer.post(base + "/batches/upload/start").status_code == 200
    assert viewer.post(base + "/batches/comparison/start").status_code == 403
    assert viewer.post(base + "/batches/comparison/items/i/action", json={"command": "retry"}).status_code == 403
    assert viewer.post(base + "/projects/p/runs", json={}).status_code == 403
    assert viewer.post("/api/stage-comparison/sessions", json={}).status_code == 403
    assert viewer.get("/api/admin/accounts").status_code == 403
    assert comparison_uploads[:2] == ["version_file", "profiles"]
    assert comparison_uploads[2]["compare_neighbors"] is False
    assert comparison_uploads[2]["batch_id"] == "new"
    assert comparison_uploads[3:] == ["batch_start"]


def test_section_grants_survive_unrelated_changes_and_audit_revocation(portal):
    _, _, account, _ = portal
    store.set_sections(None, account["id"], {"audit_results": {"view": True, "upload": True},
                       "version_comparison": {"view": True, "upload": True}})
    store.set_sections(None, account["id"], {"version_comparison": {"view": True}})
    assert store.get_account(account["id"])["uploads"] == {
        "audit_results": False, "version_comparison": True, "stage_comparison": False}
    store.set_sections(None, account["id"], {})
    store.set_sections(None, account["id"], {"version_comparison": {"view": True}})
    assert not any(store.get_account(account["id"])["uploads"].values())


@pytest.mark.parametrize("sections", [{"schedule": {"view": True, "upload": True}},
                                    {"version_comparison": {"view": False, "upload": True}}])
def test_upload_requires_supported_section_and_view(portal, sections):
    admin, _, account, _ = portal
    assert admin.put(f"/api/admin/accounts/{account['id']}/sections", json={"sections": sections}).status_code == 400


def test_existing_legacy_audit_grant_does_not_expand_to_comparisons(portal):
    _, _, account, _ = portal
    store.set_upload_projects(None, account["id"], True)
    with sqlite3.connect(db.db_path()) as conn:
        conn.execute("DROP TABLE account_section_uploads")
    db.reset_for_tests()
    assert store.get_account(account["id"])["uploads"] == {
        "audit_results": True, "version_comparison": False, "stage_comparison": False}


def test_blocked_account_can_keep_existing_permissions_but_cannot_receive_new_upload_grants(portal):
    admin, _, account, _ = portal
    sections = {"version_comparison": {"view": True, "upload": True}, "stage_comparison": {"view": True}}
    store.set_sections(None, account["id"], sections)
    store.block(None, account["id"])
    path = f"/api/admin/accounts/{account['id']}/sections"
    assert admin.put(path, json={"sections": sections}).status_code == 200
    assert not store.get_context(account["id"]).can_upload("version_comparison")
    sections["stage_comparison"]["upload"] = True
    assert admin.put(path, json={"sections": sections}).status_code == 400
