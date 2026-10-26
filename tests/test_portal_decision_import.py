"""Импорт Excel сотрудником не открывает остальные действия администратора."""
import io
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from openpyxl import Workbook

from backend.app.services.accounts import db, store
import backend.app.services.common.user_service as users
import backend.app.services.knowledge_base.knowledge_base_service as kb


@pytest.fixture
def portal(tmp_path, monkeypatch):
    for key, value in {
        "PORTAL_AUTH_ENABLED": "true", "PORTAL_AUTH_MODE": "accounts",
        "PORTAL_ACCOUNTS_DB": str(tmp_path / "accounts.sqlite3"),
        "PORTAL_PASSWORD_ROUNDS": "1000", "PORTAL_COOKIE_SECURE": "false",
        "PORTAL_SESSION_SECRET": "decision-import-secret-" + "s" * 40,
    }.items():
        monkeypatch.setenv(key, value)
    db.reset_for_tests()
    staff = tmp_path / "users.json"
    staff.write_text(json.dumps({"users": [
        {"id": "kalinina", "login": "alexandra", "name": "Калинина А.", "role": "expert"},
        {"id": "other", "login": "restricted", "name": "Другой Д.Д.", "role": "expert"},
    ], "current_id": "other"}, ensure_ascii=False))
    monkeypatch.setattr(users, "USERS_FILE", staff)
    store.create_account(login="owner", display_name="Владелец", role="admin", password="owner-password-1")
    for login, staff_id, view in (("alexandra", "kalinina", True), ("unlinked", None, True),
                                  ("restricted", "other", False)):
        store.create_account(login=login, display_name=login, role="viewer", password="viewer-password-1",
                             staff_id=staff_id, sections={"audit_results": {"view": view}})
    calls = []

    def import_excel(path, **kwargs):
        assert Path(path).read_bytes()[:2] == b"PK"
        calls.append(kwargs)
        return {"PROJECT": {"saved": 1, "accepted": 1, "rejected": 0}}

    monkeypatch.setattr(kb, "import_decisions_from_excel", import_excel)
    from backend.app.main import app

    def client(login=None):
        c = TestClient(app, headers={"X-Requested-With": "AuditManager"})
        if login:
            password = "owner-password-1" if login == "owner" else "viewer-password-1"
            assert c.post("/api/auth/login", json={"username": login, "password": password}).status_code == 200
        return c

    yield client, calls
    db.reset_for_tests()


def xlsx():
    workbook = Workbook()
    workbook.active.append(["ID", "Решение эксперта"])
    workbook.active.append(["F-1", "Принято"])
    stream = io.BytesIO()
    workbook.save(stream)
    workbook.close()
    return ("decisions.xlsx", stream.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def test_linked_employee_imports_with_own_server_author(portal):
    client, calls = portal
    c = client("alexandra")
    account = c.get("/api/auth/me").json()["account"]
    assert account["can_import_decisions"] is True
    assert account["is_admin"] is False
    assert account["sections"]["knowledge_base"]["view"] is False
    response = c.post("/api/knowledge-base/upload-excel?project_id=PROJECT&version_id=v2",
                      files={"file": xlsx()}, data={"reviewer": "Другой Д.Д."})
    assert response.status_code == 200, response.text
    assert response.json()["reviewer"] == "Калинина А."
    assert calls == [{"default_project_id": "PROJECT", "reviewer": "Калинина А.",
                      "account_id": account["account_id"]}]
    page = c.get("/")
    assert '"can_import_decisions": true' in page.text
    assert '"can_operate": false' in page.text
    for method, path in (("POST", "/api/audit/batch"),
                         ("POST", "/api/knowledge-base/expert-review/PROJECT"),
                         ("PUT", "/api/schedule/plan")):
        denied = c.request(method, path, json=[])
        assert denied.status_code == 403
        assert denied.json()["detail"]["code"] == "admin_only"
    assert c.get("/api/admin/accounts").status_code == 403


@pytest.mark.parametrize("login,status", [(None, 401), ("unlinked", 403), ("restricted", 403)])
def test_missing_employee_or_section_cannot_import(portal, login, status):
    client, calls = portal
    c = client(login)
    if login:
        assert c.get("/api/auth/me").json()["account"]["can_import_decisions"] is False
    response = c.post("/api/knowledge-base/upload-excel?staff_id=kalinina", files={"file": xlsx()})
    assert response.status_code == status
    if login:
        assert response.json()["detail"]["code"] == "forbidden_decision_import"
    assert not calls
