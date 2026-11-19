"""«Отработано» в сравнении стадий ставят инженеры-viewer с просмотром раздела.

Отметка не открывает остальные изменяющие действия раздела: они по-прежнему
только у администратора.
"""
import json

import pytest
from fastapi.testclient import TestClient

from backend.app.core import access_policy
from backend.app.services.accounts import db, store as accounts
import backend.app.services.common.user_service as users
from backend.app.services.stage_comparison import store as sc_store

PATH = "/api/stage-comparison/sessions/sess-1/worked-pairs"
BODY = {"left_pdf": "old/a.pdf", "right_pdf": "new/a.pdf"}


@pytest.fixture
def portal(tmp_path, monkeypatch):
    for key, value in {
        "PORTAL_AUTH_ENABLED": "true", "PORTAL_AUTH_MODE": "accounts",
        "PORTAL_ACCOUNTS_DB": str(tmp_path / "accounts.sqlite3"),
        "PORTAL_PASSWORD_ROUNDS": "1000", "PORTAL_COOKIE_SECURE": "false",
        "PORTAL_SESSION_SECRET": "mark-worked-secret-" + "s" * 40,
    }.items():
        monkeypatch.setenv(key, value)
    db.reset_for_tests()
    staff = tmp_path / "users.json"
    staff.write_text(json.dumps({"users": [
        {"id": "eng", "login": "engineer", "name": "Инженер И.И.", "role": "expert"},
    ], "current_id": "eng"}, ensure_ascii=False))
    monkeypatch.setattr(users, "USERS_FILE", staff)
    accounts.create_account(login="owner", display_name="Владелец", role="admin", password="owner-password-1")
    accounts.create_account(login="engineer", display_name="engineer", role="viewer",
                            password="viewer-password-1", staff_id="eng",
                            sections={"stage_comparison": {"view": True}})
    accounts.create_account(login="outsider", display_name="outsider", role="viewer",
                            password="viewer-password-1", sections={"audit_results": {"view": True}})
    calls = []

    def mark(session_id, left, right, *, by, by_id):
        calls.append((session_id, left, right, by))
        return {"worked_pairs": [{"left_pdf": left, "right_pdf": right, "by": by, "by_id": by_id}]}

    monkeypatch.setattr(sc_store, "mark_pair_worked", mark)
    monkeypatch.setattr(sc_store, "suggest_document_pairing", lambda session_id: {"ok": True})
    from backend.app.main import app

    def client(login):
        c = TestClient(app, headers={"X-Requested-With": "AuditManager"})
        assert c.post("/api/auth/login", json={"username": login, "password": "viewer-password-1"}).status_code == 200
        return c

    yield client, calls
    db.reset_for_tests()


def test_policy_is_mark_worked_for_stage_comparison():
    policy = access_policy.policy_for("POST", "/api/stage-comparison/sessions/{session_id}/worked-pairs")
    assert (policy.kind, policy.section) == ("MARK_WORKED", "stage_comparison")


def test_engineer_with_section_view_marks_pair(portal):
    client, calls = portal
    response = client("engineer").post(PATH, json=BODY)
    assert response.status_code == 200, response.text
    assert calls == [("sess-1", "old/a.pdf", "new/a.pdf", "Инженер И.И.")]


def test_viewer_without_section_is_refused(portal):
    client, calls = portal
    response = client("outsider").post(PATH, json=BODY)
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "forbidden_section"
    assert calls == []


def test_mark_worked_does_not_open_other_writes(portal):
    client, _ = portal
    response = client("engineer").post("/api/stage-comparison/sessions/sess-1/document-pairing/suggest")
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "admin_only"


def test_catalog_progress_is_open_to_viewers_of_stage_comparison():
    """Опрос «Ход выполнения» каталога — чтение статусов; без него у инженеров
    каждые 6 с всплывало «Действие доступно только администратору»."""
    from backend.app.core import access_policy as ap
    policy = ap.policy_for("GET", "/api/project-comparison/catalog/progress")
    assert (policy.kind, policy.section) == (ap.VIEW, "stage_comparison")
