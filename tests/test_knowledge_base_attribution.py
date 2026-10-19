"""Серверная атрибуция экспертной оценки.

Инцидент 2026-06-24: решения Калининой уезжали на «Узун А. И.» (глобальный
current_id). План доступа (поправка 0.2): автор — сотрудник вошедшего
аккаунта; без связи с сотрудником — 409, а не пустой автор; неизменённые
решения при повторном сохранении не переподписываются.
"""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from backend.app.core import access
from backend.app.core.attribution import resolve_author
import backend.app.services.common.user_service as user_service
import backend.app.services.knowledge_base.knowledge_base_service as kb
from backend.app.models.expert_review import ExpertDecision


def _req(ctx):
    return SimpleNamespace(state=SimpleNamespace(portal_account=ctx), cookies={})


@pytest.fixture
def staff(monkeypatch):
    users = {"kalinina": {"id": "kalinina", "login": "alexandra", "name": "Калинина А."}}
    monkeypatch.setattr(user_service, "get_user_by_id", lambda uid: users.get(uid))
    monkeypatch.setattr(user_service, "get_current_user", lambda: {"id": "uzun", "name": "Узун А. И."})


def test_auth_off_trusts_body(staff, monkeypatch):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "false")
    assert resolve_author(_req(None), body_reviewer="Кто-то Б.").name == "Кто-то Б."


def test_linked_account_uses_staff_name_not_body(staff):
    ctx = access.AccountContext(mode=access.MODE_ACCOUNTS, login="alexandra", role="admin",
                                account_id=7, staff_id="kalinina")
    author = resolve_author(_req(ctx), body_reviewer="Узун А. И.")
    assert (author.name, author.account_id) == ("Калинина А.", 7)


@pytest.mark.parametrize("staff_id", [None, "ghost"])
def test_unlinked_account_is_rejected_not_global_default(staff, staff_id):
    ctx = access.AccountContext(mode=access.MODE_ACCOUNTS, login="x", role="admin",
                                account_id=1, staff_id=staff_id)
    with pytest.raises(HTTPException) as exc:
        resolve_author(_req(ctx), body_reviewer="Узун А. И.")
    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "account_not_linked_to_staff"


def test_no_session_is_401(staff, monkeypatch):
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setattr("backend.app.core.portal_auth.resolve_request_account_sync", lambda r: None)
    with pytest.raises(HTTPException) as exc:
        resolve_author(_req(None))
    assert exc.value.status_code == 401


def _dec(item, decision, reason=None, **extra):
    return ExpertDecision(item_id=item, item_type="finding", decision=decision,
                          rejection_reason=reason, **extra)


def test_stamp_keeps_unchanged_and_signs_changed(monkeypatch):
    stored = {"decisions": [
        {"item_id": "F-1", "item_type": "finding", "decision": "accepted",
         "rejection_reason": None, "reviewer": "Кулик А.С.", "timestamp": "2026-09-01T10:00:00"},
        {"item_id": "F-2", "item_type": "finding", "decision": "accepted",
         "reviewer": "Кулик А.С.", "timestamp": "2026-09-01T10:00:00"},
        {"item_id": "F-3", "item_type": "finding", "decision": "rejected", "carried_over": True,
         "reviewer": "Кулик А.С.", "timestamp": "2026-09-01T10:00:00"},
    ]}
    monkeypatch.setattr(kb, "load_expert_review", lambda pid: stored)
    incoming = [
        _dec("F-1", "accepted", timestamp="2099-01-01", reviewer="Подмена"),  # без изменений
        _dec("F-2", "rejected", "не так"),                                      # изменено
        _dec("F-3", "rejected"),                                                # подтверждение авто-переноса
        _dec("F-4", "accepted", expert_account_id=999, carried_over=True),     # новое, поля тела игнорируются
    ]
    out, unchanged = kb.stamp_submitted_decisions("P", incoming, "Босс Б.Б.", account_id=3,
                                                  now="2026-10-19T12:00:00")
    by = {d.item_id: d for d in out}
    assert unchanged == {("finding", "F-1")}
    assert (by["F-1"].reviewer, by["F-1"].timestamp) == ("Кулик А.С.", "2026-09-01T10:00:00")
    for item in ("F-2", "F-3", "F-4"):
        assert (by[item].reviewer, by[item].timestamp, by[item].expert_account_id) == \
            ("Босс Б.Б.", "2026-10-19T12:00:00", 3)
        assert by[item].carried_over is False
