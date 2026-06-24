"""Серверная атрибуция автора экспертной оценки (_resolve_reviewer).

Инцидент 2026-06-24: решения Калининой уезжали на «Узун А. И.» (глобальный
current_id), а Excel-импорт вообще терял автора. Фикс — резолвить автора на
сервере из портал-сессии, без молчаливого отката на глобального дефолта.
"""
from types import SimpleNamespace

import backend.app.api.routers.knowledge_base as kb_router


class _FakeRequest:
    """Минимальная заглушка Request (важно лишь, что объект передаётся дальше)."""


def _patch(monkeypatch, *, enabled, username, user):
    monkeypatch.setattr(
        kb_router.portal_auth, "get_settings",
        lambda: SimpleNamespace(enabled=enabled),
    )
    monkeypatch.setattr(
        kb_router.portal_auth, "request_username",
        lambda request, settings: username,
    )
    monkeypatch.setattr(
        kb_router.user_service, "get_user_by_login",
        lambda login: user,
    )


def test_auth_off_trusts_body(monkeypatch):
    """auth выключен (локальная разработка) → доверяем телу запроса."""
    _patch(monkeypatch, enabled=False, username=None, user=None)
    assert kb_router._resolve_reviewer(_FakeRequest(), "Кто-то Б.") == "Кто-то Б."


def test_auth_on_matched_uses_session_name(monkeypatch):
    """auth включён, логин сопоставлен → автор СТРОГО из сессии (не из тела)."""
    _patch(
        monkeypatch, enabled=True, username="alexandra",
        user={"id": "kalinina", "login": "alexandra", "name": "Калинина А."},
    )
    # даже если клиент прислал чужого/дефолтного — сервер перебивает сессией
    assert kb_router._resolve_reviewer(_FakeRequest(), "Узун А. И.") == "Калинина А."


def test_auth_on_unmatched_is_empty_not_global_default(monkeypatch):
    """auth включён, но логин не сопоставлен → пусто, НЕ глобальный Узун."""
    _patch(monkeypatch, enabled=True, username="ghost", user=None)
    assert kb_router._resolve_reviewer(_FakeRequest(), "Узун А. И.") == ""


def test_auth_on_no_session_is_empty(monkeypatch):
    """auth включён, сессии нет (username=None) → пусто."""
    _patch(monkeypatch, enabled=True, username=None, user=None)
    assert kb_router._resolve_reviewer(_FakeRequest()) == ""
