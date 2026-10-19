"""Единое определение объекта запроса (план доступа, поправка 0.4).

Объект может прийти тремя путями: параметром пути (``{object_id}``),
заголовком ``X-Object-Id`` (его ставит фронт на каждый fetch) и query
``?object_id=``. Правила:

* значения из разных источников не совпали → ``400 object_conflict``;
* объект указан, но его нет в реестре → ``404 unknown_object`` (при
  включённой авторизации; при выключенной — локальная разработка и тесты
  с временными объектами — проверяется только конфликт);
* ``_`` в пути (``/api/external-register/_/…``) означает «не указан».

Молчаливого перехода к глобальному объекту или первому совпадению нет:
при явно выбранном объекте документ ищется только в нём (``read_canary``
и резолверы получают объект из привязанного ContextVar).
"""
from __future__ import annotations

from typing import Optional

from fastapi import HTTPException
from starlette.requests import HTTPConnection

_PATH_PARAMS = ("object_id",)
_ABSENT = {"", "_"}


def request_object_ids(conn: HTTPConnection) -> dict[str, str]:
    found: dict[str, str] = {}
    params = conn.scope.get("path_params") or {}
    for name in _PATH_PARAMS:
        value = str(params.get(name) or "").strip()
        if value not in _ABSENT:
            found["path"] = value
    header = (conn.headers.get("x-object-id") or "").strip()
    if header not in _ABSENT:
        found["header"] = header
    query = (conn.query_params.get("object_id") or "").strip()
    if query not in _ABSENT:
        found["query"] = query
    return found


def resolve_request_object(conn: HTTPConnection, *, check_known: bool) -> Optional[str]:
    """Объект запроса или None; бросает 400/404 по правилам модуля."""
    found = request_object_ids(conn)
    values = set(found.values())
    if len(values) > 1:
        raise HTTPException(400, {
            "code": "object_conflict",
            "message": "Объект указан по-разному в пути, заголовке и параметрах запроса.",
            "sources": sorted(found),
        })
    if not values:
        return None
    object_id = values.pop()
    if check_known:
        from backend.app.services.common import object_service

        if object_service.get_object_by_id(object_id) is None:
            raise HTTPException(404, {"code": "unknown_object", "message": "Объект не найден."})
    return object_id


def _session_belongs(session_id: str, object_id: str) -> Optional[bool]:
    """Сессия сравнения стадий принадлежит объекту? None — сессии нет (решит маршрут)."""
    from pathlib import Path

    from backend.app.services.stage_comparison import stage_upload, store

    paths = store.session_stage_paths(session_id)
    if paths is None:
        return None
    try:
        _, comparison_dir = stage_upload.resolve_object_dir(object_id, create=False)
    except stage_upload.StageUploadError:
        return False
    root = comparison_dir.resolve()
    expected = (str(root / "stage_1"), str(root / "stage_2"))
    actual = tuple(str(Path(p).expanduser().resolve()) for p in paths)
    return actual == expected


def enforce_object_scope(conn: HTTPConnection) -> Optional[str]:
    from backend.app.core.access import MODE_DISABLED, portal_mode

    check = portal_mode() != MODE_DISABLED
    object_id = resolve_request_object(conn, check_known=check)
    session_id = str((conn.scope.get("path_params") or {}).get("session_id") or "")
    if check and object_id and session_id \
            and str(conn.scope.get("path") or conn.url.path).startswith("/api/stage-comparison/"):
        if _session_belongs(session_id, object_id) is False:
            raise HTTPException(404, {"code": "session_not_in_object",
                                      "message": "Сессия не относится к выбранному объекту."})
    return object_id


__all__ = ["enforce_object_scope", "request_object_ids", "resolve_request_object"]
