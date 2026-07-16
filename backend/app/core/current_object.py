"""
Per-request «текущий объект» — pure-ASGI middleware.

Зачем: «текущий объект» исторически был ГЛОБАЛЬНЫМ серверным состоянием
(`objects.json → current_id`), одним на всех пользователей. Когда один инженер
переключал объект через `POST /api/objects/switch`, глобальный `current_id`
менялся у ВСЕХ, и у остальных «пропадали проекты» (список текущего объекта в
их UI становился чужим/пустым).

Решение: фронт шлёт выбранный объект заголовком `X-Object-Id` на каждый
`/api/`-запрос. Этот middleware на время обработки запроса привязывает объект к
ContextVar `project_service._bound_object_id` (тот же механизм, которым конвейер
изолирует job'ы по объекту). Дальше `object_service.get_current_*` и
`project_service._get_projects_dir` резолвят СВОЙ объект per-request; глобальный
`current_id` остаётся лишь дефолтом для свежих сессий без заголовка.

Свойства:
- **Pure ASGI** (не BaseHTTPMiddleware) и регистрируется innermost → одна
  контекст-цепочка с роутером, ContextVar гарантированно виден в обработчике.
- **Fail-soft**: неизвестный/битый `object_id` → просто не привязываем (падаем
  на глобальный `current_id`), чтобы кривой заголовок не спрятал все проекты.
- Не трогает тело запроса/ответа (безопасно для стриминга и загрузок).
- Не «протекает» в фоновые job'ы: менеджер конвейера в начале задачи сам
  перепривязывает `job.object_id` (`_create_bound_task`), перекрывая любое
  унаследованное значение.
"""
from __future__ import annotations

from urllib.parse import parse_qs


class CurrentObjectMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        object_id = self._extract_object_id(scope)
        token = self._bind_if_known(object_id) if object_id else None
        try:
            await self.app(scope, receive, send)
        finally:
            if token is not None:
                self._unbind(token)

    @staticmethod
    def _extract_object_id(scope) -> str | None:
        """Заголовок `X-Object-Id` (приоритет) → query-параметр `object_id`."""
        try:
            for name, value in scope.get("headers") or []:
                if name == b"x-object-id":
                    v = value.decode("latin-1").strip()
                    if v:
                        return v
        except Exception:
            pass
        try:
            qs = (scope.get("query_string") or b"").decode("latin-1")
            if qs:
                vals = parse_qs(qs).get("object_id")
                if vals and vals[0].strip():
                    return vals[0].strip()
        except Exception:
            pass
        return None

    @staticmethod
    def _bind_if_known(object_id: str):
        """Привязать объект к ContextVar, только если он существует. Fail-soft."""
        try:
            from backend.app.services.common import object_service, project_service

            if object_service.get_object_by_id(object_id) is None:
                return None
            return project_service.bind_object(object_id)
        except Exception:
            return None

    @staticmethod
    def _unbind(token) -> None:
        try:
            from backend.app.services.common import project_service

            project_service.unbind_object(token)
        except Exception:
            pass
