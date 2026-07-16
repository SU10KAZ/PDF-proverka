"""Per-request «текущий объект»: заголовок X-Object-Id → binding на время запроса.

Регрессия против глобального current_id (переключение объекта одним инженером
«прятало» проекты у остальных). См. backend/app/core/current_object.py.
"""
import asyncio
import json

import httpx
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from backend.app.core.current_object import CurrentObjectMiddleware
from backend.app.services.common import object_service, project_service


def _seed(path, current="objA"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({
            "objects": [
                {"id": "objA", "name": "A", "projects_dir": "/tmp/objA"},
                {"id": "objB", "name": "B", "projects_dir": "/tmp/objB"},
            ],
            "current_id": current,
        }),
        encoding="utf-8",
    )


def _build_app():
    async def whoami(request):
        if request.query_params.get("delay"):
            await asyncio.sleep(0.01)
        return JSONResponse({"current_id": object_service.get_current_id()})

    app = Starlette(routes=[Route("/api/whoami", whoami)])
    app.add_middleware(CurrentObjectMiddleware)
    return app


def test_object_service_binding_precedence(tmp_path, monkeypatch):
    objects_file = tmp_path / "objects.json"
    _seed(objects_file)
    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)

    assert object_service.get_current_id() == "objA"  # глобальный дефолт
    token = project_service.bind_object("objB")
    try:
        assert object_service.get_current_id() == "objB"  # binding перекрывает
    finally:
        project_service.unbind_object(token)
    assert object_service.get_current_id() == "objA"  # снятие → снова глобальный


def test_object_service_unknown_binding_falls_back(tmp_path, monkeypatch):
    objects_file = tmp_path / "objects.json"
    _seed(objects_file)
    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)

    token = project_service.bind_object("does-not-exist")
    try:
        # Битый override не прячет всё — fail-soft на глобальный current_id.
        assert object_service.get_current_id() == "objA"
    finally:
        project_service.unbind_object(token)


def test_middleware_header_binds_and_resets(tmp_path, monkeypatch):
    objects_file = tmp_path / "objects.json"
    _seed(objects_file)
    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)
    client = TestClient(_build_app())

    # Без заголовка → глобальный A.
    assert client.get("/api/whoami").json()["current_id"] == "objA"
    # Заголовок B → B (per-request).
    assert client.get("/api/whoami", headers={"X-Object-Id": "objB"}).json()["current_id"] == "objB"
    # Неизвестный объект в заголовке → fail-soft на A.
    assert client.get("/api/whoami", headers={"X-Object-Id": "nope"}).json()["current_id"] == "objA"
    # Утечки нет: следующий запрос без заголовка → снова A.
    assert client.get("/api/whoami").json()["current_id"] == "objA"
    # Fallback: query-параметр object_id тоже привязывает.
    assert client.get("/api/whoami?object_id=objB").json()["current_id"] == "objB"


def test_middleware_two_users_isolated(tmp_path, monkeypatch):
    """Симуляция инцидента: глобальный current_id = A, «сосед» на B их не сбивает."""
    objects_file = tmp_path / "objects.json"
    _seed(objects_file, current="objA")
    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)
    client = TestClient(_build_app())

    # Инженер на объекте A (глобальный) и инженер на объекте B (свой заголовок)
    # видят каждый свой объект в перемешанных запросах.
    assert client.get("/api/whoami", headers={"X-Object-Id": "objA"}).json()["current_id"] == "objA"
    assert client.get("/api/whoami", headers={"X-Object-Id": "objB"}).json()["current_id"] == "objB"
    assert client.get("/api/whoami", headers={"X-Object-Id": "objA"}).json()["current_id"] == "objA"


def test_middleware_concurrent_requests_do_not_leak(tmp_path, monkeypatch):
    objects_file = tmp_path / "objects.json"
    _seed(objects_file)
    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)

    async def exercise():
        transport = httpx.ASGITransport(app=_build_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            async def scoped(object_id):
                response = await client.get(
                    "/api/whoami?delay=1",
                    headers={"X-Object-Id": object_id},
                )
                return response.json()["current_id"]

            return await asyncio.gather(scoped("objA"), scoped("objB"))

    assert asyncio.run(exercise()) == ["objA", "objB"]
