from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from backend.app.api.routers import project_assemblies as router_mod

def test_disabled_module_is_404_and_does_not_touch_service(monkeypatch):
    called = False

    def forbidden(_object_id):
        nonlocal called
        called = True
        raise AssertionError("service must not be called")

    monkeypatch.setattr(router_mod.service, "enabled", lambda: False)
    monkeypatch.setattr(router_mod.service, "list_sources", forbidden)
    with pytest.raises(HTTPException) as error:
        asyncio.run(router_mod.sources("obj"))
    assert error.value.status_code == 404
    assert called is False


def test_sources_contract_when_enabled(monkeypatch):
    async def inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(router_mod.service, "enabled", lambda: True)
    monkeypatch.setattr(router_mod.service, "list_sources", lambda object_id: {
        "schema": "project_assembly_sources/1", "object_id": object_id, "items": [],
    })
    monkeypatch.setattr(router_mod, "run_in_threadpool", inline)
    response = asyncio.run(router_mod.sources("obj"))
    assert response == {"schema": "project_assembly_sources/1", "object_id": "obj", "items": []}
