"""GET /api/projects: одновременные запросы одного объекта ждут один расчёт.

21.10.2026 список объекта под нагрузкой считался минутами, портал бросал
запрос через 25 с и слал новый, а брошенные продолжали считаться — до 27
одинаковых расчётов одновременно. v2_projects_list_shared объединяет их.
"""
import asyncio
import threading
import time

import pytest
from fastapi import HTTPException

from backend.app.services.storage import read_canary


@pytest.fixture(autouse=True)
def _clean_inflight():
    read_canary._projects_list_inflight.clear()
    yield
    read_canary._projects_list_inflight.clear()


def _slow_list(calls, delay=0.2, fail=False):
    lock = threading.Lock()

    def fake(object_id=None):
        with lock:
            calls.append(object_id)
        time.sleep(delay)
        if fail:
            raise HTTPException(status_code=404, detail="projects_v2 storage not available")
        return {"projects": [{"project_id": f"p-{object_id}"}], "object_name": str(object_id)}

    return fake


def test_concurrent_requests_same_object_share_one_computation(monkeypatch):
    calls = []
    monkeypatch.setattr(read_canary, "v2_projects_list", _slow_list(calls))

    async def run():
        return await asyncio.gather(*[read_canary.v2_projects_list_shared("obj1") for _ in range(10)])

    results = asyncio.run(run())
    assert calls == ["obj1"]
    assert all(r == results[0] for r in results)
    assert results[0]["projects"][0]["project_id"] == "p-obj1"
    assert read_canary._projects_list_inflight == {}


def test_different_objects_computed_separately(monkeypatch):
    calls = []
    monkeypatch.setattr(read_canary, "v2_projects_list", _slow_list(calls))

    async def run():
        return await asyncio.gather(
            read_canary.v2_projects_list_shared("a"),
            read_canary.v2_projects_list_shared("b"),
            read_canary.v2_projects_list_shared("a"),
        )

    ra, rb, ra2 = asyncio.run(run())
    assert sorted(calls) == ["a", "b"]
    assert ra["object_name"] == "a" and rb["object_name"] == "b" and ra2 is ra


def test_sequential_request_recomputes_no_stale_cache(monkeypatch):
    calls = []
    monkeypatch.setattr(read_canary, "v2_projects_list", _slow_list(calls, delay=0))

    async def run():
        await read_canary.v2_projects_list_shared("obj1")
        await read_canary.v2_projects_list_shared("obj1")

    asyncio.run(run())
    assert calls == ["obj1", "obj1"]


def test_error_goes_to_all_waiters_and_is_not_sticky(monkeypatch):
    calls = []
    monkeypatch.setattr(read_canary, "v2_projects_list", _slow_list(calls, fail=True))

    async def run():
        return await asyncio.gather(
            *[read_canary.v2_projects_list_shared("obj1") for _ in range(3)],
            return_exceptions=True,
        )

    results = asyncio.run(run())
    assert calls == ["obj1"]
    assert all(isinstance(r, HTTPException) and r.status_code == 404 for r in results)

    monkeypatch.setattr(read_canary, "v2_projects_list", _slow_list(calls, delay=0))
    ok = asyncio.run(read_canary.v2_projects_list_shared("obj1"))
    assert ok["object_name"] == "obj1"


def test_cancelled_request_does_not_cancel_shared_computation(monkeypatch):
    calls = []
    monkeypatch.setattr(read_canary, "v2_projects_list", _slow_list(calls, delay=0.3))

    async def run():
        first = asyncio.ensure_future(read_canary.v2_projects_list_shared("obj1"))
        await asyncio.sleep(0.05)
        second = asyncio.ensure_future(read_canary.v2_projects_list_shared("obj1"))
        await asyncio.sleep(0.05)
        first.cancel()  # клиент оборвал первый запрос
        return await second

    result = asyncio.run(run())
    assert calls == ["obj1"]
    assert result["object_name"] == "obj1"


def test_none_object_uses_default_key(monkeypatch):
    calls = []
    monkeypatch.setattr(read_canary, "v2_projects_list", _slow_list(calls))

    async def run():
        return await asyncio.gather(
            read_canary.v2_projects_list_shared(None),
            read_canary.v2_projects_list_shared(None),
        )

    asyncio.run(run())
    assert calls == [None]
