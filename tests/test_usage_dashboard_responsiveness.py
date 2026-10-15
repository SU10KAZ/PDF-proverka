"""Periodic statistics reads must not stall unrelated HTTP requests."""
import asyncio
import threading
from types import SimpleNamespace

from fastapi import FastAPI
import httpx
import pytest


@pytest.mark.parametrize("case", ["global", "cost", "daily", "events", "blocked", "status"])
def test_slow_dashboard_read_does_not_block_navigation(monkeypatch, case):
    from backend.app.api.routers import usage
    from backend.app.services.llm import paid_api_events, paid_api_guard, paid_cost_dashboard

    cases = {
        "global": ("global", usage.global_scanner, "get_counters",
                   SimpleNamespace(model_dump=lambda: {"ready": True}), {"ready": True}),
        "cost": ("paid-cost", usage.paid_cost_tracker, "get", {"ready": True}, {"ready": True}),
        "daily": ("paid-cost/daily?days=7", paid_cost_dashboard,
                  "build_paid_cost_daily_dashboard", {"ready": True}, {"ready": True}),
        "events": ("paid-cost/events?limit=20", paid_api_events,
                   "read_paid_events_tail", [], {"events": []}),
        "blocked": ("paid-cost/blocked-events?limit=20", paid_api_events,
                    "read_blocked_events_tail", [], {"events": []}),
        "status": ("paid-api/status", paid_api_guard, "status_snapshot",
                   {"ready": True}, {"ready": True}),
    }
    path, target, method, result, expected = cases[case]
    started, release = threading.Event(), threading.Event()

    def blocked_read(*args, **kwargs):
        started.set()
        if not release.wait(3):
            raise RuntimeError("Dashboard read blocked the event loop")
        return result

    monkeypatch.setattr(target, method, blocked_read)
    app = FastAPI()
    app.include_router(usage.router)

    @app.get("/navigation")
    async def navigation():
        return {"ready": True}

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url="http://test") as client:
            pending = asyncio.create_task(client.get(f"/api/usage/{path}"))
            try:
                assert await asyncio.to_thread(started.wait, 2)
                response = await asyncio.wait_for(client.get("/navigation"), timeout=1)
                assert response.status_code == 200
                assert not pending.done()
            finally:
                release.set()
                response = await pending
            assert response.status_code == 200
            assert response.json() == expected

    asyncio.run(exercise())
