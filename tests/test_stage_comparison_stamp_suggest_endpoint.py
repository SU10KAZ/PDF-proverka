"""Regression: `suggest-by-stamp` endpoint must not 500 with NameError.

The endpoint wraps the heavy stamp matcher in
`fastapi.concurrency.run_in_threadpool` to keep the event loop responsive.
That symbol MUST be imported in the router module — otherwise the route raises
`NameError: name 'run_in_threadpool' is not defined` **at request time**.
`compileall` / module import do NOT catch this (the name is only resolved when
the endpoint runs), which is exactly how it slipped through and broke the
deploy-cutover live smoke. These tests lock it down.

Heavy matcher (`store.suggest_alignment_by_stamp`) is monkeypatched, so no MD /
LLM / network / filesystem session is required.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison
from backend.app.main import app


def test_run_in_threadpool_symbol_available_in_router():
    """Cheap guard: the symbol the endpoint calls must be importable/callable."""
    assert callable(stage_comparison.run_in_threadpool)


def test_suggest_by_stamp_endpoint_does_not_raise_name_error(monkeypatch):
    """Route test: POST suggest-by-stamp returns the (mocked) matcher result,
    not a 500 NameError. Proves `run_in_threadpool` is wired into the endpoint."""
    calls = {}

    def _fake_suggest(session_id, pair_id, use_llm=True):
        calls["args"] = (session_id, pair_id, use_llm)
        return {"method": "stamp", "matched_count": 0, "suggested_items": []}

    # Endpoint reads `store.suggest_alignment_by_stamp` at call time, so patching
    # the attribute on the router's `store` module is enough.
    monkeypatch.setattr(
        stage_comparison.store, "suggest_alignment_by_stamp", _fake_suggest
    )

    client = TestClient(app)
    resp = client.post(
        "/api/stage-comparison/sessions/s1/pairs/p1/page-alignment/suggest-by-stamp",
        json={"use_llm": False},
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["method"] == "stamp"
    # The endpoint forwarded our args through run_in_threadpool to the matcher.
    assert calls["args"] == ("s1", "p1", False)
