"""Этап прогона «Сведение дублей»: идёт после V3 под замком пары, не меняет V3, пишет свой ход (0 вызовов модели)."""
from __future__ import annotations

import contextlib
import copy
import json

import pytest

from backend.app.services.project_change_consolidator import contracts as C
from backend.app.services.project_change_consolidator import stage as S
from backend.app.services.project_change_consolidator.storage import SHADOW_DIR_NAME
from backend.tests.project_change_consolidator.test_e2e_production_boundary import consolidate, env  # noqa: F401
from backend.tests.project_change_v3 import generic_fixture as gf

COMPLETED = {"status": "COMPLETED", "reason_code": "v3_completed", "run_id": "r" * 32, "session_id": "s", "pair_id": "p"}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (S.FLAG, S.PROVIDER_ENV, S.MAX_CALLS_ENV):
        monkeypatch.delenv(name, raising=False)


def _orchestrator(monkeypatch, state, events):
    from backend.app.services.project_change_v3 import engine as v3_engine
    from backend.app.services.stage_comparison import production_orchestrator as orch

    @contextlib.contextmanager
    def lock(session_id, pair_id):
        events.append("lock")
        yield
        events.append("unlock")

    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setattr(orch.production_store, "production_pair_lock", lock)
    monkeypatch.setattr(v3_engine, "run_v3_production_comparison", lambda *a, **k: state)
    return orch


def test_stage_runs_inside_the_pair_lock_with_the_run_cancel_token(monkeypatch):
    events = []
    state = copy.deepcopy(COMPLETED)
    orch = _orchestrator(monkeypatch, state, events)

    def fake_run(s, p, st, *, cancel_token=None):
        events.append(("stage", st is state, orch.active_run_control(s, p).cancel_token is cancel_token))
        raise RuntimeError("boom")

    monkeypatch.setattr(S, "run", fake_run)
    out = orch.run_production_comparison("s", "p", input_mode="FULL")
    assert out is state and out == COMPLETED
    assert events == ["lock", ("stage", True, True), "unlock"]  # its exception is swallowed
    assert orch.active_run_control("s", "p") is None


@pytest.mark.parametrize("env_vars,state", [
    ({S.FLAG: "0"}, COMPLETED),
    ({}, {**COMPLETED, "status": "FAILED"}),
    ({}, {**COMPLETED, "reason_code": "v3_failed"}),
    ({}, {**COMPLETED, "run_id": ""}),
])
def test_stage_does_not_start(monkeypatch, tmp_path, env_vars, state):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)
    before = copy.deepcopy(state)
    assert S.run("s", "p", state) is None
    assert state == before and S.read_status("s", "p") is None


def test_stage_is_on_by_default_and_skips_with_a_closed_gate(monkeypatch, tmp_path):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    assert S.enabled()
    out = S.run("s", "p", dict(COMPLETED))
    assert (out["status"], out["reason_code"], out["calls_done"]) == ("SKIPPED", "provider_unavailable", 0)
    assert S.read_status("s", "p")["status"] == "SKIPPED"
    monkeypatch.setenv(S.PROVIDER_ENV, "openrouter:any:high")
    assert S.run("s", "p", dict(COMPLETED))["reason_code"] == "bad_provider_config"


def test_default_provider_is_opus_55_without_calls():
    provider = S.provider_from_spec(S.DEFAULT_PROVIDER)
    assert (provider.model, provider.reasoning, provider.call_count) == ("claude-opus-5-5", "xhigh", 0)


def test_running_status_without_a_live_stage_is_interrupted(monkeypatch, tmp_path):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))
    S._write("s", "p", {"status": "RUNNING", "source_run_id": "r" * 32})
    assert S.public_status("s", "p", "r" * 32)["status"] == "INTERRUPTED"
    assert S.public_status("s", "p", "x" * 32) is None
    with S._LIVE_LOCK:
        S._LIVE.add(("s", "p"))
    try:
        assert S.public_status("s", "p", "r" * 32)["status"] == "RUNNING"
    finally:
        with S._LIVE_LOCK:
            S._LIVE.discard(("s", "p"))


def _run_pair(env):  # noqa: F811
    client, sid = env["client"], env["session_id"]
    state = client.post(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/run",
                        json={"input_mode": "DOCUMENT"}).json()
    assert state["reason_code"] == "v3_completed", state
    return client, sid, state


def test_stage_consolidates_the_completed_run_and_reports_progress(env):  # noqa: F811
    from backend.app.services.project_change_consolidator.view import pair_consolidations
    from backend.app.services.project_change_v3.provider import FakeProvider
    from backend.app.services.stage_comparison import paths

    client, sid, state = _run_pair(env)
    production = paths.production_dir(sid, gf.PAIR_ID)
    # Под тестовым провайдером V3 этап сам не идёт: модель в тестах прогона не вызывается.
    assert S.read_status(sid, gf.PAIR_ID) is None and not (production / SHADOW_DIR_NAME).exists()
    v3_state = json.loads((production / "state.json").read_text(encoding="utf-8"))

    fake = FakeProvider(handlers={C.STAGE: consolidate})
    out = S.run(sid, gf.PAIR_ID, state, provider=fake)
    assert out["status"] == "COMPLETED", out
    assert out["calls_total"] == out["calls_done"] == len(fake.calls) == 2
    assert json.loads((production / "state.json").read_text(encoding="utf-8")) == v3_state  # V3 не тронут
    [entry] = pair_consolidations(sid, gf.PAIR_ID)
    assert entry["consolidator_run_id"] == out["consolidator_run_id"]

    public = client.get(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/state").json()
    assert public["consolidation"]["status"] == "COMPLETED" and public["consolidation"]["live"] is False
    assert public["consolidation"]["consolidator_run_id"] == out["consolidator_run_id"]


def test_call_plan_above_the_limit_is_skipped_before_any_call(env, monkeypatch):  # noqa: F811
    from backend.app.services.project_change_v3.provider import FakeProvider

    _client, sid, state = _run_pair(env)
    monkeypatch.setenv(S.MAX_CALLS_ENV, "1")
    fake = FakeProvider(handlers={C.STAGE: consolidate})
    out = S.run(sid, gf.PAIR_ID, state, provider=fake)
    assert (out["status"], out["reason_code"], out["calls_total"]) == ("SKIPPED", "call_plan_exceeded", 2)
    assert fake.calls == []


def test_cancelled_stage_sends_nothing(env):  # noqa: F811
    from backend.app.services.project_change_v3.provider import FakeProvider
    from backend.app.services.stage_comparison.ai.gateway import CancelToken

    _client, sid, state = _run_pair(env)
    token = CancelToken()
    token.cancel()
    fake = FakeProvider(handlers={C.STAGE: consolidate})
    out = S.run(sid, gf.PAIR_ID, state, provider=fake, cancel_token=token)
    assert out["status"] == "CANCELLED" and fake.calls == []


def test_retry_endpoint_needs_a_completed_run(env):  # noqa: F811
    client, sid = env["client"], env["session_id"]
    response = client.post(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/consolidation")
    assert response.status_code == 409
