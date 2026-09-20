"""A V3 run is visible while it works and can be stopped — zero model calls.

Live acceptance of the first real V3 comparison (2026-09-20) showed two
job-state defects of the production path:

* the state said ``V3: подготовка источников`` for the whole multi-hour run
  (it was written once, before the Mapper), so nothing told the user which
  stage was working;
* ``POST …/production/cancel`` answered ``run_not_owned_by_this_process``: the
  V3 branch never registered a run control, so a started run could not be
  stopped — neither between calls nor inside the CLI session in flight.

Neither fix touches what the model sees (prompts, data, images, schema).
"""
from __future__ import annotations

import os
import stat
import threading
import time

import pytest

from backend.tests.project_change_v3 import generic_fixture as gf


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    built = gf.build_comparison(tmp_path)
    from backend.app.services.project_change_v3 import scope

    monkeypatch.setattr(scope, "_object_stage_paths", lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})
    from backend.app.services.stage_comparison import production_orchestrator as orch

    def _legacy(*_a, **_k):
        raise AssertionError("legacy comparison invoked")

    monkeypatch.setattr(orch, "_run_production_comparison_locked", _legacy)
    monkeypatch.setattr(orch, "_run_production_comparison_impl", _legacy)
    yield built
    from backend.app.services.project_change_v3.provider import reset_test_provider

    reset_test_provider()


def _run(built, provider):
    from backend.app.services.project_change_v3.provider import set_test_provider
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison

    set_test_provider(provider)
    return run_production_comparison(built["session_id"], gf.PAIR_ID, input_mode="DOCUMENT")


def _stored(built, name="state"):
    from backend.app.services.stage_comparison import production_store

    return production_store.load_artifact(built["session_id"], gf.PAIR_ID, name, include_domain_keys=True)


def _fake(handlers=None):
    from backend.app.services.project_change_v3.provider import FakeProvider

    return FakeProvider(handlers=handlers or gf.fake_handlers())


def _record_states(monkeypatch):
    from backend.app.services.stage_comparison import production_store

    real, seen = production_store.save_artifact, []

    def save(session_id, pair_id, name, payload):
        if name == "state":
            seen.append(dict(payload))
        return real(session_id, pair_id, name, payload)

    monkeypatch.setattr(production_store, "save_artifact", save)
    return seen


# ── Progress ───────────────────────────────────────────────────────────────

def test_running_state_names_every_stage(env, monkeypatch):
    seen = _record_states(monkeypatch)
    final = _run(env, _fake())
    assert final["status"] == "REVIEW"
    running = [s for s in seen if s["status"] == "RUNNING"]
    stages = [s.get("current_stage") for s in running]
    regions = len(_stored(env, "project_change_v3_semantic_map")["regions"])
    assert stages == [None, "MAPPING", *["MINING"] * regions, "DEDUPE"]
    mining = [s for s in running if s.get("current_stage") == "MINING"]
    assert [(s["processed"], s["total"]) for s in mining] == [(i, regions) for i in range(regions)]
    assert all(s["unit"] == "region" and s["current_item"] for s in mining)
    # One run, one identity: progress never forks run_id / started_at and never claims success.
    assert {s["run_id"] for s in seen} == {final["run_id"]}
    assert {s["started_at"] for s in seen} == {final["started_at"]}
    assert all(s["completed_at"] is None and s["legacy_invoked"] is False for s in running)
    assert [s["status"] for s in seen][-1] == "REVIEW" and seen[-1]["current_stage"] is None


def test_progress_that_cannot_be_persisted_stops_the_run(env, monkeypatch):
    from backend.app.services.stage_comparison import production_store

    real = production_store.save_artifact

    def save(session_id, pair_id, name, payload):
        if name == "state" and payload.get("current_stage") == "MINING":
            raise OSError("read-only")
        return real(session_id, pair_id, name, payload)

    monkeypatch.setattr(production_store, "save_artifact", save)
    provider = _fake()
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "state_persistence_failed"
    assert _stored(env)["status"] == "FAILED"
    assert [c["stage"] for c in provider.calls] == ["MAPPING"]  # never mined blind
    assert _stored(env, "project_change_v3_result") is None


# ── Cancel ─────────────────────────────────────────────────────────────────

def test_cancel_between_calls_stops_before_the_next_stage(env):
    from backend.app.services.stage_comparison.production_orchestrator import (
        active_run_control,
        cancel_production_comparison,
    )

    handlers, answers = gf.fake_handlers(), {}

    def mapping(**kwargs):  # the user presses "Остановить анализ" while the Mapper works
        answers["cancel"] = cancel_production_comparison(env["session_id"], gf.PAIR_ID, requested_by="engineer")
        return handlers["MAPPING"](**kwargs)

    provider = _fake({**handlers, "MAPPING": mapping})
    state = _run(env, provider)
    assert answers["cancel"]["cancelled"] is True and answers["cancel"]["reason_code"] == "cancel_requested"
    assert answers["cancel"]["run_id"] == state["run_id"]
    assert state["status"] == "FAILED" and state["reason_code"] == "v3_cancelled"
    assert "остановлен" in state["message"] and state["legacy_invoked"] is False
    assert [c["stage"] for c in provider.calls] == ["MAPPING"]
    assert _stored(env)["status"] == "FAILED"
    assert _stored(env, "project_change_v3_result") is None
    assert active_run_control(env["session_id"], gf.PAIR_ID) is None
    from backend.app.services.project_change_v3 import presentation

    assert presentation.published_run(env["session_id"], gf.PAIR_ID) is None


@pytest.mark.parametrize("code", ["CANCELLED", "provider_cancelled"])
def test_cancel_reported_by_either_transport_is_v3_cancelled(env, code):
    """exec reports ``CANCELLED``, app-server ``provider_cancelled``: one user cancel, one outcome."""
    from backend.app.services.project_change_v3.provider import ProviderError
    from backend.app.services.stage_comparison.production_orchestrator import cancel_production_comparison

    def mining(**_kw):
        assert cancel_production_comparison(env["session_id"], gf.PAIR_ID)["cancelled"] is True
        raise ProviderError(code, "отменено")  # what the transport says about the killed call

    state = _run(env, _fake({**gf.fake_handlers(), "MINING": mining}))
    assert state["status"] == "FAILED" and state["reason_code"] == "v3_cancelled"


def test_provider_cancel_without_a_user_cancel_keeps_its_own_code(env):
    from backend.app.services.project_change_v3.provider import ProviderError

    def mining(**_kw):
        raise ProviderError("provider_cancelled", "cancelled elsewhere")

    state = _run(env, _fake({**gf.fake_handlers(), "MINING": mining}))
    assert state["status"] == "FAILED" and state["reason_code"] == "provider_cancelled"


def test_run_control_is_released_after_every_outcome(env):
    from backend.app.services.project_change_v3.provider import ProviderError
    from backend.app.services.stage_comparison.production_orchestrator import (
        active_run_control,
        cancel_production_comparison,
    )

    def boom(**_kw):
        raise ProviderError("provider_quota", "quota exhausted")

    assert _run(env, _fake({**gf.fake_handlers(), "DEDUPE": boom}))["reason_code"] == "provider_quota"
    assert active_run_control(env["session_id"], gf.PAIR_ID) is None
    assert _run(env, _fake())["status"] == "REVIEW"
    assert active_run_control(env["session_id"], gf.PAIR_ID) is None
    late = cancel_production_comparison(env["session_id"], gf.PAIR_ID)
    assert late["cancelled"] is False and late["reason_code"] == "no_active_run"
    assert _stored(env)["status"] == "REVIEW"  # a late cancel never rewrites a finished run


def test_cancel_kills_the_cli_session_in_flight(env, tmp_path, monkeypatch):
    """Orchestrator → engine → CodexProvider → gateway: the token reaches the live CLI process."""
    import sys
    import types

    try:
        import jsonschema  # noqa: F401
    except ImportError:
        monkeypatch.setitem(sys.modules, "jsonschema", types.SimpleNamespace(validate=lambda *_a: None))
    slow = tmp_path / "slow_codex.py"
    slow.write_text("#!/usr/bin/env python3\nimport sys, time\nsys.stdin.buffer.read()\ntime.sleep(300)\n",
                    encoding="utf-8")
    slow.chmod(slow.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("STAGE_COMPARISON_AI_CODEX_BIN", str(slow))
    from backend.app.services.project_change_v3.provider import CodexProvider
    from backend.app.services.stage_comparison.ai import gateway
    from backend.app.services.stage_comparison.production_orchestrator import (
        active_run_control,
        cancel_production_comparison,
    )

    handlers = gf.fake_handlers()

    class MiningThroughGateway(CodexProvider):
        def complete(self, **kwargs):
            if kwargs["stage"] != "MINING":
                self.last_transport = None
                return handlers[kwargs["stage"]](**{k: kwargs[k] for k in ("call_id", "pair_id", "data",
                                                                           "schema", "images")})
            return super().complete(**kwargs)

    outcome: dict = {}
    worker = threading.Thread(target=lambda: outcome.update(state=_run(env, MiningThroughGateway())), daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 30
        while gateway.live_process_count() == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert gateway.live_process_count() == 1, "the Miner CLI session never started"
        control = active_run_control(env["session_id"], gf.PAIR_ID)
        assert control is not None and _stored(env)["run_id"] == control.run_id
        started = time.monotonic()
        answer = cancel_production_comparison(env["session_id"], gf.PAIR_ID, requested_by="engineer")
        worker.join(timeout=30)
        assert not worker.is_alive(), "cancel did not stop the run"
    finally:
        gateway.kill_live_processes()  # a failing assertion must not leave the slow CLI behind
        worker.join(timeout=30)
    assert time.monotonic() - started < 15
    assert answer["cancelled"] is True and answer["run_id"] == control.run_id
    state = outcome["state"]
    assert state["status"] == "FAILED" and state["reason_code"] == "v3_cancelled"
    assert gateway.live_process_count() == 0
    assert _stored(env)["status"] == "FAILED" and _stored(env, "project_change_v3_result") is None
    last = state["provenance"]["transport_calls"][-1]  # the killed call is receipted, without usage
    assert last["stage"] == "MINING" and last["provider_ok"] is False and last["usage"] is None
    assert active_run_control(env["session_id"], gf.PAIR_ID) is None
