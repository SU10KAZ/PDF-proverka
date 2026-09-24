"""Phase D4: the production hook is OFF by default and can never change the V3 state (0 model calls)."""
from __future__ import annotations

import contextlib
import copy
import sys
from pathlib import Path

import pytest

from backend.app.services.project_change_consolidator import hook as H

HOOK_MODULE = "backend.app.services.project_change_consolidator.hook"
COMPLETED = {"status": "COMPLETED", "reason_code": "v3_completed", "run_id": "r" * 32, "session_id": "s", "pair_id": "p"}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (H.FLAG, H.PROVIDER_ENV, H.MAX_CALLS_ENV):
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


def test_flag_off_by_default_does_not_import_the_consolidator(monkeypatch):
    events = []
    state = copy.deepcopy(COMPLETED)
    orch = _orchestrator(monkeypatch, state, events)
    monkeypatch.delitem(sys.modules, HOOK_MODULE, raising=False)
    out = orch.run_production_comparison("s", "p", input_mode="FULL")
    assert out is state and out == COMPLETED and events == ["lock", "unlock"]
    assert HOOK_MODULE not in sys.modules


def test_flag_on_runs_hook_after_the_pair_lock_and_keeps_state(monkeypatch):
    events = []
    state = copy.deepcopy(COMPLETED)
    orch = _orchestrator(monkeypatch, state, events)
    monkeypatch.setenv(H.FLAG, "1")
    monkeypatch.setattr(H, "after_v3_run", lambda s, p, st: events.append(("hook", st is state)) or 1 / 0)
    out = orch.run_production_comparison("s", "p", input_mode="FULL")
    assert out is state and out == COMPLETED
    assert events == ["lock", "unlock", ("hook", True)]  # after the lock; its exception is swallowed


@pytest.mark.parametrize("env,state,status", [
    ({}, COMPLETED, "SKIPPED_FLAG_OFF"),
    ({H.FLAG: "true"}, COMPLETED, "SKIPPED_FLAG_OFF"),
    ({H.FLAG: "1"}, {**COMPLETED, "status": "FAILED"}, "SKIPPED_SOURCE_NOT_COMPLETED"),
    ({H.FLAG: "1"}, {**COMPLETED, "reason_code": "v3_failed"}, "SKIPPED_SOURCE_NOT_COMPLETED"),
    ({H.FLAG: "1"}, COMPLETED, "SKIPPED_NO_PROVIDER_CONFIG"),
    ({H.FLAG: "1", H.PROVIDER_ENV: "openrouter:any:high"}, COMPLETED, "SKIPPED_BAD_PROVIDER_CONFIG"),
    ({H.FLAG: "1", H.PROVIDER_ENV: "claude_code_cli:claude-opus-5:xhigh",
      "PROJECT_COMPARISON_V3_ALLOW_INFERENCE": "0"}, COMPLETED, "SKIPPED_INFERENCE_GATE"),
])
def test_hook_gates(monkeypatch, env, state, status):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    before = copy.deepcopy(state)
    assert H.after_v3_run("s", "p", state, start=False)["status"] == status
    assert state == before


def test_open_gate_with_explicit_provider_is_queued_without_calls(monkeypatch):
    from backend.app.services.project_change_v3 import provider_gate

    monkeypatch.setenv(H.FLAG, "1")
    monkeypatch.setenv(H.PROVIDER_ENV, "claude_code_cli:claude-opus-5:xhigh")
    monkeypatch.setattr(provider_gate, "check_provider_readiness", lambda: {"available": True})
    out = H.after_v3_run("s", "p", dict(COMPLETED), start=False)
    assert out == {"status": "QUEUED", "run_id": COMPLETED["run_id"]}
    provider = H.provider_from_spec("claude_code_cli:claude-opus-5:xhigh")
    assert (provider.model, provider.reasoning) == ("claude-opus-5", "xhigh") and provider.call_count == 0


def test_readers_never_import_the_consolidator():
    root = Path(__file__).resolve().parents[2] / "app" / "services"
    readers = [root / "project_change_v3" / "presentation.py", root / "project_change_v3" / "human_mapping_bridge.py",
               *sorted((root / "project_change_catalog").rglob("*.py")),
               *sorted((root / "human_mapping_production").rglob("*.py"))]
    assert readers
    for path in readers:
        assert "project_change_consolidator" not in path.read_text(encoding="utf-8"), path
