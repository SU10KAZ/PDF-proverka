"""Exercise planning, receipts and recovery through the pipeline with fake inference."""
import json
from pathlib import Path

from backend.app.services.project_change_v3 import contracts, engine, mapper_planning, miner_plan, provenance, run_storage
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401
from backend.tests.project_change_v3.test_mapper_only_handoff import planned  # noqa: F401
from backend.tests.project_change_v3.test_mapper_portions import _portion_handlers
from backend.tests.project_change_v3.test_resume_and_dedupe_repair import _run, _stored


def test_balanced_runtime_persists_exact_packet_keys_and_completion(env, monkeypatch):
    monkeypatch.setenv(mapper_planning.MODE_ENV, "balanced")
    monkeypatch.setenv(miner_plan.BALANCED_ENV, "balanced")
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "astra")
    prepare = engine.prepare_comparison_sources

    def heavy(**kwargs):
        prepared = prepare(**kwargs)
        for p in prepared["structure"]:
            p["synthetic_text"] = p["side"] + str(p["physical_page"]) + "Ж" * 70000
        return prepared

    monkeypatch.setattr(engine, "prepare_comparison_sources", heavy)
    fake = FakeProvider(handlers=_portion_handlers([]))
    set_test_provider(fake)
    state = _run(env["session_id"])
    assert state["status"] in {"REVIEW", "COMPLETED"}, state
    plan = state["provenance"]["mapper_plan"]
    assert plan["mode"] == "balanced" and plan["complete"]
    assert plan["accepted_portions"] == [1, 2, 3, 4]
    attempts = [json.loads(Path(row["path"]).read_text()) for row in state["provenance"]["mapper_attempts"]]
    assert [r["request_key"] for r in attempts] == [r["request_key"] for r in plan["final_packets"]]
    assert len({r["request_key"] for r in attempts}) == 4
    assert _stored(env["session_id"], "project_change_v3_mapper_plan") == plan
    assert _stored(env["session_id"], "project_change_v3_miner_plan")["new_calls"] == 2
    assert state["provenance"]["miner_plan"]["estimated_new_input_tokens"] > 0


def test_unknown_planner_mode_stops_without_inference(env, monkeypatch):
    monkeypatch.setenv(mapper_planning.MODE_ENV, "typo")
    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)
    state = _run(env["session_id"])
    assert state["reason_code"] == "mapper_planner_invalid"
    assert not fake.calls


def test_payload_version_drift_refuses_saved_answers(env, planned, monkeypatch):
    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)
    monkeypatch.setattr(provenance, "SOURCE_PAYLOAD_VERSION", "different representation")
    state = _run(env["session_id"], resume_from_run_id=planned["run_id"], model_profile="opus55")
    assert state["reason_code"] == "resume_refused", state
    assert "source_payload_version" in state["message"] and not fake.calls


def test_rejected_portion_leaves_the_remaining_plan_open(env, monkeypatch):
    monkeypatch.setenv(mapper_planning.MODE_ENV, "legacy")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_MAPPER_CONCURRENCY", "1")
    monkeypatch.setattr(engine, "mapper_portions_mod_single_envelope_error", lambda *_: "force two portions")
    monkeypatch.setattr(engine, "mapper_portions_mod_fits",
                        lambda pair, pages: sum(p["side"] == "NEW" for p in pages) <= 1)
    handlers = _portion_handlers([])
    original = handlers["MAPPING"]

    def mapping(**kwargs):
        if kwargs["data"]["portion"]["index"] == 2:
            raise ProviderError("TEST_FAILURE", "synthetic failure")
        return original(**kwargs)

    set_test_provider(FakeProvider(handlers={**handlers, "MAPPING": mapping}))
    state = _run(env["session_id"])
    assert state["status"] == "FAILED"
    path = run_storage.artifact_path(env["session_id"], gf.PAIR_ID, "project_change_v3_mapper_plan", state["run_id"])
    plan = json.loads(path.read_text())
    assert plan["accepted_portions"] == [1] and plan["complete"] is False
    assert _stored(env["session_id"], "project_change_v3_result") is None
