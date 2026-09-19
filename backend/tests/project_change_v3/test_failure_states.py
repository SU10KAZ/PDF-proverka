"""No silent COMPLETED: every V3 failure is FAILED/REVIEW, logged, never legacy."""
from __future__ import annotations

import json
import logging

import pytest

from backend.tests.project_change_v3 import generic_fixture as gf


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "0")
    built = gf.build_comparison(tmp_path)
    from backend.app.services.project_change_v3 import scope

    monkeypatch.setattr(scope, "_object_stage_paths",
                        lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})
    # Legacy must never run from the V3 entry.
    from backend.app.services.stage_comparison import production_orchestrator as orch

    def _legacy(*_a, **_k):
        raise AssertionError("legacy comparison invoked")

    monkeypatch.setattr(orch, "_run_production_comparison_locked", _legacy)
    monkeypatch.setattr(orch, "_run_production_comparison_impl", _legacy)
    from backend.app.services.project_change_v3.provider import FakeProvider, reset_test_provider, set_test_provider

    set_test_provider(FakeProvider(handlers=gf.fake_handlers()))
    try:
        yield built
    finally:
        reset_test_provider()


def _run(session_id):
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison

    return run_production_comparison(session_id, gf.PAIR_ID, input_mode="DOCUMENT")


def _stored(session_id, name="state"):
    from backend.app.services.stage_comparison import production_store

    return production_store.load_artifact(session_id, gf.PAIR_ID, name, include_domain_keys=True)


def test_happy_path_is_review_with_hint(env):
    state = _run(env["session_id"])
    assert state["status"] == "REVIEW" and state["reason_code"] == "v3_completed"
    assert _stored(env["session_id"])["status"] == "REVIEW"


def test_source_preparation_failure_is_failed(env, caplog):
    import os

    os.remove(env["left"]["pdf_path"].replace("document.pdf", "blocks.json"))
    with caplog.at_level(logging.ERROR):
        state = _run(env["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "source_preparation_failed"
    assert _stored(env["session_id"])["status"] == "FAILED"
    assert _stored(env["session_id"], "project_change_v3_result") is None
    assert any("source_preparation_failed" in r.getMessage() for r in caplog.records)


def test_missing_markdown_is_failed_not_empty_text(env):
    import os

    os.remove(env["right"]["pdf_path"].replace("document.pdf", "document.md"))
    state = _run(env["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "source_preparation_failed"
    assert "markdown" in state["message"]


def test_result_persistence_failure_is_failed(env, monkeypatch):
    from backend.app.services.stage_comparison import production_store

    real = production_store.save_artifact

    def save(session_id, pair_id, name, payload):
        if name == "project_change_v3_result":
            raise OSError("disk full")
        return real(session_id, pair_id, name, payload)

    monkeypatch.setattr(production_store, "save_artifact", save)
    state = _run(env["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "result_persistence_failed"
    assert _stored(env["session_id"])["status"] == "FAILED"


def test_state_persistence_failure_never_reports_completed(env, monkeypatch, caplog):
    from backend.app.services.stage_comparison import production_store

    real = production_store.save_artifact

    def save(session_id, pair_id, name, payload):
        if name == "state" and payload.get("status") in {"COMPLETED", "REVIEW"}:
            raise OSError("read-only")
        return real(session_id, pair_id, name, payload)

    monkeypatch.setattr(production_store, "save_artifact", save)
    with caplog.at_level(logging.ERROR):
        state = _run(env["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "state_persistence_failed"
    assert state["attempted_status"] == "REVIEW"
    assert _stored(env["session_id"])["status"] == "FAILED"
    assert any("state persistence failed" in r.getMessage() for r in caplog.records)
    from backend.app.services.project_change_v3 import presentation

    assert presentation.published_run(env["session_id"], gf.PAIR_ID) is None


def test_human_mapping_publish_failure_is_review(env, monkeypatch, caplog):
    from backend.app.services.project_change_v3 import scope

    def unresolved(_session_id):
        raise scope.ScopeUnresolved("no owner")

    monkeypatch.setattr(scope, "object_id_for_session", unresolved)
    with caplog.at_level(logging.ERROR):
        state = _run(env["session_id"])
    assert state["status"] == "REVIEW" and state["reason_code"] == "v3_human_mapping_unavailable"
    assert state["human_mapping_published"] is False and "ScopeUnresolved" in state["human_mapping_error"]
    assert any("Human Mapping publication failed" in r.getMessage() for r in caplog.records)


def test_provider_error_is_failed(env):
    from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, set_test_provider

    def boom(**_kw):
        raise ProviderError("provider_quota", "quota exhausted")

    set_test_provider(FakeProvider(handlers={**gf.fake_handlers(), "MINING": boom}))
    state = _run(env["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "provider_quota"
    assert state["legacy_invoked"] is False


def test_unexpected_crash_never_stays_running(env, monkeypatch):
    from backend.app.services.project_change_v3 import engine

    def crash(**_kw):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(engine, "optimized_region_bundle", crash)
    state = _run(env["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "v3_internal_error"
    assert _stored(env["session_id"])["status"] == "FAILED"


def test_closed_gate_fails_without_cli_probe_or_legacy(env, monkeypatch):
    from backend.app.services.project_change_v3.provider import reset_test_provider
    from backend.app.services.stage_comparison.ai import gateway

    reset_test_provider()
    monkeypatch.setattr(gateway, "validate_runtime", lambda **_k: pytest.fail("CLI probed while gate closed"))
    monkeypatch.setattr(gateway, "call_codex", lambda *_a, **_k: pytest.fail("model called"))
    state = _run(env["session_id"])
    assert state["status"] == "FAILED" and state["reason_code"] == "v3_inference_kill_switch"
    assert state["model_calls"] == 0
    assert state["provenance"]["provider_gate"]["gateway_detail"] == "not_checked"


def test_open_gate_requires_vision_capable_runtime(monkeypatch):
    from backend.app.services.project_change_v3.provider_gate import check_provider_readiness
    from backend.app.services.stage_comparison.ai import gateway

    seen = {}
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "1")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "0")
    monkeypatch.delenv("PROJECT_COMPARISON_V3_PROVIDER_READY", raising=False)

    def runtime(**kwargs):
        seen.update(kwargs)
        return {"ok": False, "problems": ["codex CLI не поддерживает --image (vision)"]}

    monkeypatch.setattr(gateway, "validate_runtime", runtime)
    gate = check_provider_readiness()
    assert seen == {"require_vision": True, "deep": False, "require_json_events": True}
    assert gate["available"] is False and gate["reason"] == "provider_not_ready"
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1")
    assert check_provider_readiness()["reason"] == "deploy_force_unavailable"


def test_result_metadata_versions(env):
    _run(env["session_id"])
    result = _stored(env["session_id"], "project_change_v3_result")
    prov = result["provenance"]
    for key in ("engine_version", "mapper_prompt_version", "miner_prompt_version", "dedupe_version",
                "source_packaging_version", "schema_version", "model", "reasoning",
                "mapper_prompt_sha256", "miner_prompt_sha256", "dedupe_prompt_sha256", "structure_sha256"):
        assert prov.get(key), key
    assert (prov["model"], prov["reasoning"]) == ("gpt-6-astra", "xhigh")
    assert result["schema"] == "projectchange_v3_final/2" and result["run_id"]
    assert json.dumps(result, ensure_ascii=False).count("legacy_invoked") == 1
