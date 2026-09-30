"""3.9.0: модель ProjectChange V3 выбирается на прогон из разрешённого списка."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.project_change_catalog.catalog import model_display
from backend.app.services.project_change_v3 import contracts, engine, provenance, provider, provider_gate, transport


@pytest.fixture
def choices(monkeypatch):
    monkeypatch.setenv("PROJECT_COMPARISON_V3_MODEL_CHOICES", "astra,opus55")
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "astra")


def test_choices_list_default_first_and_reject_unknown(choices, monkeypatch):
    assert [p.key for p in contracts.profile_choices()] == ["astra", "opus55"]
    assert contracts.resolve_profile(None).model == "gpt-6-astra"
    assert contracts.resolve_profile("opus55").model == "claude-opus-5-5"
    with pytest.raises(ValueError):
        contracts.resolve_profile("opus5")  # есть в коде, но не разрешён установкой
    monkeypatch.setenv("PROJECT_COMPARISON_V3_MODEL_CHOICES", "astra,gpt-7")
    with pytest.raises(ValueError):
        contracts.profile_choices()


def test_without_choices_only_the_startup_default_is_offered(monkeypatch):
    monkeypatch.delenv("PROJECT_COMPARISON_V3_MODEL_CHOICES", raising=False)
    assert [p.key for p in contracts.profile_choices()] == [contracts.DEFAULT_PROFILE_KEY]


def test_provenance_transport_and_provider_follow_the_run_profile(choices, monkeypatch):
    monkeypatch.setenv("PROJECT_COMPARISON_V3_CLAUDE_BIN", "/pinned/2.1.283/claude")
    provider.reset_test_provider()
    with contracts.use_profile(contracts.resolve_profile("opus55")):
        prov = provenance.build_provenance()
        chosen = provider.get_provider()
        assert transport.provider_transport_version() == transport.CLAUDE_TRANSPORT_VERSION
    assert prov["model"] == "claude-opus-5-5"
    assert prov["model_profile"] == "opus55"
    assert prov["engine_variant"] == "ProjectChange V3 / Opus 5.5"
    assert prov["thinking"]["type"] == "adaptive"
    assert prov["transport_version"] == transport.CLAUDE_TRANSPORT_VERSION
    assert isinstance(chosen, provider.ClaudeOpusProvider)
    assert (chosen.model, chosen.binary) == ("claude-opus-5-5", "/pinned/2.1.283/claude")

    # Вне прогона — снова модель установки.
    outside = provenance.build_provenance()
    assert outside["model"] == "gpt-6-astra"
    assert outside["transport_version"] == transport.CODEX_TRANSPORT_VERSION
    assert isinstance(provider.get_provider(), provider.CodexProvider)


def test_readiness_check_uses_the_run_profile_and_its_cli(choices, monkeypatch):
    seen = {}
    from backend.app.services.stage_comparison.ai import gateway

    def fake_claude(*, reasoning_level=None, binary=None):
        seen["claude"] = binary
        return {"ok": True}

    monkeypatch.setattr(gateway, "validate_claude_runtime", fake_claude)
    monkeypatch.setattr(gateway, "validate_runtime", lambda **_: seen.setdefault("codex", True) and {"ok": True})
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "1")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_CLAUDE_BIN", "/pinned/claude")
    with contracts.use_profile(contracts.resolve_profile("opus55")):
        gate = provider_gate.check_provider_readiness()
    assert gate["available"] and gate["model"] == "claude-opus-5-5" and gate["model_profile"] == "opus55"
    assert seen == {"claude": "/pinned/claude"}


def test_production_entry_binds_the_profile_for_the_whole_run(choices, monkeypatch):
    captured = {}

    def fake_stored(**kwargs):
        captured["model"] = contracts.active_profile().model
        captured["kwargs"] = kwargs
        return {"status": "COMPLETED"}

    monkeypatch.setattr(engine, "_run_v3_pipeline_stored", fake_stored)
    engine.run_v3_production_comparison("s", "p", model_profile="opus55", run_id="r1")
    assert captured["model"] == "claude-opus-5-5"
    assert "model_profile" not in captured["kwargs"]
    assert contracts.active_profile().model == "gpt-6-astra"

    engine.run_v3_production_comparison("s", "p", run_id="r2")
    assert captured["model"] == "gpt-6-astra"


def test_api_offers_models_and_refuses_a_disallowed_one(choices):
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)

    modes = client.get("/api/stage-comparison/production/ai-modes").json()
    assert modes["models"]["default"] == "astra"
    assert [m["code"] for m in modes["models"]["items"]] == ["astra", "opus55"]

    refused = client.post("/api/stage-comparison/sessions/s/pairs/p/production/run",
                          json={"input_mode": "DOCUMENT", "model_profile": "opus5"})
    assert refused.status_code == 400


def test_catalog_shows_opus_55_as_a_version():
    assert model_display("claude-opus-5-5") == "Claude Opus 5.5"
    assert model_display("claude-opus-5") == "Claude Opus 5"
    assert model_display("gpt-6-astra") == "GPT-6 Astra"
