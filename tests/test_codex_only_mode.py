"""Режим «только Codex» (docs/codex_only_mode.md): ни одного вызова Claude."""
import json
import socket

import pytest

from backend.app.core import config
from backend.app.models.usage import LLMResult
from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo
from backend.app.services.audit_routing import compiler, describe, presets, registry
from tests.test_stage01_claude_leg import finding, project

MODE = config.STAGE01_CODEX_ONLY_MODEL_ID


def _codex_only_table():
    return presets.reference_config(presets.PRESET_CODEX_ONLY, codex_model_id=config.CODEX_STAGE_MODEL_ID)


@pytest.fixture
def isolated_stage01(monkeypatch):
    attempts = []

    def forbidden(*args, **kwargs):
        attempts.append(True)
        raise AssertionError("real network forbidden")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setenv("AUDIT_OPENROUTER_ENABLED", "0")
    monkeypatch.setattr(gfo, "STAGE01_PROTECTION_TABLE_CHECK_ENABLED", False)
    monkeypatch.setattr(gfo, "STAGE01_ABORT_ON_LEG_FAILURE_ENABLED", True)
    monkeypatch.setattr(gfo, "STAGE01_LEG_FAILURE_THRESHOLD", 1)
    monkeypatch.setattr(gfo, "STAGE01_LEG_RETRY_ATTEMPTS", 0)
    monkeypatch.setattr(gfo, "STAGE01_CLAUDE_LEG_ENABLED", True)
    monkeypatch.setattr(gfo, "provider_bridge_active", lambda: False)
    monkeypatch.setattr(gfo, "build_effective_block_context", lambda *_a, **_k: ("CTX", "raw_vector"))
    monkeypatch.setattr(gfo, "load_version_project_info",
                        lambda *_a, **_k: {"project_id": "synthetic", "section": "EOM"})
    calls = {"leg": [], "judge": [], "claude": []}

    async def leg(*args, **kwargs):
        calls["leg"].append(kwargs)
        return {"ok": True, "parsed": {"findings": [finding(kwargs["model"])]},
                "input_tokens": 1000, "output_tokens": 50, "elapsed_ms": 1}

    async def judge(messages, **kwargs):
        calls["judge"].append(kwargs)
        data = {"relationships": [], "gap_findings": [], "gap_search": {"performed": True}}
        return LLMResult(text=json.dumps(data), json_data=data, model=kwargs.get("model"),
                         input_tokens=300, output_tokens=40, duration_ms=1)

    async def claude(*args, **kwargs):
        calls["claude"].append(kwargs)
        raise AssertionError("Claude не должен вызываться в режиме «только Codex»")

    from backend.app.services.llm import claude_json_runner, codex_runner

    monkeypatch.setattr(gfo, "call_codex_for_block", leg)
    monkeypatch.setattr(gfo, "call_claude_for_block", claude)
    monkeypatch.setattr(codex_runner, "run_codex_json_messages", judge)
    monkeypatch.setattr(claude_json_runner, "run_claude_json_messages", claude)
    yield calls
    assert not attempts


@pytest.mark.asyncio
async def test_stage01_two_codex_legs_and_codex_judge(isolated_stage01, tmp_path):
    calls = isolated_stage01
    result = await gfo.run_findings_only_for_project(
        tmp_path, output_dir_override=project(tmp_path), model=MODE, timeout_s=5, write_run_log=False)

    assert [c["model"] for c in calls["leg"]] == list(config.STAGE01_CODEX_ONLY_LEG_MODELS)
    assert {c["reasoning_effort"] for c in calls["leg"]} == {"medium"}
    assert not calls["claude"]  # ЭОМ: прод добавил бы Opus-ногу, здесь её нет
    assert calls["judge"][0]["model"] == config.STAGE01_CODEX_ONLY_JUDGE_MODEL
    assert calls["judge"][0]["reasoning_effort"] == "medium"
    block = result["output_doc"]["block_analyses"][0]
    assert block["detectors_failed"] == []
    assert block["dual_review"]["status"] == "ok"
    assert result["summary"]["execution_receipt"]["openrouter_calls"] == 0


def test_stage01_mode_is_free_and_allowed():
    from backend.app.pipeline.manager import _stage01_model_spends_paid_api

    assert _stage01_model_spends_paid_api(MODE) is False
    assert MODE in config.STAGE_MODEL_RESTRICTIONS["block_batch"]
    assert config.OPTIMIZATION_CODEX_ONLY_MODEL_ID in config.STAGE_MODEL_RESTRICTIONS["optimization"]
    for stage, model in _codex_only_table().items():
        assert config.validate_stage_model_choice(stage, model) is None, (stage, model)


def test_preset_detected_and_prod_table_unchanged():
    table = _codex_only_table()
    assert presets.detect_preset(table, codex_model_id=config.CODEX_STAGE_MODEL_ID) == presets.PRESET_CODEX_ONLY
    from tests import _legacy_presets

    full = _legacy_presets.reference_config(_legacy_presets.FULL_CODEX)
    assert presets.detect_preset(full, codex_model_id=config.CODEX_STAGE_MODEL_ID) == presets.PRESET_CUSTOM
    assert all(not str(v).startswith("claude-") for v in table.values())


def test_plan_has_no_claude_action():
    plan = compiler.compile_from_center(stage_models=_codex_only_table(), env={"AUDIT_OPENROUTER_ENABLED": "false"})
    providers = {a.provider for st in plan.stages for a in st.actions if a.is_model}
    assert providers == {registry.PROVIDER_CODEX}


def test_describe_shows_real_codex_models(monkeypatch):
    monkeypatch.setattr(describe, "codex_cli_default_effort", lambda: "high")
    st = describe.describe_routing(_codex_only_table(), env={"AUDIT_OPENROUTER_ENABLED": "false"})["stages"]

    def acts(key):
        return {a["action_id"]: a for a in st[key]["actions"]}

    assert acts("block_batch")["judge_codex_only"]["model"] == config.STAGE01_CODEX_ONLY_JUDGE_MODEL
    assert acts("text_analysis")["text_audit"]["model"] == "codex/gpt-6.1-sol"
    assert acts("text_analysis")["text_audit"]["effort"] == "medium"  # не config.toml
    assert acts("findings_merge")["merge_base"]["model"] == "codex/gpt-6-sol"
    guard = acts("findings_corrector")["absence_guard"]
    assert (guard["provider"], guard["model"]) == ("codex", "codex/gpt-6.1-sol")
    primary = acts("optimization")["optimization_primary_codex"]
    assert (primary["model"], primary["effort"]) == ("codex/gpt-6.1-sol", "high")


def test_gpt61_sol_default_effort_is_medium():
    from backend.app.services.llm import codex_runner

    assert codex_runner._reasoning_effort_args(None, "gpt-6.1-sol") == ["-c", 'model_reasoning_effort="medium"']
    assert codex_runner._reasoning_effort_args("high", "gpt-6.1-sol") == ["-c", 'model_reasoning_effort="high"']
    assert codex_runner._reasoning_effort_args(None, "gpt-6-astra") == []


def test_absence_guard_goes_to_codex_in_mode(monkeypatch):
    from backend.app.pipeline.stages.text_analysis import absence_guard

    seen = []
    monkeypatch.setattr(absence_guard, "_verify_via_provider_bridge", lambda *a, **k: None)
    monkeypatch.setattr(config, "codex_only_mode_active", lambda: True)
    monkeypatch.setattr(absence_guard, "run_codex_verification",
                        lambda md, cands, timeout_sec: seen.append(len(cands)) or {0: "present"})
    monkeypatch.setattr(config, "get_claude_cli",
                        lambda: (_ for _ in ()).throw(AssertionError("Claude не нужен")))
    assert absence_guard.run_claude_verification("MD", [{"finding": "нет X"}]) == {0: "present"}
    assert seen == [1]


def test_optimization_merge_labels_primary_as_codex():
    from backend.app.pipeline.stages.optimization.ensemble import merge_optimization_documents

    item = {"id": "OPT-001", "current": "кабель А", "proposed": "кабель Б", "type": "cheaper_analog"}
    merged, report = merge_optimization_documents(
        {"meta": {}, "items": [item]}, {"meta": {}, "items": []}, run_id="r",
        claude_model="codex/gpt-6.1-sol", primary_provider="codex",
    )
    prov = merged["items"][0]["provenance"]
    assert prov["found_by"] == ["codex"]
    assert prov["detections"][0]["model"] == "codex/gpt-6.1-sol"
    assert merged["meta"]["ensemble"]["mode"] == "codex_codex"
