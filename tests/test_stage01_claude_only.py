"""Stage 01 «только Claude»: одна нога Opus + судья Sonnet с gap-search."""
import json
import socket

import pytest

from backend.app.core import config
from backend.app.models.usage import LLMResult
from backend.app.pipeline.stages.block_analysis import dual_review, gemma_findings_only as gfo
from tests.test_stage01_claude_leg import finding, project

MODE = config.STAGE01_CLAUDE_ONLY_MODEL_ID


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
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
    monkeypatch.setattr(gfo, "provider_bridge_active", lambda: False)
    monkeypatch.setattr(gfo, "build_effective_block_context", lambda *_a, **_k: ("CTX", "raw_vector"))
    monkeypatch.setattr(gfo, "load_version_project_info",
                        lambda *_a, **_k: {"project_id": "synthetic", "section": "OV"})
    yield
    assert not attempts


def install(monkeypatch, *, judge_data=None, judge_error=None):
    calls = {"leg": [], "judge": [], "codex": []}

    async def leg(*args, **kwargs):
        calls["leg"].append(kwargs)
        return {"ok": True, "parsed": {"findings": [finding("Opus")]},
                "input_tokens": 1000, "output_tokens": 50, "elapsed_ms": 1}

    async def judge(messages, **kwargs):
        calls["judge"].append((messages, kwargs))
        if judge_error:
            return LLMResult(text=judge_error, is_error=True, error_message=judge_error, model="claude/x")
        data = judge_data or {"relationships": [], "gap_findings": [], "gap_search": {"performed": True}}
        return LLMResult(text=json.dumps(data), json_data=data, model="claude/claude-sonnet-5-5",
                         input_tokens=300, output_tokens=40, duration_ms=1)

    async def codex(*args, **kwargs):
        calls["codex"].append(kwargs)
        raise AssertionError("Codex не должен вызываться в режиме «только Claude»")

    from backend.app.services.llm import claude_json_runner, codex_runner

    monkeypatch.setattr(gfo, "call_claude_for_block", leg)
    monkeypatch.setattr(gfo, "call_codex_for_block", codex)
    monkeypatch.setattr(claude_json_runner, "run_claude_json_messages", judge)
    monkeypatch.setattr(codex_runner, "run_codex_json_messages", codex)
    return calls


@pytest.mark.asyncio
async def test_opus_leg_then_sonnet_gap_search(monkeypatch, tmp_path):
    gap = {**finding("Gap"), "finding": "Пропущено: нет отметки уклона трубы"}
    calls = install(monkeypatch, judge_data={
        "relationships": [{"left_ref": "x", "right_ref": "y", "relation": "match"}],
        "gap_findings": [gap],
        "gap_search": {"performed": True, "status": "gaps_found"},
    })
    result = await gfo.run_findings_only_for_project(
        tmp_path, output_dir_override=project(tmp_path), model=MODE, timeout_s=5, write_run_log=False)

    assert len(calls["leg"]) == 1 and not calls["codex"]
    assert calls["leg"][0]["model"] == config.STAGE01_CLAUDE_ONLY_LEG_MODEL
    assert calls["leg"][0]["reasoning_effort"] == "medium"
    messages, judge_kwargs = calls["judge"][0]
    assert judge_kwargs["model"] == config.STAGE01_CLAUDE_ONLY_JUDGE_MODEL
    assert judge_kwargs["reasoning_effort"] == "medium"
    assert judge_kwargs["image_paths"]  # судья смотрит изображение блока
    assert messages[0]["content"] == dual_review.SINGLE_REVIEW_SYSTEM_PROMPT

    block = result["output_doc"]["block_analyses"][0]
    review = block["dual_review"]
    assert review["mode"] == dual_review.SINGLE_LEG_MODE
    assert review["relationships"] == []  # сравнивать не с чем: связи судьи отброшены
    assert review["counts"]["gap_findings"] == 1
    texts = [f["finding"] for f in block["findings"] + block["deferred_findings"]]
    assert any("Opus" in t for t in texts) and any("уклона" in t for t in texts)
    assert result["summary"]["execution_receipt"]["openrouter_calls"] == 0


@pytest.mark.asyncio
async def test_judge_failure_keeps_leg_findings(monkeypatch, tmp_path):
    install(monkeypatch, judge_error="claude_json_not_found")
    result = await gfo.run_findings_only_for_project(
        tmp_path, output_dir_override=project(tmp_path), model=MODE, timeout_s=5, write_run_log=False)

    block = result["output_doc"]["block_analyses"][0]
    assert block["detectors_failed"] == []
    assert block["dual_review"]["status"] == "fallback"
    assert block["dual_review"]["mode"] == dual_review.SINGLE_LEG_MODE
    texts = [f["finding"] for f in block["findings"] + block["deferred_findings"]]
    assert any("Opus" in t for t in texts)


def test_mode_is_free_of_paid_api_and_allowed_on_blocks():
    from backend.app.pipeline.manager import _stage01_model_spends_paid_api

    assert _stage01_model_spends_paid_api(MODE) is False
    assert MODE in config.STAGE_MODEL_RESTRICTIONS["block_batch"]
