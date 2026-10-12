"""Optional Claude leg of Stage 01 (replacement for the disabled OpenRouter leg)."""
import json
import socket
from types import SimpleNamespace

import pytest

from backend.app.models.usage import LLMResult
from backend.app.pipeline.stages.block_analysis import dual_review, gemma_findings_only as gfo
from backend.app.pipeline.stages.block_analysis.ensemble_mode import ASTRA_MODEL, SOL_MODEL

CLAUDE = "claude-opus-5-5"


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    attempts = []

    def forbidden(*args, **kwargs):
        attempts.append(True)
        raise AssertionError("real network forbidden")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.delenv("AUDIT_SECOND_LEG", raising=False)
    monkeypatch.setenv("AUDIT_OPENROUTER_ENABLED", "0")
    monkeypatch.setattr(gfo, "STAGE01_THIRD_LEG_ENABLED", True)
    monkeypatch.setattr(gfo, "STAGE01_THIRD_LEG_MODEL", SOL_MODEL)
    monkeypatch.setattr(gfo, "CODEX_STAGE_MODEL_ID", ASTRA_MODEL)
    monkeypatch.setattr(gfo, "STAGE01_DUAL_REVIEW_MODEL", SOL_MODEL)
    monkeypatch.setattr(gfo, "STAGE01_DUAL_REVIEW_ENABLED", True)
    monkeypatch.setattr(gfo, "STAGE01_DUAL_GAP_SEARCH_ENABLED", False)
    monkeypatch.setattr(gfo, "STAGE01_PROTECTION_TABLE_CHECK_ENABLED", False)
    monkeypatch.setattr(gfo, "STAGE01_ABORT_ON_LEG_FAILURE_ENABLED", True)
    monkeypatch.setattr(gfo, "STAGE01_LEG_FAILURE_THRESHOLD", 1)
    monkeypatch.setattr(gfo, "STAGE01_CLAUDE_LEG_ENABLED", True)
    monkeypatch.setattr(gfo, "STAGE01_CLAUDE_LEG_MODEL", CLAUDE)
    monkeypatch.setattr(gfo, "STAGE01_CLAUDE_LEG_EFFORT", "medium")
    monkeypatch.setattr(gfo, "STAGE01_CLAUDE_LEG_DISCIPLINES", frozenset({"EOM", "AR"}))
    monkeypatch.setattr(gfo, "provider_bridge_active", lambda: False)
    monkeypatch.setattr(gfo, "build_effective_block_context", lambda *_a, **_k: ("CTX", "raw_vector"))
    yield
    assert not attempts


def project(tmp_path, blocks=("B-1",)):
    from backend.app.pipeline.stages.block_context.contract import SCHEMA_VERSION
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import stage02_crop_policy

    output = tmp_path / "_output"
    crops = output / "blocks_stage02_100"
    crops.mkdir(parents=True)
    for block_id in blocks:
        (crops / f"{block_id}.png").write_bytes(b"synthetic-image")
    (crops / "index.json").write_text(json.dumps({**stage02_crop_policy(), "blocks": [
        {"block_id": b, "page": 1, "file": f"{b}.png", "size_kb": 1} for b in blocks]}))
    (output / "document_graph.json").write_text(json.dumps(
        {"pages": [{"page": 1, "sheet_no": "1", "text_blocks": []}]}))
    (output / "block_context_summary.json").write_text(json.dumps({
        "schema_version": SCHEMA_VERSION, "stage": "block_context",
        "reference_catalog": {"runtime_source": "pipeline_stage_embedded_catalog", "records_total": 1},
        "status": "ok", "blocks_total": len(blocks), "blocks_ready": len(blocks), "blocks_failed": 0,
        "source_counts": {"raw_vector": len(blocks)},
        "blocks": [{"block_id": b, "page": 1, "source_kind": "raw_vector",
                    "coverage_status": "ready", "warnings": []} for b in blocks]}))
    return output


def finding(label):
    return {"severity": "ЭКСПЛУАТАЦИОННОЕ", "category": "marking", "finding": f"{label}: дубль обозначения QF5",
            "norm_quote": None, "value_found": "QF5", "recommendation": "Исправить маркировку."}


def install_legs(monkeypatch, *, claude_result, section="EOM"):
    calls = {"codex": [], "claude": [], "judge": []}

    async def codex(*args, **kwargs):
        calls["codex"].append((args, kwargs))
        return {"ok": True, "parsed": {"findings": [finding(kwargs["model"])]},
                "input_tokens": 100, "output_tokens": 10, "elapsed_ms": 1}

    async def claude(*args, **kwargs):
        calls["claude"].append((args, kwargs))
        return claude_result()

    async def judge(messages, **kwargs):
        payload = json.loads(messages[1]["content"])
        calls["judge"].append((messages[0]["content"], payload))
        groups = payload["detector_results"]
        relationships = []
        if groups[0]["findings"] and groups[1]["findings"]:
            relationships = [{"left_ref": groups[0]["findings"][0]["ref"],
                              "right_ref": groups[1]["findings"][0]["ref"],
                              "relation": "match", "extends": "none", "confidence": 0.9, "reason": "same"}]
        data = {"relationships": relationships, "gap_findings": [],
                "gap_search": {"performed": False, "status": "disabled"}}
        return LLMResult(text=json.dumps(data), json_data=data, model=SOL_MODEL,
                         input_tokens=0, output_tokens=0, duration_ms=1)

    from backend.app.services.llm import codex_runner

    monkeypatch.setattr(gfo, "call_codex_for_block", codex)
    monkeypatch.setattr(gfo, "call_claude_for_block", claude)
    monkeypatch.setattr(codex_runner, "run_codex_json_messages", judge)
    monkeypatch.setattr(gfo, "load_version_project_info",
                        lambda *_a, **_k: {"project_id": "synthetic", "section": section})
    return calls


@pytest.mark.asyncio
async def test_claude_leg_gets_codex_inputs_and_judge_compares_it_with_codex_group(monkeypatch, tmp_path):
    calls = install_legs(monkeypatch, claude_result=lambda: {
        "ok": True, "parsed": {"findings": [finding("Opus")]},
        "input_tokens": 1000, "cached_input_tokens": 200, "output_tokens": 50, "elapsed_ms": 1})
    output = project(tmp_path)
    result = await gfo.run_findings_only_for_project(
        tmp_path, output_dir_override=output, model="ensemble/gpt-codex", timeout_s=5, write_run_log=False)

    astra = next(c for c in calls["codex"] if c[1]["model"] == ASTRA_MODEL)
    claude_args, claude_kwargs = calls["claude"][0]
    assert claude_args == astra[0]  # same block, enrichment, page text, crops dir
    for key in ("system_prompt", "routed_context", "document_context", "document_type", "page_neighbors"):
        assert claude_kwargs[key] == astra[1][key]
    assert claude_kwargs["model"] == CLAUDE and claude_kwargs["reasoning_effort"] == "medium"

    system_prompt, payload = calls["judge"][0]
    assert [g["model"] for g in payload["detector_results"]] == [CLAUDE, f"{ASTRA_MODEL}+{SOL_MODEL}"]
    assert "Opus и Astra+Sol" in system_prompt and '"left_ref": "claude:001"' in system_prompt

    block = result["output_doc"]["block_analyses"][0]
    summary = result["summary"]
    assert block["detectors_ok"] == [CLAUDE, ASTRA_MODEL, SOL_MODEL] and block["detectors_failed"] == []
    assert block["dual_review"]["counts"]["matches"] == 1
    receipt = summary["execution_receipt"]
    assert receipt["stage_01_mode"] == "TWO_MODEL_NO_OPENROUTER" and receipt["openrouter_calls"] == 0
    assert receipt["claude_calls"] == 1 and receipt["claude_failures"] == 0
    assert summary["claude_leg"]["combined_input_tokens"] == 1000
    assert any(leg["provider"] == "claude" and leg["optional"] for leg in summary["audit_legs"])


@pytest.mark.asyncio
async def test_claude_limit_is_optional_and_switches_the_leg_off(monkeypatch, tmp_path):
    calls = install_legs(monkeypatch, claude_result=lambda: {
        "ok": False, "error": "usage limit (claude): try later", "input_tokens": 0,
        "output_tokens": 0, "elapsed_ms": 1})
    output = project(tmp_path, blocks=("B-1", "B-2"))
    result = await gfo.run_findings_only_for_project(
        tmp_path, output_dir_override=output, model="ensemble/gpt-codex", timeout_s=5,
        parallelism=1, write_run_log=False)

    summary = result["summary"]
    assert summary["blocks_ok"] == 2 and summary["blocks_failed"] == 0
    assert not summary.get("aborted_on_leg_failure") and not summary.get("leg_failures")
    assert len(calls["claude"]) == 1  # the limit disabled the leg for the rest of the run
    assert summary["claude_leg"]["disabled_reason"] == "subscription_limit"
    for block in result["output_doc"]["block_analyses"]:
        assert block["detectors_ok"] == [ASTRA_MODEL, SOL_MODEL] and block["detectors_failed"] == []
    for _prompt, payload in calls["judge"]:
        assert [g["model"] for g in payload["detector_results"]] == [ASTRA_MODEL, SOL_MODEL]


@pytest.mark.asyncio
async def test_claude_leg_skips_disciplines_outside_the_list(monkeypatch, tmp_path):
    calls = install_legs(monkeypatch, section="KJ", claude_result=lambda: pytest.fail("must not run"))
    output = project(tmp_path)
    result = await gfo.run_findings_only_for_project(
        tmp_path, output_dir_override=output, model="ensemble/gpt-codex", timeout_s=5, write_run_log=False)
    assert calls["claude"] == []
    assert "claude_leg" not in result["summary"]
    assert result["summary"]["execution_receipt"]["claude_leg_model"] is None


@pytest.mark.asyncio
async def test_two_model_judge_prompt_is_unchanged(monkeypatch, tmp_path):
    seen = []

    async def judge(messages, **kwargs):
        seen.append(messages[0]["content"])
        data = {"relationships": [], "gap_findings": [], "gap_search": {}}
        return LLMResult(text=json.dumps(data), json_data=data, model=SOL_MODEL)

    from backend.app.services.llm import codex_runner

    monkeypatch.setattr(codex_runner, "run_codex_json_messages", judge)
    combined = gfo.combine_detector_results([
        (ASTRA_MODEL, {"ok": True, "parsed": {"findings": [finding("a")]}}),
        (SOL_MODEL, {"ok": True, "parsed": {"findings": [finding("b")]}})], run_id="r")
    await dual_review.review_dual_findings(
        combined["parsed"]["findings"], block_id="B", page=1, block_context="", image_path=tmp_path / "x.png",
        reviewer_model=SOL_MODEL, run_id="r", project_id="p", timeout=5, gap_search_enabled=False,
        comparison_models=[ASTRA_MODEL, SOL_MODEL])
    assert "Astra и Sol" in seen[0]
    assert '"left_ref": "codex:001"' in seen[0] and '"right_ref": "codex:002"' in seen[0]


def test_group_side_rejects_pairs_inside_one_side():
    combined = gfo.combine_detector_results([
        (CLAUDE, {"ok": True, "parsed": {"findings": [finding("o")]}}),
        (ASTRA_MODEL, {"ok": True, "parsed": {"findings": [finding("a")]}}),
        (SOL_MODEL, {"ok": True, "parsed": {"findings": [finding("s")]}})], run_id="r")
    refs = [f["_detector_ref"] for f in combined["parsed"]["findings"]]
    assert refs == ["claude:001", "codex:001", "codex:002"]
    payload = {"relationships": [
        {"left_ref": "codex:001", "right_ref": "codex:002", "relation": "match"},   # same side → ignored
        {"left_ref": "claude:001", "right_ref": "codex:002", "relation": "match"},  # Opus vs Sol → kept
    ]}
    normalized = dual_review.normalize_review_payload(
        payload, combined["parsed"]["findings"], reviewer_model=SOL_MODEL, gap_search_enabled=False,
        comparison_models=[CLAUDE, [ASTRA_MODEL, SOL_MODEL]])
    assert [(r["left_ref"], r["right_ref"]) for r in normalized["report"]["relationships"]] == [
        ("claude:001", "codex:002")]
    assert normalized["report"]["unmatched_refs"] == ["codex:001"]


def test_manager_records_claude_leg_on_its_own_subscription(monkeypatch):
    from backend.app.pipeline import manager as manager_module

    records = []
    monkeypatch.setattr(manager_module.usage_tracker, "record_usage", records.append)
    summary = {
        "model": "ensemble/gpt-codex",
        "totals": {"input_tokens": 5000, "output_tokens": 500, "estimated_cost_usd_total": 0.0},
        "claude_leg": {"model": CLAUDE, "calls": 2, "input_tokens": 2000, "output_tokens": 200,
                       "cached_input_tokens": 0, "cache_write_input_tokens": 0,
                       "combined_input_tokens": 1500, "combined_output_tokens": 150},
    }
    stub_manager = SimpleNamespace(_enrich_pipeline_log=lambda *args, **kwargs: None)
    job = SimpleNamespace(project_id="p", job_id="j", version_id="", cost_usd=0.0, cli_calls=0)
    manager_module.PipelineManager._record_findings_only_usage(stub_manager, job, summary)
    claude, ensemble = records
    assert claude.model == CLAUDE and claude.input_tokens == 2000 and claude.cost_usd == 0.0
    assert claude.cost_usd_notional == pytest.approx((2000 * 4.0 + 200 * 20.0) / 1_000_000)
    assert ensemble.model == "ensemble/gpt-codex"
    assert (ensemble.input_tokens, ensemble.output_tokens) == (3500, 350)


def test_audit_log_names_opus_and_its_optional_failure():
    from backend.app.pipeline.stages.block_analysis.runner import _stage01_models_cell, _stage01_model_name

    assert _stage01_model_name("claude-opus-5-5") == "Opus"
    cell = _stage01_models_cell(["claude-opus-5-5", "codex/gpt-5.6-sol"], [])
    assert cell.startswith("Opus, Codex-sol")
    assert _stage01_models_cell(["codex/gpt-5.6-sol"], [], ["claude-opus-5-5"]).endswith("✗Opus (необяз.)")
