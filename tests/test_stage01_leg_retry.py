"""Повтор упавшей ноги ансамбля этапа 01 (STAGE01_LEG_RETRY_*), 20.10.2026.

Упавшая нога не останавливает аудит сразу: пауза и перезапрос до N раз.
Остановка (STAGE01_ABORT_ON_LEG_FAILURE_ENABLED) — только если нога не
поднялась и после всех повторов.
"""
import asyncio
import json
import socket

import pytest

from backend.app.models.usage import LLMResult
from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo
from backend.app.pipeline.stages.block_analysis.ensemble_mode import ASTRA_MODEL, SOL_MODEL

CAPACITY_ERROR = f"codex/{SOL_MODEL}: codex_exec_exit_1; codex_json_not_found; attempts_3"


# ── call_leg_with_retry ────────────────────────────────────────────────────

def _ok(**extra):
    return {"ok": True, "parsed": {"findings": []}, **extra}


def _fail(error=CAPACITY_ERROR):
    return {"ok": False, "error": error, "parse_error": "codex_failed"}


def _scripted(outcomes):
    calls = []

    async def make_call():
        calls.append(True)
        out = outcomes[min(len(calls), len(outcomes)) - 1]
        if isinstance(out, BaseException):
            raise out
        return dict(out)

    return make_call, calls


@pytest.mark.asyncio
async def test_success_first_try_is_untouched():
    make_call, calls = _scripted([_ok()])
    res = await gfo.call_leg_with_retry(make_call, attempts=5, delay_s=0)
    assert res["ok"] and len(calls) == 1
    assert "leg_retry_attempts" not in res


@pytest.mark.asyncio
async def test_failed_leg_recovers_on_retry():
    retries = []
    make_call, calls = _scripted([_fail(), _fail(), _ok()])
    res = await gfo.call_leg_with_retry(
        make_call, attempts=5, delay_s=0,
        on_retry=lambda a, m, e: retries.append((a, m)),
    )
    assert res["ok"] and len(calls) == 3
    assert res["leg_retry_attempts"] == 2
    assert retries == [(1, 5), (2, 5)]


@pytest.mark.asyncio
async def test_gives_up_after_all_retries_and_returns_last_failure():
    make_call, calls = _scripted([_fail()])
    res = await gfo.call_leg_with_retry(make_call, attempts=5, delay_s=0)
    assert len(calls) == 6  # 1 попытка + 5 повторов
    assert res["ok"] is False and res["error"] == CAPACITY_ERROR
    assert res["leg_retry_attempts"] == 5


@pytest.mark.asyncio
async def test_exception_is_retried_and_last_one_reraised():
    make_call, calls = _scripted([OSError("boom")])
    with pytest.raises(OSError):
        await gfo.call_leg_with_retry(make_call, attempts=2, delay_s=0)
    assert len(calls) == 3

    make_call, calls = _scripted([OSError("boom"), _ok()])
    res = await gfo.call_leg_with_retry(make_call, attempts=2, delay_s=0)
    assert res["ok"] and len(calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [
    "codex: usage limit reached", "insufficient_quota", "Invalid API key",
    "OPENROUTER_DISABLED",
])
async def test_non_retryable_errors_fail_at_once(error):
    make_call, calls = _scripted([_fail(error)])
    res = await gfo.call_leg_with_retry(make_call, attempts=5, delay_s=0)
    assert len(calls) == 1 and res["ok"] is False


@pytest.mark.asyncio
async def test_hung_attempt_is_cut_and_not_retried():
    calls = []

    async def make_call():
        calls.append(True)
        await asyncio.sleep(10)

    res = await gfo.call_leg_with_retry(
        make_call, attempts=5, delay_s=0, attempt_timeout_s=0.05,
    )
    assert len(calls) == 1
    assert res["ok"] is False and res["parse_error"] == "leg_hard_timeout"


@pytest.mark.asyncio
async def test_stop_event_interrupts_pause():
    stop = asyncio.Event()
    make_call, calls = _scripted([_fail()])

    async def _stop_soon():
        await asyncio.sleep(0.05)
        stop.set()

    asyncio.ensure_future(_stop_soon())
    res = await asyncio.wait_for(
        gfo.call_leg_with_retry(make_call, attempts=5, delay_s=30, stop_events=[None, stop]),
        timeout=5,
    )
    assert len(calls) == 1 and res["ok"] is False


@pytest.mark.asyncio
async def test_zero_attempts_keeps_old_behaviour():
    make_call, calls = _scripted([_fail(), _ok()])
    res = await gfo.call_leg_with_retry(make_call, attempts=0, delay_s=0)
    assert len(calls) == 1 and res["ok"] is False


def test_config_defaults_are_five_retries_and_thirty_seconds():
    from backend.app.core import config
    text = open(config.__file__, encoding="utf-8").read()
    assert 'os.environ.get("STAGE01_LEG_RETRY_ATTEMPTS", "5")' in text
    assert 'os.environ.get("STAGE01_LEG_RETRY_DELAY_S", "30")' in text


# ── Сквозной прогон этапа 01 (Astra + Sol, без OpenRouter) ────────────────

@pytest.fixture
def stage(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        raise AssertionError("real network forbidden")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.delenv("AUDIT_SECOND_LEG", raising=False)
    monkeypatch.setenv("AUDIT_OPENROUTER_ENABLED", "0")
    monkeypatch.setattr(gfo, "STAGE01_THIRD_LEG_ENABLED", True)
    monkeypatch.setattr(gfo, "STAGE01_THIRD_LEG_MODEL", SOL_MODEL)
    monkeypatch.setattr(gfo, "CODEX_STAGE_MODEL_ID", ASTRA_MODEL)
    monkeypatch.setattr(gfo, "STAGE01_DUAL_REVIEW_MODEL", SOL_MODEL)
    monkeypatch.setattr(gfo, "STAGE01_DUAL_REVIEW_ENABLED", False)
    monkeypatch.setattr(gfo, "STAGE01_DUAL_GAP_SEARCH_ENABLED", False)
    monkeypatch.setattr(gfo, "STAGE01_PROTECTION_TABLE_CHECK_ENABLED", False)
    monkeypatch.setattr(gfo, "STAGE01_ABORT_ON_LEG_FAILURE_ENABLED", True)
    monkeypatch.setattr(gfo, "STAGE01_LEG_FAILURE_THRESHOLD", 1)
    monkeypatch.setattr(gfo, "STAGE01_LEG_RETRY_ATTEMPTS", 5)
    monkeypatch.setattr(gfo, "STAGE01_LEG_RETRY_DELAY_S", 0.0)
    monkeypatch.setattr(gfo, "provider_bridge_active", lambda: False)
    monkeypatch.setattr(gfo, "build_effective_block_context", lambda *_a, **_k: ("CTX", "raw_vector"))
    monkeypatch.setattr(
        gfo, "load_version_project_info",
        lambda *_a, **_k: {"project_id": "synthetic", "section": "EOM"},
    )

    async def unexpected(*args, **kwargs):
        raise AssertionError("no other transport permitted")

    monkeypatch.setattr(gfo, "call_gpt_for_block", unexpected)
    monkeypatch.setattr(gfo, "call_claude_cli_for_block", unexpected)

    from backend.app.pipeline.stages.block_context.contract import SCHEMA_VERSION
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import stage02_crop_policy

    output = tmp_path / "_output"
    blocks = output / "blocks_stage02_100"
    blocks.mkdir(parents=True)
    (blocks / "B-1.png").write_bytes(b"synthetic-image")
    (blocks / "index.json").write_text(json.dumps({**stage02_crop_policy(), "blocks": [
        {"block_id": "B-1", "page": 1, "file": "B-1.png", "size_kb": 1}]}))
    (output / "document_graph.json").write_text(
        json.dumps({"pages": [{"page": 1, "sheet_no": "1", "text_blocks": []}]}))
    (output / "block_context_summary.json").write_text(json.dumps({
        "schema_version": SCHEMA_VERSION, "stage": "block_context",
        "reference_catalog": {"runtime_source": "pipeline_stage_embedded_catalog", "records_total": 1},
        "status": "ok", "blocks_total": 1, "blocks_ready": 1, "blocks_failed": 0,
        "source_counts": {"raw_vector": 1},
        "blocks": [{"block_id": "B-1", "page": 1, "source_kind": "raw_vector",
                    "coverage_status": "ready", "warnings": []}]}))

    calls = {ASTRA_MODEL: 0, SOL_MODEL: 0}
    script = {"sol_failures": 0, "sol_error": CAPACITY_ERROR}

    async def codex(*args, **kwargs):
        model = kwargs["model"]
        calls[model] += 1
        if model == SOL_MODEL and calls[model] <= script["sol_failures"]:
            return {"ok": False, "error": script["sol_error"], "parse_error": "codex_failed",
                    "input_tokens": 0, "output_tokens": 0, "elapsed_ms": 1}
        return {"ok": True, "parsed": {"findings": []},
                "input_tokens": 0, "output_tokens": 0, "elapsed_ms": 1}

    monkeypatch.setattr(gfo, "call_codex_for_block", codex)

    async def run():
        events = []
        result = await gfo.run_findings_only_for_project(
            tmp_path, output_dir_override=output, model="ensemble/gpt-codex",
            api_key=None, timeout_s=5, write_run_log=False, on_progress=events.append,
        )
        return result, events

    return run, calls, script


@pytest.mark.asyncio
async def test_stage_survives_leg_that_recovers(stage):
    run, calls, script = stage
    script["sol_failures"] = 3
    result, events = await run()
    summary = result["summary"]
    assert not summary.get("aborted_on_leg_failure")
    assert summary["blocks_ok"] == 1 and summary["blocks_partial"] == 0
    assert result["output_doc"]["block_analyses"][0]["detectors_failed"] == []
    # Перезапрашивается только упавшая нога — ответившая Astra не повторяется.
    assert calls == {ASTRA_MODEL: 1, SOL_MODEL: 4}
    retry_events = [e for e in events if e.get("type") == "leg_retry"]
    assert [e["retry"] for e in retry_events] == [1, 2, 3]
    assert {e["leg"] for e in retry_events} == {SOL_MODEL}


@pytest.mark.asyncio
async def test_stage_aborts_when_leg_never_recovers(stage):
    run, calls, script = stage
    script["sol_failures"] = 100
    result, _ = await run()
    summary = result["summary"]
    assert summary["aborted_on_leg_failure"] is True
    assert calls[SOL_MODEL] == 6
    assert summary["leg_failures"][0]["failed_legs"] == [SOL_MODEL]


@pytest.mark.asyncio
async def test_stage_does_not_wait_on_exhausted_quota(stage):
    run, calls, script = stage
    script["sol_failures"] = 100
    script["sol_error"] = "codex: usage limit reached"
    result, events = await run()
    assert result["summary"]["aborted_on_leg_failure"] is True
    assert calls[SOL_MODEL] == 1
    assert not [e for e in events if e.get("type") == "leg_retry"]
