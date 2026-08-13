"""11J.1: frozen tail, result provenance/hash и provider concurrency.

Ни один тест не запускает Claude/Codex/OpenRouter runtime. Ledger заполняется
синтетическим ProviderInferenceResult, concurrency проверяется на локальных
критических секциях execution layer.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from types import SimpleNamespace

import pytest

from audit_worker import package_io
from audit_worker.providers.inference import ProviderInferenceResult, STATUS_SUCCESS
from audit_worker.providers.inference_ledger import InferenceLedger
from audit_worker.providers import pipeline_bridge
from backend.app.models.distributed_workers import JobType
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.execution import registry as execution_registry
from backend.app.pipeline.stages.norms.runner import run_norm_verification
from backend.app.services.audit_routing import presets, registry
from backend.app.services.distributed_workers import repositories, result_import
from tests.test_audit_routing_plan import build_plan


def _attempt(plan: dict) -> dict:
    return {
        "attempt_id": "att-11j1",
        "job_id": "job-11j1",
        "job_type": JobType.AUDIT_PIPELINE_V1.value,
        "payload": json.dumps(
            {
                "params": {
                    "routing_plan_contract_version": 1,
                    "routing_plan": plan,
                }
            }
        ),
    }


def test_frozen_lookup_found_not_found_and_invalid(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    plan = build_plan(presets.PRESET_FULL_CODEX)
    handle = SimpleNamespace(remote_job_id="job-11j1")

    monkeypatch.setattr(
        repositories,
        "get_logical_job",
        lambda *_args, **_kwargs: {
            "payload": json.dumps(
                {
                    "params": {
                        "routing_plan_contract_version": 1,
                        "routing_plan": plan.to_dict(),
                    }
                }
            )
        },
    )
    found = execution_registry.frozen_routing_plan(handle)
    assert found.plan_hash() == plan.plan_hash()
    assert "FROZEN_ROUTING_PLAN FOUND" in caplog.text

    caplog.clear()
    monkeypatch.setattr(
        repositories,
        "get_logical_job",
        lambda *_args, **_kwargs: {"payload": json.dumps({"params": {}})},
    )
    assert execution_registry.frozen_routing_plan(handle) is None
    assert "NOT_FOUND" in caplog.text and "legacy_contract_v0" in caplog.text

    caplog.clear()
    monkeypatch.setattr(
        repositories,
        "get_logical_job",
        lambda *_args, **_kwargs: {
            "payload": json.dumps(
                {"params": {"routing_plan_contract_version": 1}}
            )
        },
    )
    with pytest.raises(execution_registry.FrozenRoutingPlanError, match="NOT_FOUND"):
        execution_registry.frozen_routing_plan(handle)
    assert "NOT_FOUND" in caplog.text and "fail closed" in caplog.text

    caplog.clear()
    damaged = plan.to_dict()
    damaged["routing_plan_hash"] = "sha256:" + "0" * 64
    monkeypatch.setattr(
        repositories,
        "get_logical_job",
        lambda *_args, **_kwargs: {
            "payload": json.dumps(
                {
                    "params": {
                        "routing_plan_contract_version": 1,
                        "routing_plan": damaged,
                    }
                }
            )
        },
    )
    with pytest.raises(execution_registry.FrozenRoutingPlanError, match="INVALID"):
        execution_registry.frozen_routing_plan(handle)
    assert "INVALID" in caplog.text


def test_result_package_has_per_action_provenance_and_roundtrip_hash(tmp_path):
    plan = build_plan(presets.PRESET_FULL_CODEX)
    raw_plan = plan.to_dict()
    job_dir = tmp_path / "job"
    (job_dir / "result").mkdir(parents=True)
    (job_dir / "result" / "audit_manifest.json").write_text(
        "{}", encoding="utf-8"
    )

    action = next(
        item
        for stage, item in plan.model_actions()
        if item.action_id == "detector_openrouter"
    )
    ledger = InferenceLedger(job_dir, attempt_id="att-11j1", job_id="job-11j1")
    key = "block_analysis-B1-" + "a" * 32
    claimed = ledger.begin(
        key,
        provider=str(action.provider),
        capability=str(action.capability),
        action_id=action.action_id,
        stage="block_analysis",
        purpose="block_analysis:B1",
        prompt_sha256="b" * 64,
    )
    assert claimed.may_call_model
    ledger.complete(
        key,
        ProviderInferenceResult(
            provider=str(action.provider), model="fake/openrouter-model",
            status=STATUS_SUCCESS, result={"findings": []}, exit_code=0,
        ),
    )

    archive = tmp_path / "result.tar.gz"
    package_io.build_result_package(
        dest_path=archive,
        job_dir=job_dir,
        job_id="job-11j1",
        attempt_id="att-11j1",
        project_id="project",
        version_id="v1",
        worker_id="worker31",
        worker_version="test",
        protocol_version=1,
        manifest_version=1,
        job_type=JobType.AUDIT_PIPELINE_V1.value,
        required_artifacts=[],
        pipeline_revision="rev",
        stage_completion={"block_analysis": "done"},
        routing_plan=raw_plan,
    )
    manifest = package_io.read_manifest(archive)
    assert manifest["routing_plan_id"] == plan.routing_plan_id
    assert manifest["routing_plan_hash"] == plan.plan_hash()
    assert manifest["provider_action_provenance"] == [
        {
            "ledger_key": key,
            "stage_id": "block_batch",
            "action_id": "detector_openrouter",
            "logical_invocation_identity": "block_analysis:B1",
            "block_identity": "B1",
            "provider": registry.PROVIDER_OPENROUTER,
            "capability": registry.CAP_BLOCK_DETECTOR,
            "resolved_model_metadata": {
                "reported_model": "fake/openrouter-model",
                "model_report": "",
            },
            "status": "success",
        }
    ]
    result_import._validate_routing_provenance(
        manifest=manifest, attempt=_attempt(raw_plan)
    )


def test_result_routing_hash_mismatch_is_rejected():
    plan = build_plan(presets.PRESET_CLAUDE_GPT_CODEX)
    manifest = {
        "routing_plan_id": plan.routing_plan_id,
        "routing_plan_hash": "sha256:" + "0" * 64,
        "provider_action_provenance": [],
    }
    with pytest.raises(result_import.ResultImportError, match="hash.*не совпадает"):
        result_import._validate_routing_provenance(
            manifest=manifest, attempt=_attempt(plan.to_dict())
        )


def test_provider_concurrency_limits_are_independent(monkeypatch):
    monkeypatch.setenv("AUDIT_WORKER_PROVIDER_CLAUDE_MAX_CONCURRENCY", "1")
    monkeypatch.setenv("AUDIT_WORKER_PROVIDER_CODEX_MAX_CONCURRENCY", "2")
    running = {"claude": 0, "codex": 0}
    peaks = {"claude": 0, "codex": 0}
    guard = threading.Lock()
    start = threading.Barrier(7)

    def enter(provider: str) -> None:
        start.wait()
        with pipeline_bridge.provider_concurrency_slot(provider):
            with guard:
                running[provider] += 1
                peaks[provider] = max(peaks[provider], running[provider])
            time.sleep(0.05)
            with guard:
                running[provider] -= 1

    threads = [
        threading.Thread(target=enter, args=(provider,))
        for provider in ("claude",) * 3 + ("codex",) * 3
    ]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=3)
        assert not thread.is_alive()
    assert peaks == {"claude": 1, "codex": 2}


def test_zero_norms_write_authoritative_empty_handoff_marker(tmp_path):
    """Успешный zero-norm tail создаёт обязательный norm_checks.json."""
    output = tmp_path / "latest"
    output.mkdir()
    (output / "03_findings.json").write_text(
        json.dumps(
            {
                "findings": [{
                    "id": "F-TEST",
                    "problem": "Синтетическое замечание без ссылки на норму",
                    "norm": None,
                }]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    log_rows: list[tuple[str, str]] = []
    stages: list[tuple[str, str, dict]] = []

    async def log(message, level="info"):
        log_rows.append((str(level), str(message)))

    async def yes():
        return True

    async def rate_limit(_reason, _output):
        return True

    async def subprocess_stub(*_args, **_kwargs):
        raise AssertionError("zero-norm branch не должна запускать subprocess/model")

    ctx = PipelineStageContext(
        project_dir=tmp_path,
        project_id="zero-norm-project",
        output_dir=output,
        log=log,
        check_before_launch=yes,
        check_pause=yes,
        wait_for_rate_limit=rate_limit,
        record_cli_usage=lambda *_args, **_kwargs: None,
        update_pipeline_log=lambda stage, status, **kwargs: stages.append(
            (stage, status, kwargs)
        ),
        run_subprocess=subprocess_stub,
        project_info={},
    )

    result = asyncio.run(run_norm_verification(ctx))

    assert result.success is True
    marker = json.loads((output / "norm_checks.json").read_text(encoding="utf-8"))
    assert marker["checks"] == []
    assert marker["paragraph_checks"] == []
    assert marker["meta"]["total_checked"] == 0
    assert marker["meta"]["source"] == "norms_main_status_index"
    assert stages[-1][0:2] == ("norm_verify", "done")
    assert "authoritative empty" in stages[-1][2]["message"]
