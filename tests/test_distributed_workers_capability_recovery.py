from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from backend.app.services.distributed_workers import audit_job_service, job_service


RECOVERY_MARKER = "_capabilities_recovered_from_grpc_snapshot"


def _worker(capabilities: dict) -> dict:
    return {"capabilities": json.dumps(capabilities)}


@pytest.mark.parametrize(
    "provider_evidence",
    [
        {"provider_capabilities": {"claude": ["strong_audit"]}},
        {"provider_capabilities": {}, "providers": ["codex"]},
    ],
)
def test_grpc_stripped_real_capabilities_are_recovered(provider_evidence):
    caps = job_service.worker_capabilities(
        _worker({"job_types": ["audit_pipeline_v1"], **provider_evidence})
    )

    assert caps["real_llm_enabled"] is True
    assert caps["pipeline_provider_bridge_enabled"] is True
    assert caps["provider_mode"] == "real"
    assert caps[RECOVERY_MARKER] is True


@pytest.mark.parametrize(
    ("fake_evidence", "expected_bridge"),
    [
        ({"providers": ["fake_codex"]}, False),
        (
            {
                "provider_mode": "fake",
                "provider_capabilities": {"claude": ["strong_audit"]},
                "pipeline_provider_bridge_enabled": True,
            },
            True,
        ),
    ],
)
def test_explicit_fake_capabilities_remain_fail_closed(
    fake_evidence, expected_bridge
):
    caps = job_service.worker_capabilities(_worker(fake_evidence))

    assert caps["real_llm_enabled"] is False
    assert caps["pipeline_provider_bridge_enabled"] is expected_bridge
    assert caps["provider_mode"] == "fake"
    assert caps[RECOVERY_MARKER] is True


def test_explicit_real_llm_false_is_not_overridden_by_provider_evidence():
    declared = {
        "real_llm_enabled": False,
        "provider_mode": "fake",
        "pipeline_provider_bridge_enabled": False,
        "provider_capabilities": {"claude": ["strong_audit"]},
    }

    assert job_service.worker_capabilities(_worker(declared)) == declared


def test_explicit_real_llm_true_is_preserved():
    declared = {
        "real_llm_enabled": True,
        "provider_mode": "real",
        "pipeline_provider_bridge_enabled": True,
        "provider_capabilities": {"codex": ["cheap_review"]},
    }

    assert job_service.worker_capabilities(_worker(declared)) == declared


def test_unknown_capabilities_remain_fail_closed_without_recovery_marker():
    declared = {"provider_capabilities": {}, "providers": ["other"]}

    assert job_service.worker_capabilities(_worker(declared)) == declared


def test_audit_compatibility_report_uses_recovered_values():
    worker = {
        **_worker(
            {
                "job_types": ["audit_pipeline_v1"],
                "provider_capabilities": {"openrouter": ["block_detector"]},
            }
        ),
        "worker_id": "wrk_grpc_snapshot",
        "protocol_version": 1,
        "registration_status": "approved",
        "connection_status": "online",
        "resource_snapshot": {},
    }

    report = audit_job_service.compatibility_report(
        worker,
        settings=SimpleNamespace(protocol_version=1),
        active_attempts=[],
    )

    assert report["real_llm_enabled"] is True
    assert report["provider_mode"] == "real"
