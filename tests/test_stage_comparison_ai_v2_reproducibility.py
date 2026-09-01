"""Cold-run provenance and stability gate for AI Analyst v2."""
from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.stage_comparison.ai import cache as cache_module
from backend.app.services.stage_comparison.ai_v2.engine import (
    PROMPT_AUDIT_VERSION,
    SCHEMA_VERSION,
)
from backend.app.services.stage_comparison.production_artifacts import (
    content_signature,
)
from scripts.stage_comparison_ai_v2_reproducibility import evaluate


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8",
    )


def _run_dir(
    root: Path,
    *,
    accepted: set[str] = frozenset({"t1"}),
    supported: set[str] = frozenset({"t1"}),
    cache_enabled: bool = False,
    unsupported: int = 0,
) -> Path:
    prompt_manifest = []
    task_ids = ["t1", "t2", "t3"]
    for sequence, task_id in enumerate(task_ids, 1):
        role = "table_identity" if sequence < 3 else "analyst"
        prompt = f"frozen prompt {sequence}"
        system_prompt = "frozen system"
        schema = {"type": "object", "properties": {}}
        payload = {
            "kind": "stage_comparison_ai_v2_prompt_payload",
            "schema_version": PROMPT_AUDIT_VERSION,
            "sequence": sequence,
            "role": role,
            "system_prompt": system_prompt,
            "prompt": prompt,
            "response_schema": schema,
        }
        prompt_digest = cache_module.digest_prompt(prompt, system_prompt)
        schema_digest = cache_module.digest_schema(schema)
        evidence_digest = f"evidence-{sequence}"
        cache_key = cache_module.cache_key(
            evidence_digest=evidence_digest,
            model="gpt-5.6-sol",
            reasoning_level="low",
            prompt_version="prompt.v1",
            schema_version="contract.v1",
            role=role,
            prompt_digest=prompt_digest,
            schema_digest=schema_digest,
        )
        payload_file = f"{sequence:02d}_{role}.json"
        _write(root / "low" / "prompt_payloads" / payload_file, payload)
        prompt_manifest.append({
            "schema_version": PROMPT_AUDIT_VERSION,
            "sequence": sequence,
            "role": role,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "low",
            "prompt_version": "prompt.v1",
            "contract_schema_version": "contract.v1",
            "prompt_bytes": len(prompt.encode()) + len(system_prompt.encode()),
            "prompt_digest": prompt_digest,
            "schema_digest": schema_digest,
            "payload_signature": content_signature(payload),
            "evidence_digest": evidence_digest,
            "context_signature": "context",
            "model_context_signature": "model-context",
            "cache_key": cache_key,
            "task_ids": [task_id],
            "payload_file": payload_file,
        })
    prompt_signature = content_signature({
        "schema": PROMPT_AUDIT_VERSION,
        "sessions": prompt_manifest,
    })
    run = {
        "schema_version": SCHEMA_VERSION,
        "model": "gpt-5.6-sol",
        "reasoning_effort": "low",
        "fast_input_signature": "fast",
        "context_signature": "context",
        "model_context_signature": "model-context",
        "inventory_signature": "inventory",
        "prompt_signature": prompt_signature,
        "prompt_manifest": prompt_manifest,
        "settings": {"prompt_version": "prompt.v1", "max_sessions": 3},
        "resolutions": [
            {
                "task_id": task_id,
                "status": (
                    "AI_RESOLVED_VERIFIED" if task_id in accepted
                    else "HUMAN_REQUIRED"
                ),
            }
            for task_id in task_ids
        ],
        "diagnostics": {
            "routed": 3,
            "ai_resolved_verified": len(accepted),
            "verifier_rejected": 3 - len(accepted),
            "unsupported_published": unsupported,
            "duration_ms": 100,
            "model_calls": 3,
            "sessions": 3,
            "cache": {
                "enabled": cache_enabled,
                "hits": 0,
                "misses": 0,
                "writes": 0,
            },
        },
    }
    run["input_signature"] = content_signature({
        "schema": run["schema_version"],
        "context": run["context_signature"],
        "model_context": run["model_context_signature"],
        "inventory": run["inventory_signature"],
        "fast_input": run["fast_input_signature"],
        "prompt": run["prompt_signature"],
        "effort": run["reasoning_effort"],
        "model": run["model"],
    })
    outcomes = [
        {
            "task_id": task_id,
            "outcome": (
                "MATERIALIZED_FINDING" if task_id in supported
                else "HUMAN_REQUIRED"
            ),
            "materialized_finding_ids": (
                [f"finding-{task_id}"] if task_id in supported else []
            ),
            "removed_review_target_ids": (
                [task_id] if task_id in supported else []
            ),
        }
        for task_id in task_ids
    ]
    materialization = {
        "outcomes": outcomes,
        "diagnostics": {
            "materialized_findings": len(supported),
            "stage7_before": 77,
            "stage7_after": 77 - len(supported),
            "unsupported_materialized": unsupported,
        },
        "human_review_plan": {
            "summary": {"mandatory_human_interactions": 6},
        },
        "preliminary_report": {
            "summary": {"counts": {"ai_verified": len(supported)}},
        },
    }
    _write(root / "fast_input_manifest.json", {"input_signature": "fast"})
    _write(root / "low" / "run.json", run)
    _write(root / "low" / "materialization.json", materialization)
    return root


def test_three_identical_cache_disabled_runs_pass_gate(tmp_path):
    dirs = [_run_dir(tmp_path / f"run-{index}") for index in range(3)]
    result = evaluate(dirs)
    assert result["verdict"] == "A"
    assert result["recommend_rollout"] is True
    assert result["problems"] == []
    assert {
        value["ratio"] for value in result["resolution_overlap"].values()
    } == {1.0}
    assert {
        value["ratio"]
        for value in result["supported_materialized_overlap"].values()
    } == {1.0}


def test_safe_materialized_drift_is_verdict_b(tmp_path):
    dirs = [
        _run_dir(tmp_path / "run-1"),
        _run_dir(tmp_path / "run-2", accepted={"t2"}, supported={"t2"}),
        _run_dir(tmp_path / "run-3"),
    ]
    result = evaluate(dirs)
    assert result["verdict"] == "B"
    assert result["recommend_rollout"] is False
    assert result["problems"] == []


def test_cache_enabled_run_is_system_defect_c(tmp_path):
    dirs = [
        _run_dir(tmp_path / "run-1"),
        _run_dir(tmp_path / "run-2", cache_enabled=True),
        _run_dir(tmp_path / "run-3"),
    ]
    result = evaluate(dirs)
    assert result["verdict"] == "C"
    assert any("cache was not disabled" in value for value in result["problems"])
