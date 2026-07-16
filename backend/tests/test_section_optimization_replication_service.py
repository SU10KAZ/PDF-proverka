from __future__ import annotations

import asyncio

import pytest

from backend.app.services import section_optimization_pipeline_service as pipeline
from backend.app.services import section_optimization_replication_service as replication


@pytest.fixture(autouse=True)
def isolated_storage(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline, "_root", lambda: tmp_path)
    monkeypatch.setattr(pipeline, "_resolve_object_id", lambda object_id: object_id or "object-1")
    monkeypatch.setattr(replication, "_resolve_object_id", lambda object_id: object_id or "object-1")
    pipeline._ACTIVE_TASKS.clear()
    replication._ACTIVE_TASKS.clear()

    async def fake_agent(dossier, **_kwargs):
        graphics = bool((dossier.get("candidate") or {}).get("graphics_recommended"))
        assessments = [
            {
                "project_id": target["project_id"],
                "project_name": target["project_name"],
                "version_id": target["version_id"],
                "verdict": "needs_graphics" if graphics else "applicable_with_conditions",
                "confidence": 0.82,
                "reason": "Проверено умным агентом",
                "target_row_ids": [row["row_id"] for row in target["rows"]],
                "conditions": ["Сохранить технические параметры"],
                "missing_data": [],
                "graphics_required": graphics,
                "graphics_reason": "Проверить схему" if graphics else "",
                "suggested_pages": [10] if graphics else [],
                "expert_action": "Подтвердить применимость",
            }
            for target in dossier.get("targets") or []
        ]
        return {
            "agent_version": 1,
            "overall_recommendation": "needs_graphics" if graphics else "replicate_with_conditions",
            "summary": "Агент подготовил инженерную оценку",
            "target_assessments": assessments,
            "cross_project_risks": [],
            "expert_summary": "Передать эксперту",
        }, {
            "status": "complete",
            "agent_version": 1,
            "model": "codex/gpt-test",
            "input_tokens": 100,
            "output_tokens": 50,
            "duration_ms": 10,
        }

    monkeypatch.setattr(replication, "analyze_replication_dossier", fake_agent)


def _snapshot(graphics_recommended: bool = False) -> dict:
    return {
        "meta": {"section": "EOM", "generated_at": "2026-07-15T00:00:00+00:00"},
        "accepted_optimizations": [
            {
                "source_ref": "P1:OPT-001",
                "project_id": "P1",
                "project_name": "Корпус 1",
                "version_id": "v001",
                "id": "OPT-001",
                "current": "Текущее решение",
                "proposed": "Принятое предложение",
                "spec_items": ["Розетка IP44"],
            }
        ],
        "specification_rows": [
            {
                "row_id": "SPEC-1",
                "project_id": "P2",
                "project_name": "Корпус 2",
                "version_id": "v002",
                "page": 10,
                "sheet": "ЭОМ.СО",
                "position": "1",
                "name": "Розетка IP44",
                "unit": "шт.",
                "quantity": "12",
            }
        ],
        "signals": [
            {
                "signal_id": "REPL-1",
                "kind": "replicate_accepted_optimization",
                "title": "Тиражировать розетку",
                "reason": "Решение применимо к корпусу 2",
                "match_basis": "совпадение",
                "match_score": 1,
                "representative_proposal": "Принятое предложение",
                "source_project_ids": ["P1"],
                "target_project_ids": ["P2"],
                "evidence_refs": ["P1:OPT-001"],
                "target_row_ids": ["SPEC-1"],
                "graphics_recommended": graphics_recommended,
            }
        ],
    }


@pytest.mark.asyncio
async def test_replication_builds_and_persists_expert_dossier(tmp_path):
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")

    started = replication.start_replication("EOM", "REPL-1", object_id="object-1")
    assert started["status"] == "queued"

    for _ in range(20):
        await asyncio.sleep(0)
        job = replication.get_replication(
            "EOM", started["replication_id"], object_id="object-1", include_dossier=True,
        )
        if job["status"] not in {"queued", "running"}:
            break

    assert job["status"] == "awaiting_expert"
    assert job["agent_status"] == "complete"
    assert job["agent_model"] == "codex/gpt-test"
    assert job["agent_assessments"][0]["verdict"] == "applicable_with_conditions"
    assert job["dossier"]["source_decisions"][0]["source_ref"] == "P1:OPT-001"
    assert job["dossier"]["agent_review"]["overall_recommendation"] == "replicate_with_conditions"
    assert job["dossier"]["targets"][0]["project_id"] == "P2"
    assert job["dossier"]["targets"][0]["rows"][0]["row_id"] == "SPEC-1"
    assert (tmp_path / "object-1" / "EOM" / "replications" / f"{job['replication_id']}.json").is_file()


@pytest.mark.asyncio
async def test_replication_waits_for_graphics_and_prevents_duplicate_process():
    pipeline.store_latest_snapshot("EOM", _snapshot(graphics_recommended=True), object_id="object-1", run_id="test-run")
    started = replication.start_replication("EOM", "REPL-1", object_id="object-1")

    for _ in range(20):
        await asyncio.sleep(0)
        job = replication.get_replication("EOM", started["replication_id"], object_id="object-1")
        if job["status"] not in {"queued", "running"}:
            break

    assert job["status"] == "awaiting_graphics"
    assert job["graphics_requests"][0]["project_id"] == "P2"
    with pytest.raises(replication.SectionReplicationConflict):
        replication.start_replication("EOM", "REPL-1", object_id="object-1")


@pytest.mark.asyncio
async def test_start_all_replications_launches_every_pending_candidate_once():
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")

    result = replication.start_all_replications("EOM", object_id="object-1")

    assert result["total_candidates"] == 1
    assert result["started_count"] == 1
    assert result["skipped_count"] == 0
    job_id = result["replications"][0]["replication_id"]
    for _ in range(20):
        await asyncio.sleep(0)
        job = replication.get_replication("EOM", job_id, object_id="object-1")
        if job["status"] not in {"queued", "running"}:
            break
    assert job["status"] == "awaiting_expert"

    repeated = replication.start_all_replications("EOM", object_id="object-1")
    assert repeated["started_count"] == 0
    assert repeated["skipped_count"] == 1
    assert repeated["skipped"][0]["status"] == "awaiting_expert"
