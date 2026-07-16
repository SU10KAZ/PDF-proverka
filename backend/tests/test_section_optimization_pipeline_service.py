from __future__ import annotations

import asyncio

import pytest

from backend.app.services import section_optimization_pipeline_service as pipeline


@pytest.fixture(autouse=True)
def isolated_pipeline_storage(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline, "_root", lambda: tmp_path)
    monkeypatch.setattr(pipeline, "_resolve_object_id", lambda object_id: object_id or "object-1")
    pipeline._ACTIVE_TASKS.clear()


def _collected():
    return {
        "section": "EOM",
        "section_project_count": 2,
        "projects": [
            {"project_id": "P1", "project_name": "Проект 1", "version_id": "v001", "specification_rows": 1, "graphic_blocks": 3},
            {"project_id": "P2", "project_name": "Проект 2", "version_id": "v001", "specification_rows": 1, "graphic_blocks": 5},
        ],
        "specification_rows": [{"project_id": "P1", "name": "Кабель", "unit": "м"}],
        "accepted_optimizations": [],
        "warnings": [],
        "optimization_items": 0,
        "graphic_blocks_available": 8,
    }


def _snapshot():
    return {
        "meta": {"section": "EOM", "generated_at": "2026-07-14T00:00:00+00:00"},
        "projects": _collected()["projects"],
        "signals": [
            {"signal_id": "SIG-1", "graphics_recommended": True, "project_ids": ["P1", "P2"]},
            {"signal_id": "SIG-2", "graphics_recommended": False, "project_ids": ["P1"]},
        ],
    }


@pytest.mark.asyncio
async def test_pipeline_runs_real_stages_and_persists_snapshot(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline, "collect_section_optimization_data", lambda *args, **kwargs: _collected())
    monkeypatch.setattr(pipeline, "normalize_section_optimization_data", lambda collected: collected)
    monkeypatch.setattr(pipeline, "synthesize_section_optimization_data", lambda normalized: _snapshot())

    started = pipeline.start_pipeline("eom", object_id="object-1")
    assert started["status"] == "queued"

    for _ in range(20):
        await asyncio.sleep(0)
        state = pipeline.get_pipeline_state("EOM", object_id="object-1")
        if state["status"] != "queued" and state["status"] != "running":
            break

    assert state["status"] == "ready_for_review"
    assert [stage["status"] for stage in state["stages"]] == [
        "done", "done", "done", "waiting", "waiting", "waiting",
    ]
    assert pipeline.get_latest_snapshot("EOM", object_id="object-1") == _snapshot()
    section_dir = tmp_path / "object-1" / "EOM"
    assert (section_dir / "snapshot.json").is_file()
    assert (section_dir / "pipeline.json").is_file()
    assert len(list((section_dir / "history").glob("*.snapshot.json"))) == 1


@pytest.mark.asyncio
async def test_recalculation_keeps_serving_previous_persisted_snapshot(monkeypatch):
    previous = {"meta": {"section": "EOM", "generated_at": "old"}, "signals": []}
    pipeline.store_latest_snapshot("EOM", previous, object_id="object-1", run_id="initial-run")
    monkeypatch.setattr(pipeline, "collect_section_optimization_data", lambda *args, **kwargs: _collected())
    monkeypatch.setattr(pipeline, "normalize_section_optimization_data", lambda collected: collected)
    monkeypatch.setattr(pipeline, "synthesize_section_optimization_data", lambda normalized: _snapshot())

    state = pipeline.start_pipeline("EOM", object_id="object-1")

    assert state["snapshot_persisted"] is True
    assert state["serving_previous_snapshot"] is True
    assert pipeline.get_latest_snapshot("EOM", object_id="object-1") == previous

    for _ in range(20):
        await asyncio.sleep(0)
        state = pipeline.get_pipeline_state("EOM", object_id="object-1")
        if state["status"] == "ready_for_review":
            break
    assert state["serving_previous_snapshot"] is False
    assert pipeline.get_latest_snapshot("EOM", object_id="object-1") == _snapshot()


@pytest.mark.asyncio
async def test_graphics_stage_builds_plan_without_starting_model(monkeypatch):
    monkeypatch.setattr(pipeline, "collect_section_optimization_data", lambda *args, **kwargs: _collected())
    monkeypatch.setattr(pipeline, "normalize_section_optimization_data", lambda collected: collected)
    monkeypatch.setattr(pipeline, "synthesize_section_optimization_data", lambda normalized: _snapshot())

    pipeline.start_pipeline("EOM", object_id="object-1")
    for _ in range(20):
        await asyncio.sleep(0)
        state = pipeline.get_pipeline_state("EOM", object_id="object-1")
        if state["status"] == "ready_for_review":
            break

    state = pipeline.request_graphics_plan("EOM", object_id="object-1")
    graphics = next(stage for stage in state["stages"] if stage["key"] == "graphics")
    assert graphics["status"] == "done"
    assert state["graphics_plan"]["signals_count"] == 1
    assert state["graphics_plan"]["graphic_blocks_available"] == 8
    assert "Vision/LLM не запускался" in state["graphics_plan"]["note"]
