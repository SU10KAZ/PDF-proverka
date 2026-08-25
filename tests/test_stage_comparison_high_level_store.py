from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.stage_comparison import high_level_project_changes as high
from backend.app.services.stage_comparison import paths
from backend.app.services.stage_comparison import project_change_summary as stage5
from backend.app.services.stage_comparison import store


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def source_summary() -> dict:
    evidence = {
        "evidence_id": "area-1", "source_status": "CHANGED",
        "summary": "Площадь помещения 1.А.1 изменена с 10 до 11 м².",
        "before": "1.А.1 10 м²", "after": "1.А.1 11 м²",
        "reason": "Изменено числовое значение площади.",
        "left_fragment_ids": ["L1"], "right_fragment_ids": ["R1"],
        "left_pages": [1], "right_pages": [2], "left_anchors": [], "right_anchors": [],
        "deterministic_class_hint": "PROJECT_CHANGE",
        "deterministic_category_hint": "areas",
    }
    item = {
        "id": "change-1", "title": "Скорректирована площадь помещения.",
        "category": "areas", "evidence_ids": ["area-1"], "count": 1,
        "details": [evidence],
    }
    return {
        "version": stage5.VERSION, "kind": stage5.KIND, "pair_id": "pair-1",
        "source_signature": "stage5-signature", "status": "completed",
        "sheet_groups": [{
            "group_id": "sheet-1", "left_pages": [1], "right_pages": [2],
            "left_labels": ["Лист 1"], "right_labels": ["Лист 2"],
            "pair_precheck": {"status": stage5.PAIR_OK},
            "aggregation_status": "ai_aggregated", "project_changes": [item],
            "service_structure": [], "review": [], "atomic_evidence": [evidence],
        }],
    }


@pytest.mark.asyncio
async def test_run_persists_only_additive_artifact_and_leaves_stage5_unchanged(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "runtime"))
    summary = source_summary()
    groups = high.build_semantic_groups(summary)
    signature = high.source_signature(summary, groups)
    monkeypatch.setattr(store, "_load_session_meta", lambda *_: {"ok": True})
    monkeypatch.setattr(store, "_load_pair", lambda *_: {"id": "pair-1"})
    monkeypatch.setattr(
        store, "_current_high_level_signature", lambda *_: (summary, groups, signature),
    )
    write_json(paths.project_change_summary_path("session-1", "pair-1"), summary)
    before = paths.project_change_summary_path("session-1", "pair-1").read_bytes()

    result = await store.run_high_level_project_changes(
        "session-1", "pair-1", allow_ai=False,
    )

    assert result["summary"]["high_level_changes"] == 1
    assert paths.high_level_project_changes_path("session-1", "pair-1").exists()
    assert paths.text_entities_path("session-1", "pair-1").exists()
    text_entities = json.loads(
        paths.text_entities_path("session-1", "pair-1").read_text(encoding="utf-8")
    )
    assert text_entities["kind"] == "stage_comparison_text_entities"
    assert text_entities["source_artifact"]["pair_id"] == "pair-1"
    assert paths.project_change_summary_path("session-1", "pair-1").read_bytes() == before


def test_old_run_without_stage53_artifact_remains_readable(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "runtime"))
    summary = source_summary()
    groups = high.build_semantic_groups(summary)
    signature = high.source_signature(summary, groups)
    monkeypatch.setattr(store, "_load_session_meta", lambda *_: {"ok": True})
    monkeypatch.setattr(store, "_load_pair", lambda *_: {"id": "pair-1"})
    monkeypatch.setattr(
        store, "_current_high_level_signature", lambda *_: (summary, groups, signature),
    )
    assert store.get_high_level_project_changes_state("session-1", "pair-1") is None
    assert store.get_text_entities_state("session-1", "pair-1") is None


def test_high_level_api_routes_do_not_reuse_stage5_response(monkeypatch):
    expected = {"version": 1, "kind": high.KIND, "high_level_changes": []}

    async def run(*_args):
        return expected

    monkeypatch.setattr(store, "run_high_level_project_changes", run)
    monkeypatch.setattr(store, "get_high_level_project_changes_state", lambda *_: expected)
    monkeypatch.setattr(store, "get_text_entities_state", lambda *_: None)
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    base = "/api/stage-comparison/sessions/session/pairs/pair/high-level-project-changes"
    assert client.post(base).json() == expected
    assert client.get(base).json() == expected
    assert client.get(base.rsplit("/", 1)[0] + "/text-entities").json()["status"] == (
        "not_started"
    )
