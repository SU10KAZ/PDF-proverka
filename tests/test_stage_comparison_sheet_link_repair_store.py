from __future__ import annotations

import json

import pytest

from backend.app.services.stage_comparison import paths
from backend.app.services.stage_comparison import project_change_summary
from backend.app.services.stage_comparison import store


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def setup_repair_inputs(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "runtime"))
    session_id, pair_id = "session-1", "pair-1"
    monkeypatch.setattr(store, "_load_session_meta", lambda *_: {"ok": True})
    monkeypatch.setattr(store, "_load_pair", lambda *_: {"id": pair_id})
    links = {
        "version": 1, "pair_id": pair_id, "updated_at": "before",
        "unlinked_left_pages": [],
        "links": [{
            "id": "bad", "left_pages": [1], "right_pages": [1],
            "source": "manual", "confidence": "manual", "reason": [],
        }],
    }
    suggestions = {
        "version": 2, "pair_id": pair_id,
        "left_sheet_index": [
            {"pdf_page": 1, "title": "Молниезащита"},
            {"pdf_page": 2, "title": "Однолинейная схема ВРУ-А"},
        ],
        "right_sheet_index": [
            {"pdf_page": 1, "title": "Однолинейная схема ВРУ-А"},
        ],
        "suggestions": [],
    }
    write_json(paths.sheet_links_path(session_id, pair_id), links)
    write_json(paths.sheet_match_suggestions_path(session_id, pair_id), suggestions)
    groups = [{
        "group_id": "bad",
        "pair_precheck": {"status": project_change_summary.PAIR_REVIEW_REQUIRED},
    }]
    return session_id, pair_id, links, groups


def test_apply_persists_atomic_link_snapshot_and_audit(tmp_path, monkeypatch):
    session_id, pair_id, before, groups = setup_repair_inputs(tmp_path, monkeypatch)
    result = store._apply_sheet_link_repair(session_id, pair_id, groups)
    assert result is not None
    saved = json.loads(paths.sheet_links_path(session_id, pair_id).read_text(encoding="utf-8"))
    assert saved["links"][0]["left_pages"] == [2]
    assert saved["links"][0]["source"] == "auto_repair"
    assert saved["unlinked_left_pages"] == [1]
    audit = store.get_sheet_link_repairs_state(session_id, pair_id)
    assert len(audit["active_repairs"]) == 1
    recorded = audit["active_repairs"][0]
    assert recorded["before_snapshot"] == before
    assert recorded["after_snapshot"] == saved
    assert recorded["dependent_artifacts_recomputed"] is False


@pytest.mark.asyncio
async def test_undo_restores_snapshot_and_marks_recomputed(tmp_path, monkeypatch):
    session_id, pair_id, before, groups = setup_repair_inputs(tmp_path, monkeypatch)
    repair = store._apply_sheet_link_repair(session_id, pair_id, groups)
    monkeypatch.setattr(store, "_load_session_meta", lambda *_: {"ok": True})
    monkeypatch.setattr(store, "_load_pair", lambda *_: {"id": pair_id})
    calls = []

    async def recompute(*args):
        calls.append(args)
        return {"status": "completed"}

    monkeypatch.setattr(store, "_recompute_after_sheet_link_change", recompute)
    monkeypatch.setattr(store, "get_pair_view", lambda *_: {"pair": {"id": pair_id}})
    result = await store.undo_sheet_link_repair(session_id, pair_id, repair["id"])
    restored = json.loads(paths.sheet_links_path(session_id, pair_id).read_text(encoding="utf-8"))
    assert {key: value for key, value in restored.items() if key != "updated_at"} == {
        key: value for key, value in before.items() if key != "updated_at"
    }
    assert calls == [(session_id, pair_id)]
    audit = store.get_sheet_link_repairs_state(session_id, pair_id)
    assert audit["active_repairs"] == []
    assert audit["repairs"][0]["status"] == "undone"
    assert audit["repairs"][0]["dependent_artifacts_recomputed"] is True
    assert result == {"pair": {"id": pair_id}}
    assert store._apply_sheet_link_repair(session_id, pair_id, groups) is None


@pytest.mark.asyncio
async def test_undo_refuses_to_overwrite_links_changed_after_repair(tmp_path, monkeypatch):
    session_id, pair_id, _before, groups = setup_repair_inputs(tmp_path, monkeypatch)
    repair = store._apply_sheet_link_repair(session_id, pair_id, groups)
    monkeypatch.setattr(store, "_load_session_meta", lambda *_: {"ok": True})
    monkeypatch.setattr(store, "_load_pair", lambda *_: {"id": pair_id})
    changed = json.loads(paths.sheet_links_path(session_id, pair_id).read_text(encoding="utf-8"))
    changed["unlinked_left_pages"].append(99)
    write_json(paths.sheet_links_path(session_id, pair_id), changed)
    with pytest.raises(ValueError, match="sheet_links_changed_after_repair"):
        await store.undo_sheet_link_repair(session_id, pair_id, repair["id"])


def test_manual_save_supersedes_undo_snapshot(tmp_path, monkeypatch):
    session_id, pair_id, _before, groups = setup_repair_inputs(tmp_path, monkeypatch)
    repair = store._apply_sheet_link_repair(session_id, pair_id, groups)
    monkeypatch.setattr(store, "_load_session_meta", lambda *_: {"ok": True})
    monkeypatch.setattr(store, "_load_pair", lambda *_: {
        "id": pair_id, "left": {"pdf_path": "left.pdf"}, "right": {"pdf_path": "right.pdf"},
    })
    monkeypatch.setattr(store, "_page_count", lambda *_: 10)
    monkeypatch.setattr(store, "get_sheet_matching_state", lambda *_: {"saved": True})
    store.save_sheet_links(session_id, pair_id, [{
        "id": "manual-new", "left_pages": [2], "right_pages": [1],
        "source": "manual", "confidence": "manual", "reason": ["user_corrected"],
    }])
    audit = store.get_sheet_link_repairs_state(session_id, pair_id)
    assert audit["active_repairs"] == []
    assert audit["repairs"][0]["id"] == repair["id"]
    assert audit["repairs"][0]["status"] == "superseded"


@pytest.mark.asyncio
async def test_stage5_repair_bypasses_old_cache_and_runs_one_recompute_cycle(monkeypatch):
    monkeypatch.setattr(store, "_load_session_meta", lambda *_: {"ok": True})
    monkeypatch.setattr(store, "_load_pair", lambda *_: {"id": "pair-1"})
    monkeypatch.setattr(
        store, "_current_project_change_signature",
        lambda *_: ({"kind": "final"}, [{"group_id": "bad"}], "old-signature"),
    )
    monkeypatch.setattr(store, "_apply_sheet_link_repair", lambda *_: {"id": "repair-1"})
    monkeypatch.setattr(store, "_read_json", lambda *_: {
        "version": project_change_summary.VERSION,
        "source_signature": "old-signature", "status": "completed",
        "constraints": {"fallback_policy": "review_only_v1"},
    })
    cycles = []

    async def recompute(*args):
        cycles.append(args)
        return {"version": 1, "status": "completed"}

    marked = []
    monkeypatch.setattr(store, "_recompute_after_sheet_link_change", recompute)
    monkeypatch.setattr(store, "_mark_sheet_link_repair_recomputed", lambda *args: marked.append(args))
    result = await store.run_project_change_summary("session-1", "pair-1")
    assert cycles == [("session-1", "pair-1")]
    assert marked == [("session-1", "pair-1", "repair-1")]
    assert result["sheet_link_repair_applied"] is True
