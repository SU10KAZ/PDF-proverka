"""Saved comparisons remain discoverable after unrelated document uploads."""
from copy import deepcopy

from backend.app.services.stage_comparison import store


def _setup(monkeypatch):
    left = {"pdf_path": "/object/stage_1/v001/document.pdf", "md_path": "/object/stage_1/v001/document.md", "version_id": "v001"}
    right = {"pdf_path": "/object/assemblies/a/v001/document.pdf", "version_id": "v001",
             "assembly_ref": {"assembly_id": "a", "version_id": "v001", "manifest_sha256": "manifest"}}
    meta = {"id": "current", "stage_a_path": "/object/stage_1", "stage_b_path": "/object/stage_2",
            "documents": {"stage_1": [left], "stage_2": [right]}}
    entries = [{**meta, "id": "previous"}, {**meta, "id": "finished"}]
    pairs = {sid: {"id": "p", "left": deepcopy(left), "right": deepcopy(right)} for sid in ("previous", "finished")}
    states = {
        "previous": {"run_id": "active", "status": "RUNNING", "last_activity_at": "2026-11-01",
                     "provenance": {"mapper_plan": {"complete": True, "accepted_portions": list(range(1, 111))}}},
        "finished": {"run_id": "done", "status": "COMPLETED", "last_activity_at": "2026-11-02"},
    }
    monkeypatch.setattr(store, "_read_index", lambda: {"sessions": entries})
    monkeypatch.setattr(store, "_pair_ids", lambda sid: ["p"] if sid in pairs else [])
    monkeypatch.setattr(store, "_load_pair", lambda sid, pid: pairs.get(sid))
    monkeypatch.setattr(store.paths_mod, "production_state_path", lambda sid, pid: sid)
    monkeypatch.setattr(store, "_read_json", lambda sid: states.get(sid))
    return meta, entries, pairs, states


def test_active_saved_run_wins_and_exposes_mapping_count_without_copying(monkeypatch):
    meta, _, pairs, states = _setup(monkeypatch)
    before = deepcopy((pairs, states))
    runs = store._comparison_runs(meta)
    assert len(runs) == 1
    assert runs[0]["session_id"] == "previous"
    assert runs[0]["state"]["saved_mapping_portions"] == 110
    assert (pairs, states) == before
    assert "provenance" not in runs[0]["state"]


def test_never_joins_other_object_versions_or_assembly_manifest(monkeypatch):
    meta, entries, pairs, _ = _setup(monkeypatch)
    entries[1]["stage_a_path"] = "/another-object/stage_1"
    pairs["previous"]["right"]["assembly_ref"]["manifest_sha256"] = "different"
    assert store._comparison_runs(meta) == []
    pairs["previous"]["right"] = deepcopy(meta["documents"]["stage_2"][0])
    pairs["previous"]["left"]["version_id"] = "v002"
    assert store._comparison_runs(meta) == []
    pairs["previous"]["left"]["version_id"] = "v001"
    pairs["previous"]["left"]["md_path"] = "/other.md"
    assert store._comparison_runs(meta) == []


def test_latest_terminal_run_and_incomplete_mapping_are_honest(monkeypatch):
    meta, _, _, states = _setup(monkeypatch)
    states["previous"]["status"] = "FAILED"
    assert store._comparison_runs(meta)[0]["state"]["run_id"] == "done"
    states["previous"]["last_activity_at"] = "2026-11-03"
    states["previous"]["provenance"]["mapper_plan"]["complete"] = False
    assert store._comparison_runs(meta)[0]["state"]["saved_mapping_portions"] == 0
