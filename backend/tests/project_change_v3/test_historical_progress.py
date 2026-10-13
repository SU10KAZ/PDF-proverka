"""Historical progress is derived only from the same saved run; no model calls."""
import json
from copy import deepcopy

import pytest

from backend.app.services.project_change_v3 import run_storage
from backend.app.services.project_change_v3.historical_progress import restore_pipeline


@pytest.fixture
def saved(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))
    state = {"engine": "projectchange_v3", "status": "REVIEW", "run_id": "old_run",
             "semantic_region_count": 2, "human_mapping_published": True}
    result = {"schema": "projectchange_v3_final/2", "session_id": "session", "pair_id": "pair",
              "run_id": "old_run", "projectchanges": [], "projectchange_regions": {}, "dedupe": {},
              "source_manifest": {"old_pdf_sha256": "old", "new_pdf_sha256": "new", "structure_sha256": "structure"},
              "coverage": {"complete": True, "content_unmatched_pending": 0},
              "supplemental_review": {"enabled": True, "batches": 1, "accepted": 1, "failed": 0},
              "source_verification": {"enabled": True, "batches": 4, "accepted": 4, "failed": 0},
              "quality": {"source_verification_completed": 32, "source_verification_pending": 100}}
    path = run_storage.artifact_path("session", "pair", "project_change_v3_result", "old_run")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(result))
    return state, result, path


def test_restores_completion_and_partial_verification_without_writes_or_fake_timing(saved):
    state, _, path = saved
    before_state, before_bytes = deepcopy(state), path.read_bytes()
    restored = restore_pipeline("session", "pair", state)
    assert restored["pipeline_history_source"] == "saved_result"
    stages = restored["pipeline_stages"]
    assert [s["status"] for s in stages.values()] == ["COMPLETED"] * 5 + ["PARTIAL", "COMPLETED"]
    assert "32" in stages["SOURCE_VERIFICATION"]["message"] and "100" in stages["SOURCE_VERIFICATION"]["message"]
    assert all("started_at" not in s and "finished_at" not in s for s in stages.values())
    assert state == before_state and path.read_bytes() == before_bytes


@pytest.mark.parametrize("field,value", [("status", "FAILED"), ("status", "RUNNING"),
                                          ("engine", "legacy"), ("pipeline_stages", {"MAPPING": {"status": "PARTIAL"}})])
def test_keeps_live_failed_and_recorded_history_untouched(saved, field, value):
    state, _, _ = saved
    assert restore_pipeline("session", "pair", {**state, field: value}) == {}


@pytest.mark.parametrize("field", ["session_id", "pair_id", "run_id", "schema"])
def test_rejects_other_run_or_scope(saved, field):
    state, result, path = saved
    result[field] = "other"
    path.write_text(json.dumps(result))
    assert restore_pipeline("session", "pair", state) == {}


def test_uses_explicit_run_and_does_not_fall_back_to_current_pointer(saved):
    state, _, path = saved
    run_storage.atomic(run_storage.root("session", "pair") / "current_run.json", {"run_id": "new_run"})
    assert restore_pipeline("session", "pair", state)["pipeline_stages"]["RESULT"]["status"] == "COMPLETED"
    path.unlink()
    assert restore_pipeline("session", "pair", state) == {}
    path.write_text("broken json")
    assert restore_pipeline("session", "pair", state) == {}


def test_reused_skipped_partial_coverage_and_publication(saved):
    state, result, path = saved
    state.update(resumed_from_run_id="donor", human_mapping_published=False)
    result["supplemental_review"].update(enabled=False, batches=0)
    result["source_verification"].update(enabled=False, batches=0)
    path.write_text(json.dumps(result))
    stages = restore_pipeline("session", "pair", state)["pipeline_stages"]
    assert stages["MAPPING"]["status"] == stages["MINING"]["status"] == "REUSED"
    assert stages["UNMATCHED_REVIEW"]["status"] == stages["SOURCE_VERIFICATION"]["status"] == "SKIPPED"
    assert stages["RESULT"]["status"] == "PARTIAL"
    result["coverage"]["complete"] = False
    path.write_text(json.dumps(result))
    assert restore_pipeline("session", "pair", state)["pipeline_stages"]["MINING"]["status"] == "PARTIAL"


def test_missing_evidence_does_not_mark_stages_done(saved):
    state, result, path = saved
    for key in ["source_manifest", "projectchange_regions", "coverage", "supplemental_review", "source_verification", "dedupe"]:
        result.pop(key)
    path.write_text(json.dumps(result))
    assert list(restore_pipeline("session", "pair", state)["pipeline_stages"]) == ["RESULT"]
